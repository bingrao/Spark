/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.storage
import java.io.{File, FileWriter, IOException}
import java.nio.file.{Files, Paths}
import java.util.{HashMap => JHashMap}
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.immutable.List
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source
import com.google.common.cache.CacheBuilder

import scala.util.Random
import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.network.shuffle.ExternalBlockStoreClient
import org.apache.spark.rpc.{IsolatedRpcEndpoint, RpcCallContext, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util.{RpcUtils, ThreadUtils, Utils}



/**
 * BlockManagerMasterEndpoint is an [[IsolatedRpcEndpoint]] on the master node to track statuses
 * of all slaves' block managers.
 */
private[spark]
class BlockManagerMasterEndpoint(
    override val rpcEnv: RpcEnv,
    val isLocal: Boolean,
    conf: SparkConf,
    listenerBus: LiveListenerBus,
    externalBlockStoreClient: Option[ExternalBlockStoreClient],
    blockManagerInfo: mutable.Map[BlockManagerId, BlockManagerInfo])
  extends IsolatedRpcEndpoint with Logging {

  // Mapping from executor id to the block manager's local disk directories.
  private val executorIdToLocalDirs =
    CacheBuilder
      .newBuilder()
      .maximumSize(conf.get(config.STORAGE_LOCAL_DISK_BY_EXECUTORS_CACHE_SIZE))
      .build[String, Array[String]]()

  // Mapping from external shuffle service block manager id to the block statuses.
  private val blockStatusByShuffleService =
    new mutable.HashMap[BlockManagerId, JHashMap[BlockId, BlockStatus]]

  // Mapping from executor ID to block manager ID.
  private val blockManagerIdByExecutor = new mutable.HashMap[String, BlockManagerId]

  // Mapping from block id to the set of block managers that have the block.
  private val blockLocations = new JHashMap[BlockId, mutable.HashSet[BlockManagerId]]

  private val askThreadPool =
    ThreadUtils.newDaemonCachedThreadPool("block-manager-ask-thread-pool", 100)
  private implicit val askExecutionContext = ExecutionContext.fromExecutorService(askThreadPool)

  private val topologyMapper = {
    val topologyMapperClassName = conf.get(
      config.STORAGE_REPLICATION_TOPOLOGY_MAPPER)
    val clazz = Utils.classForName(topologyMapperClassName)
    val mapper =
      clazz.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[TopologyMapper]
    logInfo(s"Using $topologyMapperClassName for getting topology information")
    mapper
  }

  val proactivelyReplicate = conf.get(config.STORAGE_REPLICATION_PROACTIVE)

  val defaultRpcTimeout = RpcUtils.askRpcTimeout(conf)

  logInfo("BlockManagerMasterEndpoint up")
  // same as `conf.get(config.SHUFFLE_SERVICE_ENABLED)
  //   && conf.get(config.SHUFFLE_SERVICE_FETCH_RDD_ENABLED)`
  private val externalShuffleServiceRddFetchEnabled: Boolean = externalBlockStoreClient.isDefined
  private val externalShuffleServicePort: Int = StorageUtils.externalShuffleServicePort(conf)
  
  ////////////////////////////////////////////////////////////////////////////////////////////////
  private val startTime = System.currentTimeMillis
  logInfo(s"LRC: Log start time: $startTime")
  var RDDHit = 0
  var RDDMiss = 0
  var diskRead = 0
  var diskWrite = 0
  var totalReference = 0
  var partition = 0
  var taskHit = 0
  var stageHit = 0
  var totalHitBlockList = mutable.MutableList[BlockId]()

  /**
   * Reference Profile by application
   */
  private val refProfile = mutable.HashMap[Int, Int]() // yyh


  val appName = conf.getAppName.filter(!" ".contains(_))
  val prefixedPath = conf.get("spark.local.dir") + File.separator + "lrc" +
    File.separator + appName
  val dirPath = new File(prefixedPath)
  if (!dirPath.exists()) dirPath.mkdir()

  val appDAG = prefixedPath + ".txt"
  logInfo(s"LRC: Driver Endpoint tries to read profile: path: $appDAG")
  if (Files.exists(Paths.get(appDAG))) {
    for (line <- Source.fromFile(appDAG).getLines()) {
      val z = line.split(":")
      refProfile(z(0).toInt) = z(1).toInt
    }
  }
  logInfo(s"LRC: Reference Profile by Application: ${refProfile}")

  /**
   * Reference profile by job
   */
  private val refProfile_By_Job = mutable.HashMap[Int, mutable.HashMap[Int, Int]]()
  val jobDAG = prefixedPath + "-JobDAG.txt"
  logInfo(s"LRC: Driver Endpoint tries to read profile by job: path: $jobDAG")
  if (Files.exists(Paths.get(jobDAG))) {
    for (line <- Source.fromFile(jobDAG).getLines()) {
      val z = line.split("-")
      val jobId = z(0).toInt
      val this_refProfile = mutable.HashMap[Int, Int]()
      if (z.length > 1) {
        // some jobs may have no rdd refs
        val refs = z(1).split(";")
        for (ref <- refs) {
          val pairs = ref.split(":")
          this_refProfile(pairs(0).toInt) = pairs(1).toInt
        }
      }
      refProfile_By_Job(jobId) = this_refProfile
    }
  }
  logInfo(s"LRC: Reference Profile by Job: ${refProfile_By_Job}")

  /**
   * For all-or-nothing property
   * Notice that each rdd has at most one peer rdd, as no operation handles more than two RDDs
   * Be careful, here we only assume that all the peers are only required once.
   * That means once either of the peer got evicted, it is safe to clear the ref count of the other
   * for all-or-nothing considerations.
   * It's the BlockManager on slaves who decide to report an eviction of a block with a peer or not.
   * In the case where a block whose peer is already evicted, the BlockManger should not report.
   * */
  private val peerProfile = mutable.HashMap[Int, Int]()
  val peers = prefixedPath + "-Peers.txt"
  logInfo(s"LRC: Driver Endpoint tries to read peers profile: path :$peers")
  if (Files.exists(Paths.get(peers))) {
    for (line <- Source.fromFile(peers).getLines()) {
      val z = line.split(":")
      peerProfile(z(0).toInt) = z(1).toInt // mutual peers, as later we only search by key
      peerProfile(z(1).toInt) = z(0).toInt
    }
  }
  logInfo(s"LRC: Reference Profile by Peer: ${peerProfile}")
  ///////////////////////////////////////////////////////////////////////////////////


  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterBlockManager(id, localDirs, maxOnHeapMemSize, maxOffHeapMemSize, slaveEndpoint) =>
      context.reply(register(id, localDirs, maxOnHeapMemSize, maxOffHeapMemSize, slaveEndpoint))

    case _updateBlockInfo @
        UpdateBlockInfo(blockManagerId, blockId, storageLevel, deserializedSize, size) =>
      val isSuccess = updateBlockInfo(blockManagerId, blockId, storageLevel, deserializedSize, size)
      context.reply(isSuccess)
      // SPARK-30594: we should not post `SparkListenerBlockUpdated` when updateBlockInfo
      // returns false since the block info would be updated again later.
      if (isSuccess) {
        listenerBus.post(SparkListenerBlockUpdated(BlockUpdatedInfo(_updateBlockInfo)))
      }

    case GetLocations(blockId) =>
      context.reply(getLocations(blockId))

    case GetLocationsAndStatus(blockId, requesterHost) =>
      context.reply(getLocationsAndStatus(blockId, requesterHost))

    case GetLocationsMultipleBlockIds(blockIds) =>
      context.reply(getLocationsMultipleBlockIds(blockIds))

    case GetPeers(blockManagerId) =>
      context.reply(getPeers(blockManagerId))

    case GetExecutorEndpointRef(executorId) =>
      context.reply(getExecutorEndpointRef(executorId))

    case GetMemoryStatus =>
      context.reply(memoryStatus)

    case GetStorageStatus =>
      context.reply(storageStatus)

    case GetBlockStatus(blockId, askSlaves) =>
      context.reply(blockStatus(blockId, askSlaves))

    case IsExecutorAlive(executorId) =>
      context.reply(blockManagerIdByExecutor.contains(executorId))

    case GetMatchingBlockIds(filter, askSlaves) =>
      context.reply(getMatchingBlockIds(filter, askSlaves))

    case RemoveRdd(rddId) =>
      context.reply(removeRdd(rddId))

    case RemoveShuffle(shuffleId) =>
      context.reply(removeShuffle(shuffleId))

    case RemoveBroadcast(broadcastId, removeFromDriver) =>
      context.reply(removeBroadcast(broadcastId, removeFromDriver))

    case RemoveBlock(blockId) =>
      removeBlockFromWorkers(blockId)
      context.reply(true)

    case RemoveExecutor(execId) =>
      removeExecutor(execId)
      context.reply(true)

    case StopBlockManagerMaster =>
      context.reply(true)
      stop()

    ///////////////////////////////////////////////////////////////////////////
    case StartBroadcastJobId(jobId) =>
      broadcastJobDAG(jobId) // , refProfile_By_Job(jobId))
      context.reply(true)

    case ReportCacheHit(blockManagerId, list, hitBlockList) => // yyh
      updateCacheHit(blockManagerId, list, hitBlockList)
      context.reply(true)

    case GetRefProfile(blockManagerId, slaveEndPoint) => // yyh
      context.reply(getRefProfile(blockManagerId, slaveEndPoint))

    case BlockWithPeerEvicted(blockId) => // yyh
      onPeerEvicted(blockId)
      context.reply(true)

    case StartBroadcastRefCount(jobId, partitionNumber, refCount) =>
      broadcastJobDAG(jobId, partitionNumber, refCount)
      context.reply(true)

    // case ReportRefMap(blockManagerId, currentRefMap) =>
    // logInfo(s"yyh: received from $blockManagerId, $currentRefMap")
    // context.reply(true)
    ///////////////////////////////////////////////////////////////////////////
  }

  private def removeRdd(rddId: Int): Future[Seq[Int]] = {
    // First remove the metadata for the given RDD, and then asynchronously remove the blocks
    // from the slaves.

    // The message sent to the slaves to remove the RDD
    val removeMsg = RemoveRdd(rddId)

    // Find all blocks for the given RDD, remove the block from both blockLocations and
    // the blockManagerInfo that is tracking the blocks and create the futures which asynchronously
    // remove the blocks from slaves and gives back the number of removed blocks
    val blocks = blockLocations.asScala.keys.flatMap(_.asRDDId).filter(_.rddId == rddId)
    val blocksToDeleteByShuffleService =
      new mutable.HashMap[BlockManagerId, mutable.HashSet[RDDBlockId]]

    blocks.foreach { blockId =>
      val bms: mutable.HashSet[BlockManagerId] = blockLocations.remove(blockId)

      val (bmIdsExtShuffle, bmIdsExecutor) = bms.partition(_.port == externalShuffleServicePort)
      val liveExecutorsForBlock = bmIdsExecutor.map(_.executorId).toSet
      bmIdsExtShuffle.foreach { bmIdForShuffleService =>
        // if the original executor is already released then delete this disk block via
        // the external shuffle service
        if (!liveExecutorsForBlock.contains(bmIdForShuffleService.executorId)) {
          val blockIdsToDel = blocksToDeleteByShuffleService.getOrElseUpdate(bmIdForShuffleService,
            new mutable.HashSet[RDDBlockId]())
          blockIdsToDel += blockId
          blockStatusByShuffleService.get(bmIdForShuffleService).foreach { blockStatus =>
            blockStatus.remove(blockId)
          }
        }
      }
      bmIdsExecutor.foreach { bmId =>
        blockManagerInfo.get(bmId).foreach { bmInfo =>
          bmInfo.removeBlock(blockId)
        }
      }
    }
    val removeRddFromExecutorsFutures = blockManagerInfo.values.map { bmInfo =>
      bmInfo.slaveEndpoint.ask[Int](removeMsg).recover {
        case e: IOException =>
          logWarning(s"Error trying to remove RDD ${removeMsg.rddId} " +
            s"from block manager ${bmInfo.blockManagerId}", e)
          0 // zero blocks were removed
      }
    }.toSeq

    val removeRddBlockViaExtShuffleServiceFutures = externalBlockStoreClient.map { shuffleClient =>
      blocksToDeleteByShuffleService.map { case (bmId, blockIds) =>
        Future[Int] {
          val numRemovedBlocks = shuffleClient.removeBlocks(
            bmId.host,
            bmId.port,
            bmId.executorId,
            blockIds.map(_.toString).toArray)
          numRemovedBlocks.get(defaultRpcTimeout.duration.toSeconds, TimeUnit.SECONDS)
        }
      }
    }.getOrElse(Seq.empty)

    Future.sequence(removeRddFromExecutorsFutures ++ removeRddBlockViaExtShuffleServiceFutures)
  }

  private def removeShuffle(shuffleId: Int): Future[Seq[Boolean]] = {
    // Nothing to do in the BlockManagerMasterEndpoint data structures
    val removeMsg = RemoveShuffle(shuffleId)
    Future.sequence(
      blockManagerInfo.values.map { bm =>
        bm.slaveEndpoint.ask[Boolean](removeMsg)
      }.toSeq
    )
  }

  /**
   * Delegate RemoveBroadcast messages to each BlockManager because the master may not notified
   * of all broadcast blocks. If removeFromDriver is false, broadcast blocks are only removed
   * from the executors, but not from the driver.
   */
  private def removeBroadcast(broadcastId: Long, removeFromDriver: Boolean): Future[Seq[Int]] = {
    val removeMsg = RemoveBroadcast(broadcastId, removeFromDriver)
    val requiredBlockManagers = blockManagerInfo.values.filter { info =>
      removeFromDriver || !info.blockManagerId.isDriver
    }
    val futures = requiredBlockManagers.map { bm =>
      bm.slaveEndpoint.ask[Int](removeMsg).recover {
        case e: IOException =>
          logWarning(s"Error trying to remove broadcast $broadcastId from block manager " +
            s"${bm.blockManagerId}", e)
          0 // zero blocks were removed
      }
    }.toSeq

    Future.sequence(futures)
  }

  private def removeBlockManager(blockManagerId: BlockManagerId): Unit = {
    val info = blockManagerInfo(blockManagerId)

    // Remove the block manager from blockManagerIdByExecutor.
    blockManagerIdByExecutor -= blockManagerId.executorId

    // Remove it from blockManagerInfo and remove all the blocks.
    blockManagerInfo.remove(blockManagerId)

    val iterator = info.blocks.keySet.iterator
    while (iterator.hasNext) {
      val blockId = iterator.next
      val locations = blockLocations.get(blockId)
      locations -= blockManagerId
      // De-register the block if none of the block managers have it. Otherwise, if pro-active
      // replication is enabled, and a block is either an RDD or a test block (the latter is used
      // for unit testing), we send a message to a randomly chosen executor location to replicate
      // the given block. Note that we ignore other block types (such as broadcast/shuffle blocks
      // etc.) as replication doesn't make much sense in that context.
      if (locations.size == 0) {
        blockLocations.remove(blockId)
        logWarning(s"No more replicas available for $blockId !")
      } else if (proactivelyReplicate && (blockId.isRDD || blockId.isInstanceOf[TestBlockId])) {
        // As a heursitic, assume single executor failure to find out the number of replicas that
        // existed before failure
        val maxReplicas = locations.size + 1
        val i = (new Random(blockId.hashCode)).nextInt(locations.size)
        val blockLocations = locations.toSeq
        val candidateBMId = blockLocations(i)
        blockManagerInfo.get(candidateBMId).foreach { bm =>
          val remainingLocations = locations.toSeq.filter(bm => bm != candidateBMId)
          val replicateMsg = ReplicateBlock(blockId, remainingLocations, maxReplicas)
          bm.slaveEndpoint.ask[Boolean](replicateMsg)
        }
      }
    }

    listenerBus.post(SparkListenerBlockManagerRemoved(System.currentTimeMillis(), blockManagerId))
    logInfo(s"Removing block manager $blockManagerId")

  }

  private def removeExecutor(execId: String): Unit = {
    logInfo("Trying to remove executor " + execId + " from BlockManagerMaster.")
    blockManagerIdByExecutor.get(execId).foreach(removeBlockManager)
  }

  // Remove a block from the slaves that have it. This can only be used to remove
  // blocks that the master knows about.
  private def removeBlockFromWorkers(blockId: BlockId): Unit = {
    val locations = blockLocations.get(blockId)
    if (locations != null) {
      locations.foreach { blockManagerId: BlockManagerId =>
        val blockManager = blockManagerInfo.get(blockManagerId)
        if (blockManager.isDefined) {
          // Remove the block from the slave's BlockManager.
          // Doesn't actually wait for a confirmation and the message might get lost.
          // If message loss becomes frequent, we should add retry logic here.
          blockManager.get.slaveEndpoint.ask[Boolean](RemoveBlock(blockId))
        }
      }
    }
  }

  // Return a map from the block manager id to max memory and remaining memory.
  private def memoryStatus: Map[BlockManagerId, (Long, Long)] = {
    blockManagerInfo.map { case(blockManagerId, info) =>
      (blockManagerId, (info.maxMem, info.remainingMem))
    }.toMap
  }

  private def storageStatus: Array[StorageStatus] = {
    blockManagerInfo.map { case (blockManagerId, info) =>
      new StorageStatus(blockManagerId, info.maxMem, Some(info.maxOnHeapMem),
        Some(info.maxOffHeapMem), info.blocks.asScala)
    }.toArray
  }

  /**
   * Return the block's status for all block managers, if any. NOTE: This is a
   * potentially expensive operation and should only be used for testing.
   *
   * If askSlaves is true, the master queries each block manager for the most updated block
   * statuses. This is useful when the master is not informed of the given block by all block
   * managers.
   */
  private def blockStatus(
      blockId: BlockId,
      askSlaves: Boolean): Map[BlockManagerId, Future[Option[BlockStatus]]] = {
    val getBlockStatus = GetBlockStatus(blockId)
    /*
     * Rather than blocking on the block status query, master endpoint should simply return
     * Futures to avoid potential deadlocks. This can arise if there exists a block manager
     * that is also waiting for this master endpoint's response to a previous message.
     */
    blockManagerInfo.values.map { info =>
      val blockStatusFuture =
        if (askSlaves) {
          info.slaveEndpoint.ask[Option[BlockStatus]](getBlockStatus)
        } else {
          Future { info.getStatus(blockId) }
        }
      (info.blockManagerId, blockStatusFuture)
    }.toMap
  }

  /**
   * Return the ids of blocks present in all the block managers that match the given filter.
   * NOTE: This is a potentially expensive operation and should only be used for testing.
   *
   * If askSlaves is true, the master queries each block manager for the most updated block
   * statuses. This is useful when the master is not informed of the given block by all block
   * managers.
   */
  private def getMatchingBlockIds(
      filter: BlockId => Boolean,
      askSlaves: Boolean): Future[Seq[BlockId]] = {
    val getMatchingBlockIds = GetMatchingBlockIds(filter)
    Future.sequence(
      blockManagerInfo.values.map { info =>
        val future =
          if (askSlaves) {
            info.slaveEndpoint.ask[Seq[BlockId]](getMatchingBlockIds)
          } else {
            Future { info.blocks.asScala.keys.filter(filter).toSeq }
          }
        future
      }
    ).map(_.flatten.toSeq)
  }

  private def externalShuffleServiceIdOnHost(blockManagerId: BlockManagerId): BlockManagerId = {
    // we need to keep the executor ID of the original executor to let the shuffle service know
    // which local directories should be used to look for the file
    BlockManagerId(blockManagerId.executorId, blockManagerId.host, externalShuffleServicePort)
  }

  /**
   * Returns the BlockManagerId with topology information populated, if available.
   */
  private def register(
      idWithoutTopologyInfo: BlockManagerId,
      localDirs: Array[String],
      maxOnHeapMemSize: Long,
      maxOffHeapMemSize: Long,
      slaveEndpoint: RpcEndpointRef): BlockManagerId = {
    // the dummy id is not expected to contain the topology information.
    // we get that info here and respond back with a more fleshed out block manager id
    val id = BlockManagerId(
      idWithoutTopologyInfo.executorId,
      idWithoutTopologyInfo.host,
      idWithoutTopologyInfo.port,
      topologyMapper.getTopologyForHost(idWithoutTopologyInfo.host))

    val time = System.currentTimeMillis()
    executorIdToLocalDirs.put(id.executorId, localDirs)
    if (!blockManagerInfo.contains(id)) {
      blockManagerIdByExecutor.get(id.executorId) match {
        case Some(oldId) =>
          // A block manager of the same executor already exists, so remove it (assumed dead)
          logError("Got two different block manager registrations on same executor - "
              + s" will replace old one $oldId with new one $id")
          removeExecutor(id.executorId)
        case None =>
      }
      logInfo("Registering block manager %s with %s RAM, %s".format(
        id.hostPort, Utils.bytesToString(maxOnHeapMemSize + maxOffHeapMemSize), id))

      blockManagerIdByExecutor(id.executorId) = id

      val externalShuffleServiceBlockStatus =
        if (externalShuffleServiceRddFetchEnabled) {
          val externalShuffleServiceBlocks = blockStatusByShuffleService
            .getOrElseUpdate(externalShuffleServiceIdOnHost(id), new JHashMap[BlockId, BlockStatus])
          Some(externalShuffleServiceBlocks)
        } else {
          None
        }

      blockManagerInfo(id) = new BlockManagerInfo(id, System.currentTimeMillis(),
        maxOnHeapMemSize, maxOffHeapMemSize, slaveEndpoint, externalShuffleServiceBlockStatus)
    }
    listenerBus.post(SparkListenerBlockManagerAdded(time, id, maxOnHeapMemSize + maxOffHeapMemSize,
        Some(maxOnHeapMemSize), Some(maxOffHeapMemSize)))
    id
  }

  private def updateBlockInfo(
      blockManagerId: BlockManagerId,
      blockId: BlockId,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long): Boolean = {

    if (!blockManagerInfo.contains(blockManagerId)) {
      if (blockManagerId.isDriver && !isLocal) {
        // We intentionally do not register the master (except in local mode),
        // so we should not indicate failure.
        return true
      } else {
        return false
      }
    }

    if (blockId == null) {
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      return true
    }

    blockManagerInfo(blockManagerId).updateBlockInfo(blockId, storageLevel, memSize, diskSize)

    var locations: mutable.HashSet[BlockManagerId] = null
    if (blockLocations.containsKey(blockId)) {
      locations = blockLocations.get(blockId)
    } else {
      locations = new mutable.HashSet[BlockManagerId]
      blockLocations.put(blockId, locations)
    }

    if (storageLevel.isValid) {
      locations.add(blockManagerId)
    } else {
      locations.remove(blockManagerId)
    }

    if (blockId.isRDD && storageLevel.useDisk && externalShuffleServiceRddFetchEnabled) {
      val externalShuffleServiceId = externalShuffleServiceIdOnHost(blockManagerId)
      if (storageLevel.isValid) {
        locations.add(externalShuffleServiceId)
      } else {
        locations.remove(externalShuffleServiceId)
      }
    }

    // Remove the block from master tracking if it has been removed on all slaves.
    if (locations.size == 0) {
      blockLocations.remove(blockId)
    }

    logInfo(s"Update Block ${blockId}_${storageLevel}_${memSize}_${diskSize} " +
      s"to Manager_${blockManagerId} @ ${System.currentTimeMillis}")
    blockManagerInfo.foreach { case (blockManagerId, blockManagerInfo) =>
      logInfo(s"The Block Manager ID ${blockManagerId}")
      blockManagerInfo.blocks.asScala.foreach{ case (blockID, blockStatus) =>
        logInfo(s"Block: ${blockID} -> ${blockStatus}")
      }
    }

    true
  }

  private def getLocations(blockId: BlockId): Seq[BlockManagerId] = {
    if (blockLocations.containsKey(blockId)) blockLocations.get(blockId).toSeq else Seq.empty
  }

  private def getLocationsAndStatus(
      blockId: BlockId,
      requesterHost: String): Option[BlockLocationsAndStatus] = {
    val locations = Option(blockLocations.get(blockId)).map(_.toSeq).getOrElse(Seq.empty)
    val status = locations.headOption.flatMap { bmId =>
      if (externalShuffleServiceRddFetchEnabled && bmId.port == externalShuffleServicePort) {
        Option(blockStatusByShuffleService(bmId).get(blockId))
      } else {
        blockManagerInfo.get(bmId).flatMap(_.getStatus(blockId))
      }
    }

    if (locations.nonEmpty && status.isDefined) {
      val localDirs = locations.find { loc =>
        // When the external shuffle service running on the same host is found among the block
        // locations then the block must be persisted on the disk. In this case the executorId
        // can be used to access this block even when the original executor is already stopped.
        loc.host == requesterHost &&
          (loc.port == externalShuffleServicePort ||
            blockManagerInfo
              .get(loc)
              .flatMap(_.getStatus(blockId).map(_.storageLevel.useDisk))
              .getOrElse(false))
      }.flatMap { bmId => Option(executorIdToLocalDirs.getIfPresent(bmId.executorId)) }
      Some(BlockLocationsAndStatus(locations, status.get, localDirs))
    } else {
      None
    }
  }

  private def getLocationsMultipleBlockIds(
      blockIds: Array[BlockId]): IndexedSeq[Seq[BlockManagerId]] = {
    blockIds.map(blockId => getLocations(blockId))
  }

  /** Get the list of the peers of the given block manager */
  private def getPeers(blockManagerId: BlockManagerId): Seq[BlockManagerId] = {
    val blockManagerIds = blockManagerInfo.keySet
    if (blockManagerIds.contains(blockManagerId)) {
      blockManagerIds.filterNot { _.isDriver }.filterNot { _ == blockManagerId }.toSeq
    } else {
      Seq.empty
    }
  }

  /**
   * Returns an [[RpcEndpointRef]] of the [[BlockManagerSlaveEndpoint]] for sending RPC messages.
   */
  private def getExecutorEndpointRef(executorId: String): Option[RpcEndpointRef] = {
    for (
      blockManagerId <- blockManagerIdByExecutor.get(executorId);
      info <- blockManagerInfo.get(blockManagerId)
    ) yield {
      info.slaveEndpoint
    }
  }

  override def onStop(): Unit = {
    lrc_clean()
    askThreadPool.shutdownNow()
  }

  /////////////////////////////////////////////////////////////////////////////
  private def lrc_clean(): Unit = {
    val stopTime = System.currentTimeMillis
    val duration = stopTime - startTime
    RDDHit = totalReference - RDDMiss // yyh: align total reference count
    if (RDDHit < 0 ) RDDHit = 0
    logInfo(s"LRC: log stoptime: $stopTime, duration: $duration ms")
    logInfo(s"LRC: Closing blockMangerMasterEndPoint, RDD hit $RDDHit, RDD miss $RDDMiss")
    logInfo(s"LRC: Disk read count: $diskRead, disk write count: $diskWrite")
    // val path = System.getProperty("user.dir")
    val appName = conf.getAppName
    val fw = new FileWriter("result.txt", true) // true means append mode
    fw.write(s"AppName: $appName, Runtime: $duration\n")
    fw.write(s"RDD Hit\t$RDDHit\tRDD Miss\t$RDDMiss\n")
    printf(s"totalHitBlockList: $totalHitBlockList")
    // val (taskHit, stageHit) = calculateEffectiveHit()

    val fw_hit = new FileWriter("hitBlockList.txt", false)
    for (blockId <- totalHitBlockList) {
      fw_hit.write(blockId + "\n")
    }
    printf("Effective Hit Calculated\n")

    // fw.write(s"Task Hit\t$taskHit\tStage Hit\t$stageHit\n")

    fw.close()
    fw_hit.close()
  }

  private def broadcastJobDAG(jobId: Int): Unit = {
    for (bm <- blockManagerInfo.values) {
      val (currentRefMap, refMap) = bm.slaveEndpoint.askSync[(mutable.Map[BlockId, Int],
        mutable.Map[BlockId, Int])](BroadcastJobDAG(jobId, None))

      logInfo(s"LRC: Updated CurrentRefMap from ${bm.blockManagerId}: $currentRefMap")
      logInfo(s"LRC: Updated RefMap from ${bm.blockManagerId}: $refMap")
    }
  }

  private def broadcastJobDAG(jobId: Int, partitionNumber: Int,
                              refCount: mutable.HashMap[Int, Int]): Unit = {
    logInfo(s"LRC: Start to broadcast the profiled refCount of job $jobId")
    logInfo(s"LRC: The reference count for each RDD (RDD -> RefCount): $refCount")
    for (bm <- blockManagerInfo.values) {
      val (currentRefMap, refMap) = bm.slaveEndpoint.askSync[(mutable.HashMap[BlockId, Int],
        mutable.HashMap[BlockId, Int])](BroadcastJobDAG(jobId, Some(refCount)))
      // val (currentRefMap, refMap) = bm.broadcastJobDAG(jobId)
      logInfo(s"LRC: Updated CurrentRefMap from $bm: $currentRefMap")
      logInfo(s"LRC: Updated RefMap from $bm: $refMap")
    }
    // update the total reference count.
    this.synchronized{totalReference += refCount.foldLeft(0)(_ + _._2) * partitionNumber}
    partition = partitionNumber
  }

  private def updateCacheHit(blockManagerId: BlockManagerId, list: List[Int],
                             hitBlockList: mutable.MutableList[BlockId]): Boolean = {
    // list (hitCount, missCount, diskRead, diskWrite)
    this.synchronized {
      RDDHit += list(0)
      RDDMiss += list(1)
      diskRead += list(2)
      diskWrite += list(3)

    }
    logInfo(s"LRC: Received Report from $blockManagerId: " +
      s"RDD Hit count increased by ${list(0)}. now $RDDHit, " +
      s"RDD Miss count increased by ${list(1)}. now $RDDMiss, " +
      s"Disk Read count increased by ${list(2)}. now $diskRead, " +
      s"Disk Write count increased by ${list(3)}. now $diskWrite"
    )
    this.synchronized { totalHitBlockList ++=  hitBlockList}
//    logInfo(s"totalHitBlockList: $totalHitBlockList \n")
//    calculateEffectiveHit(hitBlockList)
    true
  }

  private def getRefProfile(blockManagerId: BlockManagerId, slaveEndPoint: RpcEndpointRef):
  (mutable.HashMap[Int, Int], mutable.HashMap[Int, mutable.HashMap[Int, Int]],
    mutable.HashMap[Int, Int]) = {
    logInfo(s"LRC: Got the request of refProfile from block manager $blockManagerId, responding")
    (refProfile, refProfile_By_Job, peerProfile)
  }

  private def onPeerEvicted(blockId: BlockId): Unit = {
    val rddId = blockId.asRDDId.toString.split("_")(1).toInt
    // val index = blockId.asRDDId.toString.split("_")(2).toInt
    val peerRDDId = peerProfile.get(rddId)

    if (peerRDDId.isEmpty) {
      logError(s"yyh: The reported block $blockId has no peer!")
    } else {
      // For conservative all-or-nothing, decrease the ref count of the corresponding block
      // val peerBlockId = new RDDBlockId(peerRDDId.get, index)
      // notifyPeersConservatively(blockId)

      // For strict all-or-nothing, decrease the ref count of all the blocks of both rdds
      // notifyPeersStrictly(blockId)
    }
  }
  private def notifyPeersConservatively(blockId: BlockId): Unit = {
    for (bm <- blockManagerInfo.values) {
      bm.slaveEndpoint.ask[Boolean](CheckPeersConservatively(blockId))
    }

    /**
     * val locations = blockLocations.get(blockId)
     * if (locations != null) {
     * locations.foreach { blockManagerId: BlockManagerId =>
     * val blockManager = blockManagerInfo.get(blockManagerId)
     * if (blockManager.isDefined) {
     * // yyh: tell the blockManager to decrease the ref count of the given block
     * logInfo(s"yyh: Telling $blockManager to decrease the ref count of $blockId")
     * blockManager.get.slaveEndpoint.ask[Boolean](DecreaseBlockRefCount(blockId))
     * }
     * }
     * }
     */
  }
  private def notifyPeersStrictly(blockId: BlockId): Unit = {
    for (bm <- blockManagerInfo.values) {
      bm.slaveEndpoint.ask[Boolean](CheckPeersStrictly(blockId))
    }

    /** TRY TO FIND THE LOCATION OF EACH BLOCK.
     * But we are not sure whether all the block status are reported to the master
     * val blocks = blockLocations.asScala.keys.flatMap(_.asRDDId).filter(_.rddId == rddId)
     * blocks.foreach { blockId =>
     * val bms: mutable.HashSet[BlockManagerId] = blockLocations.get(blockId)
     * bms.foreach{
     * bm =>
     * {
     * blockManagerInfo.get(bm)
     * .get.slaveEndpoint.ask[Boolean](DecreaseBlockRefCount(blockId))
     * logInfo(s"yyh: Telling $bm to decrease the ref count of $blockId")
     * }
     * }
     **
     *}
     */
  }

  def calculateEffectiveHit(hitBlockList: mutable.MutableList[BlockId]): Unit = {
    printf(s"this hitBlockList: $hitBlockList")
    val hitCount = mutable.HashMap[Int, Int]() // effective hit blocks of each rdd
    for( blockId <- hitBlockList) {
      val rddId = blockId.asRDDId.map(_.rddId).get
      val index = blockId.asRDDId.toString.split("_")(2).stripSuffix(")").toInt
      val peerRDDId = peerProfile(rddId)
      val peerBlockId = new RDDBlockId(peerRDDId, index)
      if (hitBlockList.contains(peerBlockId)) {
        printf("Hit count of RDD Id %s increased 1 by %s\n", rddId, blockId.toString)
        if (hitCount.contains(rddId)) {
          hitCount(rddId) += 1
        }
        else {
          hitCount(rddId) = 1
        }
      }
    }
    var thisTaskHit = 0
    var thisStageHit = 0
    for((rddId, count) <- hitCount) {
      thisTaskHit += count
      if(count == partition) (thisStageHit += 1)
    }
    this.synchronized {
      taskHit += thisTaskHit / 2
      stageHit += thisStageHit / 2
    } // each hit is counted twice for ZippedRDD2
  }

  def calculateEffectiveHit(): (Int, Int) = {
    val hitCount = mutable.HashMap[Int, Int]() // effective hit blocks of each rdd
    for( blockId <- totalHitBlockList) {
      val rddId = blockId.asRDDId.map(_.rddId).get
      val index = blockId.asRDDId.toString.split("_")(2).stripSuffix(")").toInt
      val peerRDDId = peerProfile(rddId)
      val peerBlockId = new RDDBlockId(peerRDDId, index)
      if (totalHitBlockList.contains(peerBlockId)) {
        printf("Hit count of RDD Id %s increased 1 by %s\n", rddId, blockId.toString)
        if (hitCount.contains(rddId)) {
          hitCount(rddId) += 1
        }
        else {
          hitCount(rddId) = 1
        }
      }
    }
    var taskHit = 0
    var stageHit = 0
    for((rddId, count) <- hitCount) {
      taskHit += count
      if(count == partition) (stageHit += 1)
    }
    this.synchronized {
      taskHit += taskHit / 2
      stageHit += stageHit / 2
    } // each hit is counted twice for ZippedRDD2
    (taskHit, stageHit)
  }

}

@DeveloperApi
case class BlockStatus(storageLevel: StorageLevel, memSize: Long, diskSize: Long) {
  def isCached: Boolean = memSize + diskSize > 0
}

@DeveloperApi
object BlockStatus {
  def empty: BlockStatus = BlockStatus(StorageLevel.NONE, memSize = 0L, diskSize = 0L)
}

private[spark] class BlockManagerInfo(
    val blockManagerId: BlockManagerId,
    timeMs: Long,
    val maxOnHeapMem: Long,
    val maxOffHeapMem: Long,
    val slaveEndpoint: RpcEndpointRef,
    val externalShuffleServiceBlockStatus: Option[JHashMap[BlockId, BlockStatus]])
  extends Logging {

  val maxMem = maxOnHeapMem + maxOffHeapMem

  val externalShuffleServiceEnabled = externalShuffleServiceBlockStatus.isDefined

  private var _lastSeenMs: Long = timeMs
  private var _remainingMem: Long = maxMem

  // Mapping from block id to its status.
  private val _blocks = new JHashMap[BlockId, BlockStatus]

  def getStatus(blockId: BlockId): Option[BlockStatus] = Option(_blocks.get(blockId))

  def updateLastSeenMs(): Unit = {
    _lastSeenMs = System.currentTimeMillis()
  }

  def updateBlockInfo(
      blockId: BlockId,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long): Unit = {

    updateLastSeenMs()

    val blockExists = _blocks.containsKey(blockId)
    var originalMemSize: Long = 0
    var originalDiskSize: Long = 0
    var originalLevel: StorageLevel = StorageLevel.NONE

    if (blockExists) {
      // The block exists on the slave already.
      val blockStatus: BlockStatus = _blocks.get(blockId)
      originalLevel = blockStatus.storageLevel
      originalMemSize = blockStatus.memSize
      originalDiskSize = blockStatus.diskSize

      if (originalLevel.useMemory) {
        _remainingMem += originalMemSize
      }
    }

    if (storageLevel.isValid) {
      /* isValid means it is either stored in-memory or on-disk.
       * The memSize here indicates the data size in or dropped from memory,
       * externalBlockStoreSize here indicates the data size in or dropped from externalBlockStore,
       * and the diskSize here indicates the data size in or dropped to disk.
       * They can be both larger than 0, when a block is dropped from memory to disk.
       * Therefore, a safe way to set BlockStatus is to set its info in accurate modes. */
      var blockStatus: BlockStatus = null
      if (storageLevel.useMemory) {
        blockStatus = BlockStatus(storageLevel, memSize = memSize, diskSize = 0)
        _blocks.put(blockId, blockStatus)
        _remainingMem -= memSize
        if (blockExists) {
          logInfo(s"Updated $blockId in memory on ${blockManagerId.hostPort}" +
            s" (current size: ${Utils.bytesToString(memSize)}," +
            s" original size: ${Utils.bytesToString(originalMemSize)}," +
            s" free: ${Utils.bytesToString(_remainingMem)})")
        } else {
          logInfo(s"Added $blockId in memory on ${blockManagerId.hostPort}" +
            s" (size: ${Utils.bytesToString(memSize)}," +
            s" free: ${Utils.bytesToString(_remainingMem)})")
        }
      }
      if (storageLevel.useDisk) {
        blockStatus = BlockStatus(storageLevel, memSize = 0, diskSize = diskSize)
        _blocks.put(blockId, blockStatus)
        if (blockExists) {
          logInfo(s"Updated $blockId on disk on ${blockManagerId.hostPort}" +
            s" (current size: ${Utils.bytesToString(diskSize)}," +
            s" original size: ${Utils.bytesToString(originalDiskSize)})")
        } else {
          logInfo(s"Added $blockId on disk on ${blockManagerId.hostPort}" +
            s" (size: ${Utils.bytesToString(diskSize)})")
        }
      }

      externalShuffleServiceBlockStatus.foreach { shuffleServiceBlocks =>
        if (!blockId.isBroadcast && blockStatus.diskSize > 0) {
          shuffleServiceBlocks.put(blockId, blockStatus)
        }
      }
    } else if (blockExists) {
      // If isValid is not true, drop the block.
      _blocks.remove(blockId)
      externalShuffleServiceBlockStatus.foreach { blockStatus =>
        blockStatus.remove(blockId)
      }
      if (originalLevel.useMemory) {
        logInfo(s"Removed $blockId on ${blockManagerId.hostPort} in memory" +
          s" (size: ${Utils.bytesToString(originalMemSize)}," +
          s" free: ${Utils.bytesToString(_remainingMem)})")
      }
      if (originalLevel.useDisk) {
        logInfo(s"Removed $blockId on ${blockManagerId.hostPort} on disk" +
          s" (size: ${Utils.bytesToString(originalDiskSize)})")
      }
    }
  }

  def removeBlock(blockId: BlockId): Unit = {
    if (_blocks.containsKey(blockId)) {
      _remainingMem += _blocks.get(blockId).memSize
      _blocks.remove(blockId)
      externalShuffleServiceBlockStatus.foreach { blockStatus =>
        blockStatus.remove(blockId)
      }
    }
  }

  def remainingMem: Long = _remainingMem

  def lastSeenMs: Long = _lastSeenMs

  def blocks: JHashMap[BlockId, BlockStatus] = _blocks

  override def toString: String = "BlockManagerInfo " + timeMs + " " + _remainingMem

  def clear(): Unit = {
    _blocks.clear()
  }
}
