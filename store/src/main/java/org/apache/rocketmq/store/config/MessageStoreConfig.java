/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.config;

import lombok.Data;
import org.apache.rocketmq.common.annotation.ImportantField;
import org.apache.rocketmq.store.ConsumeQueue;

import java.io.File;

@Data
public class MessageStoreConfig {
    //The root directory in which the log data is kept
    @ImportantField
    private String storePathRootDir = System.getProperty("user.home") + File.separator + "proj/store";

    //The directory in which the commitlog is kept
    @ImportantField
    private String storePathCommitLog = System.getProperty("user.home") + File.separator + "proj/store"
        + File.separator + "commitlog";

    // CommitLog file size,default is 1G
    private int mapedFileSizeCommitLog = 1024 * 1024 * 1024;
    // ConsumeQueue file size,default is 30W
    private int mapedFileSizeConsumeQueue = 300000 * ConsumeQueue.CQ_STORE_UNIT_SIZE;
    // enable consume queue ext
    private boolean enableConsumeQueueExt = false;
    // ConsumeQueue extend file size, 48M
    private int mappedFileSizeConsumeQueueExt = 48 * 1024 * 1024;
    // Bit count of filter bit map.
    // this will be set by pipe of calculate filter bit map.
    private int bitMapLengthConsumeQueueExt = 64;

    // CommitLog flush interval
    // flush data to disk
    @ImportantField
    private int flushIntervalCommitLog = 500;

    // Only used if TransientStorePool enabled
    // flush data to FileChannel
    @ImportantField
    private int commitIntervalCommitLog = 200;

    /**
     * introduced since 4.0.x. Determine whether to use mutex reentrantLock when putting message.<br/>
     * By default it is set to false indicating using spin lock when putting message.
     */
    private boolean useReentrantLockWhenPutMessage = false;

    // Whether schedule flush,default is real-time
    @ImportantField
    private boolean flushCommitLogTimed = false;
    // ConsumeQueue flush interval
    private int flushIntervalConsumeQueue = 1000;
    // Resource reclaim interval
    private int cleanResourceInterval = 10000;
    // CommitLog removal interval
    private int deleteCommitLogFilesInterval = 100;
    // ConsumeQueue removal interval
    private int deleteConsumeQueueFilesInterval = 100;
    private int destroyMapedFileIntervalForcibly = 1000 * 120;
    private int redeleteHangedFileInterval = 1000 * 120;
    // When to delete,default is at 4 am
    @ImportantField
    private String deleteWhen = "04";
    private int diskMaxUsedSpaceRatio = 75;
    // The number of hours to keep a log file before deleting it (in hours)
    @ImportantField
    private int fileReservedTime = 72;
    // Flow control for ConsumeQueue
    private int putMsgIndexHightWater = 600000;
    // The maximum size of a single log file,default is 512K
    private int maxMessageSize = 1024 * 1024 * 4;
    // Whether check the CRC32 of the records consumed.
    // This ensures no on-the-wire or on-disk corruption to the messages occurred.
    // This check adds some overhead,so it may be disabled in cases seeking extreme performance.
    private boolean checkCRCOnRecover = true;
    // How many pages are to be flushed when flush CommitLog
    private int flushCommitLogLeastPages = 4;
    // How many pages are to be committed when commit data to file
    private int commitCommitLogLeastPages = 4;
    // Flush page size when the disk in warming state
    private int flushLeastPagesWhenWarmMapedFile = 1024 / 4 * 16;
    // How many pages are to be flushed when flush ConsumeQueue
    private int flushConsumeQueueLeastPages = 2;
    private int flushCommitLogThoroughInterval = 1000 * 10;
    private int commitCommitLogThoroughInterval = 200;
    private int flushConsumeQueueThoroughInterval = 1000 * 60;
    @ImportantField
    private int maxTransferBytesOnMessageInMemory = 1024 * 256;
    @ImportantField
    private int maxTransferCountOnMessageInMemory = 32;
    @ImportantField
    private int maxTransferBytesOnMessageInDisk = 1024 * 64;
    @ImportantField
    private int maxTransferCountOnMessageInDisk = 8;
    @ImportantField
    private int accessMessageInMemoryMaxRatio = 40;
    @ImportantField
    private boolean messageIndexEnable = true;
    private int maxHashSlotNum = 5000000;
    private int maxIndexNum = 5000000 * 4;
    private int maxMsgsNumBatch = 64;
    @ImportantField
    private boolean messageIndexSafe = false;
    private int haListenPort = 10912;
    private int haSendHeartbeatInterval = 1000 * 5;
    private int haHousekeepingInterval = 1000 * 20;
    private int haTransferBatchSize = 1024 * 32;
    @ImportantField
    private String haMasterAddress = null;
    private int haSlaveFallbehindMax = 1024 * 1024 * 256;
    @ImportantField
    private BrokerRole brokerRole = BrokerRole.ASYNC_MASTER;
    @ImportantField
    private FlushDiskType flushDiskType = FlushDiskType.ASYNC_FLUSH;
    private int syncFlushTimeout = 1000 * 5;
    private String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";
    private long flushDelayOffsetInterval = 1000 * 10;
    @ImportantField
    private boolean cleanFileForciblyEnable = true;
    private boolean warmMapedFileEnable = false;
    private boolean offsetCheckInSlave = false;
    private boolean debugLockEnable = false;
    private boolean duplicationEnable = false;
    private boolean diskFallRecorded = true;
    private long osPageCacheBusyTimeOutMills = 1000;
    private int defaultQueryMaxNum = 32;

    @ImportantField
    private boolean transientStorePoolEnable = false;
    private int transientStorePoolSize = 5;
    private boolean fastFailIfNoBufferInStorePool = false;

    private boolean enableDLegerCommitLog = false;
    private String dLegerGroup;
    private String dLegerPeers;
    private String dLegerSelfId;

    public String getdLegerGroup() {
        return dLegerGroup;
    }

    public String getdLegerPeers() {
        return dLegerPeers;
    }

    public String getdLegerSelfId() {
        return dLegerSelfId;
    }

    public void setdLegerGroup(String dLegerGroup) {
        this.dLegerGroup = dLegerGroup;
    }

    public void setdLegerPeers(String dLegerPeers) {
        this.dLegerPeers = dLegerPeers;
    }

    public void setdLegerSelfId(String dLegerSelfId) {
        this.dLegerSelfId = dLegerSelfId;
    }

    public void setBrokerRole(BrokerRole brokerRole) {
        this.brokerRole = brokerRole;
    }

    public void setFlushDiskType(FlushDiskType type) {
        this.flushDiskType = type;
    }

    public void setBrokerRole(String brokerRole) {
        this.brokerRole = BrokerRole.valueOf(brokerRole);
    }

    public void setFlushDiskType(String type) {
        this.flushDiskType = FlushDiskType.valueOf(type);
    }

}
