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
package org.apache.rocketmq.common;

import lombok.Data;
import org.apache.rocketmq.common.annotation.ImportantField;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Data
public class BrokerConfig {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));
    @ImportantField
    private String namesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY, System.getenv(MixAll.NAMESRV_ADDR_ENV));
    @ImportantField
    private String brokerIP1 = RemotingUtil.getLocalAddress();
    private String brokerIP2 = RemotingUtil.getLocalAddress();
    @ImportantField
    private String brokerName = localHostName();
    @ImportantField
    private String brokerClusterName = "DefaultCluster";
    @ImportantField
    private long brokerId = MixAll.MASTER_ID;
    private int brokerPermission = PermName.PERM_READ | PermName.PERM_WRITE;
    private int defaultTopicQueueNums = 8;
    @ImportantField
    private boolean autoCreateTopicEnable = true;

    private boolean clusterTopicEnable = true;

    private boolean brokerTopicEnable = true;
    @ImportantField
    private boolean autoCreateSubscriptionGroup = true;
    private String messageStorePlugIn = "";
    @ImportantField
    private String msgTraceTopicName = MixAll.RMQ_SYS_TRACE_TOPIC;
    @ImportantField
    private boolean traceTopicEnable = false;
    /**
     * thread numbers for send message thread pool, since spin lock will be used by default since 4.0.x, the default
     * value is 1.
     */
    private int sendMessageThreadPoolNums = 1; //16 + Runtime.getRuntime().availableProcessors() * 4;
    private int pullMessageThreadPoolNums = 16 + Runtime.getRuntime().availableProcessors() * 2;
    private int queryMessageThreadPoolNums = 8 + Runtime.getRuntime().availableProcessors();

    private int adminBrokerThreadPoolNums = 16;
    private int clientManageThreadPoolNums = 32;
    private int consumerManageThreadPoolNums = 32;
    private int heartbeatThreadPoolNums = Math.min(32, Runtime.getRuntime().availableProcessors());

    /**
     * Thread numbers for EndTransactionProcessor
     */
    private int endTransactionThreadPoolNums = 8 + Runtime.getRuntime().availableProcessors() * 2;

    private int flushConsumerOffsetInterval = 1000 * 5;

    private int flushConsumerOffsetHistoryInterval = 1000 * 60;

    @ImportantField
    private boolean rejectTransactionMessage = false;
    @ImportantField
    private boolean fetchNamesrvAddrByAddressServer = false;
    private int sendThreadPoolQueueCapacity = 10000;
    private int pullThreadPoolQueueCapacity = 100000;
    private int queryThreadPoolQueueCapacity = 20000;
    private int clientManagerThreadPoolQueueCapacity = 1000000;
    private int consumerManagerThreadPoolQueueCapacity = 1000000;
    private int heartbeatThreadPoolQueueCapacity = 50000;
    private int endTransactionPoolQueueCapacity = 100000;

    private int filterServerNums = 0;

    private boolean longPollingEnable = true;

    private long shortPollingTimeMills = 1000;

    private boolean notifyConsumerIdsChangedEnable = true;

    private boolean highSpeedMode = false;

    private boolean commercialEnable = true;
    private int commercialTimerCount = 1;
    private int commercialTransCount = 1;
    private int commercialBigCount = 1;
    private int commercialBaseCount = 1;

    private boolean transferMsgByHeap = true;
    private int maxDelayTime = 40;

    private String regionId = MixAll.DEFAULT_TRACE_REGION_ID;
    private int registerBrokerTimeoutMills = 6000;

    private boolean slaveReadEnable = false;

    private boolean disableConsumeIfConsumerReadSlowly = false;
    private long consumerFallbehindThreshold = 1024L * 1024 * 1024 * 16;

    private boolean brokerFastFailureEnable = true;
    private long waitTimeMillsInSendQueue = 200;
    private long waitTimeMillsInPullQueue = 5 * 1000;
    private long waitTimeMillsInHeartbeatQueue = 31 * 1000;
    private long waitTimeMillsInTransactionQueue = 3 * 1000;

    private long startAcceptSendRequestTimeStamp = 0L;

    private boolean traceOn = true;

    // Switch of filter bit map calculation.
    // If switch on:
    // 1. Calculate filter bit map when construct queue.
    // 2. Filter bit map will be saved to consume queue extend file if allowed.
    private boolean enableCalcFilterBitMap = false;

    // Expect num of consumers will use filter.
    private int expectConsumerNumUseFilter = 32;

    // Error rate of bloom filter, 1~100.
    private int maxErrorRateOfBloomFilter = 20;

    //how long to clean filter data after dead.Default: 24h
    private long filterDataCleanTimeSpan = 24 * 3600 * 1000;

    // whether do filter when retry.
    private boolean filterSupportRetry = false;
    private boolean enablePropertyFilter = false;

    private boolean compressedRegister = false;

    private boolean forceRegister = true;

    /**
     * This configurable item defines interval of topics registration of broker to name server. Allowing values are
     * between 10, 000 and 60, 000 milliseconds.
     */
    private int registerNameServerPeriod = 1000 * 30;

    /**
     * The minimum time of the transactional message  to be checked firstly, one message only exceed this time interval
     * that can be checked.
     */
    @ImportantField
    private long transactionTimeOut = 6 * 1000;

    /**
     * The maximum number of times the message was checked, if exceed this value, this message will be discarded.
     */
    @ImportantField
    private int transactionCheckMax = 15;

    /**
     * Transaction message check interval.
     */
    @ImportantField
    private long transactionCheckInterval = 60 * 1000;

    /**
     * Acl feature switch
     */
    @ImportantField
    private boolean aclEnable = false;

    public static String localHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            log.error("Failed to obtain the host name", e);
        }

        return "DEFAULT_BROKER";
    }

}
