package org.apache.rocketmq.store;

import org.apache.rocketmq.common.ServiceThread;

/**
 * @author fankai
 */
abstract class FlushCommitLogService extends ServiceThread {

    protected static final int RETRY_TIMES_OVER = 10;
}
