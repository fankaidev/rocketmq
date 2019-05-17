package org.apache.rocketmq.store;

/**
 * @author fankai
 */
class FlushRealTimeService extends FlushCommitLogService {

    private CommitLog commitLog;
    private long lastFlushTimestamp = 0;
    private long printTimes = 0;

    public FlushRealTimeService(CommitLog commitLog) {this.commitLog = commitLog;}

    public void run() {
        CommitLog.log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            boolean flushCommitLogTimed = commitLog.defaultMessageStore.getMessageStoreConfig().isFlushCommitLogTimed();

            int interval = commitLog.defaultMessageStore.getMessageStoreConfig().getFlushIntervalCommitLog();
            int flushPhysicQueueLeastPages = commitLog.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogLeastPages();

            int flushPhysicQueueThoroughInterval =
                    commitLog.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogThoroughInterval();

            boolean printFlushProgress = false;

            // Print flush progress
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicQueueThoroughInterval)) {
                this.lastFlushTimestamp = currentTimeMillis;
                flushPhysicQueueLeastPages = 0;
                printFlushProgress = (printTimes++ % 10) == 0;
            }

            try {
                if (flushCommitLogTimed) {
                    Thread.sleep(interval);
                } else {
                    this.waitForRunning(interval);
                }

                if (printFlushProgress) {
                    this.printFlushProgress();
                }

                long begin = System.currentTimeMillis();
                commitLog.mappedFileQueue.flush(flushPhysicQueueLeastPages);
                long storeTimestamp = commitLog.mappedFileQueue.getStoreTimestamp();
                if (storeTimestamp > 0) {
                    commitLog.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                }
                long past = System.currentTimeMillis() - begin;
                if (past > 500) {
                    CommitLog.log.info("Flush data to disk costs {} ms", past);
                }
            } catch (Throwable e) {
                CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                this.printFlushProgress();
            }
        }

        // Normal shutdown, to ensure that all the flush before exit
        boolean result = false;
        for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
            result = commitLog.mappedFileQueue.flush(0);
            CommitLog.log.info(
                    this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " +
                            (result ? "OK" : "Not OK"));
        }

        this.printFlushProgress();

        CommitLog.log.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
        return FlushRealTimeService.class.getSimpleName();
    }

    private void printFlushProgress() {
        // CommitLog.log.info("how much disk fall behind memory, "
        // + CommitLog.this.mappedFileQueue.howMuchFallBehind());
    }

    @Override
    public long getJointime() {
        return 1000 * 60 * 5;
    }
}
