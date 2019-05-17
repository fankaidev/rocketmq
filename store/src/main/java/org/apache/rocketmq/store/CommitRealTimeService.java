package org.apache.rocketmq.store;

/**
 * @author fankai
 */
class CommitRealTimeService extends FlushCommitLogService {

    private CommitLog commitLog;
    private long lastCommitTimestamp = 0;

    public CommitRealTimeService(CommitLog commitLog) {this.commitLog = commitLog;}

    @Override
    public String getServiceName() {
        return CommitRealTimeService.class.getSimpleName();
    }

    @Override
    public void run() {
        CommitLog.log.info(this.getServiceName() + " service started");
        while (!this.isStopped()) {
            int interval = commitLog.defaultMessageStore.getMessageStoreConfig().getCommitIntervalCommitLog();

            int commitDataLeastPages = commitLog.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogLeastPages();

            int commitDataThoroughInterval = commitLog.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogThoroughInterval();

            long begin = System.currentTimeMillis();
            if (begin >= (this.lastCommitTimestamp + commitDataThoroughInterval)) {
                this.lastCommitTimestamp = begin;
                commitDataLeastPages = 0;
            }

            try {
                boolean result = commitLog.mappedFileQueue.commit(commitDataLeastPages);
                long end = System.currentTimeMillis();
                if (!result) {
                    this.lastCommitTimestamp = end; // result = false means some data committed.
                    //now wake up flush thread.
                    commitLog.flushCommitLogService.wakeup();
                }

                if (end - begin > 500) {
                    CommitLog.log.info("Commit data to file costs {} ms", end - begin);
                }
                this.waitForRunning(interval);
            } catch (Throwable e) {
                CommitLog.log.error(this.getServiceName() + " service has exception. ", e);
            }
        }

        boolean result = false;
        for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
            result = commitLog.mappedFileQueue.commit(0);
            CommitLog.log.info(
                    this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " +
                            (result ? "OK" : "Not OK"));
        }
        CommitLog.log.info(this.getServiceName() + " service end");
    }
}
