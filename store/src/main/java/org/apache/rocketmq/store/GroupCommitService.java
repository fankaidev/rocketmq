package org.apache.rocketmq.store;

import org.apache.rocketmq.store.CommitLog.GroupCommitRequest;

import java.util.ArrayList;
import java.util.List;

/**
 * GroupCommit Service
 */
class GroupCommitService extends FlushCommitLogService {

    private CommitLog commitLog;
    private volatile List<GroupCommitRequest> requestsWrite = new ArrayList<GroupCommitRequest>();
    private volatile List<GroupCommitRequest> requestsRead = new ArrayList<GroupCommitRequest>();

    public GroupCommitService(CommitLog commitLog) {this.commitLog = commitLog;}

    public synchronized void putRequest(final GroupCommitRequest request) {
        synchronized (this.requestsWrite) {
            this.requestsWrite.add(request);
        }
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }
    }

    private void swapRequests() {
        List<GroupCommitRequest> tmp = this.requestsWrite;
        this.requestsWrite = this.requestsRead;
        this.requestsRead = tmp;
    }

    private void doCommit() {
        synchronized (this.requestsRead) {
            if (!this.requestsRead.isEmpty()) {
                for (GroupCommitRequest req : this.requestsRead) {
                    // There may be a message in the next file, so a maximum of
                    // two times the flush
                    boolean flushOK = false;
                    for (int i = 0; i < 2 && !flushOK; i++) {
                        flushOK = commitLog.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();

                        if (!flushOK) {
                            commitLog.mappedFileQueue.flush(0);
                        }
                    }

                    req.wakeupCustomer(flushOK);
                }

                long storeTimestamp = commitLog.mappedFileQueue.getStoreTimestamp();
                if (storeTimestamp > 0) {
                    commitLog.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                }

                this.requestsRead.clear();
            } else {
                // Because of individual messages is set to not sync flush, it
                // will come to this process
                commitLog.mappedFileQueue.flush(0);
            }
        }
    }

    public void run() {
        CommitLog.log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                this.waitForRunning(10);
                this.doCommit();
            } catch (Exception e) {
                CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        // Under normal circumstances shutdown, wait for the arrival of the
        // request, and then flush
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            CommitLog.log.warn("GroupCommitService Exception, ", e);
        }

        synchronized (this) {
            this.swapRequests();
        }

        this.doCommit();

        CommitLog.log.info(this.getServiceName() + " service end");
    }

    @Override
    protected void onWaitEnd() {
        this.swapRequests();
    }

    @Override
    public String getServiceName() {
        return GroupCommitService.class.getSimpleName();
    }

    @Override
    public long getJointime() {
        return 1000 * 60 * 5;
    }
}
