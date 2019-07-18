public class ClientStatistics {
    private int index;
    private long[] transactionCount;
    private long[] executionTime;

    public ClientStatistics(int index, long[] transactionCount, long[] executionTime) {
        this.index = index;
        this.transactionCount = transactionCount;
        this.executionTime = executionTime;
    }

    public long[] getAllTransactionCount() {
        return transactionCount;
    }

    public long[] getAllExecutionTime() {
        return executionTime;
    }

    public long getTransactionCount(int index) {
        return transactionCount[index];
    }

    public long getExecutionTime(int index) {
        return executionTime[index];
    }

    public int getIndex() {
        return index;
    }

    public void setTransactionCount(long[] transactionCount) {
        this.transactionCount = transactionCount;
    }

    public void setExecutionTime(long[] executionTime) {
        this.executionTime = executionTime;
    }

    public long getTotalTransactionCount() {
        long total = 0;
        for (long count : transactionCount) {
            total += count;
        }
        return total;
    }

    public long getTotalExecutionTime() {
        long total = 0;
        for (long time : executionTime) {
            total += time;
        }
        return total;
    }

    public double getThroughput() {
        double duration = (double) this.getTotalExecutionTime() / 1000000000;
        return (double) this.getTotalTransactionCount() / duration;
    }
}
