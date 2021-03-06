package de.tub.dima.condor.core.synopsis;


public abstract class StratifiedSynopsis<Partition>{
    private Partition partitionValue = null;

    public Partition getPartitionValue() {
        return partitionValue;
    }

    public void setPartitionValue(Partition partitionValue) {
        if (this.partitionValue == null) {
            this.partitionValue = partitionValue;
        }
    }
}
