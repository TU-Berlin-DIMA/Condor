package Sketches;

import Sketches.HashFunctions.PairwiseIndependentHashFunctions;

public class HyperLogLog<T> implements Sketch<T, Integer>{

    private final int regNum; //number of registers
    private final int logRegNum;
    private final byte[] registers;
    public long distinctItemCount; // Field so that value is accessible after serializing
    private final PairwiseIndependentHashFunctions hashFunctions;

    /**
     * @param logRegNum the logarithm of the number of registers. should be in 4...16
     *        Going beyond 16 would make the data structure big for no good reason.
     *        Setting logRegNum in at 10-12 should give roughly 2% accuracy most of the time
     */
    public HyperLogLog(int logRegNum, long seed) {
        HyperLogLog.checkSpaceValid(logRegNum);
        this.regNum = 1 << logRegNum;
        this.registers = new byte[this.regNum];
        this.logRegNum = logRegNum;
        this.hashFunctions = new PairwiseIndependentHashFunctions(2, seed);
    }

    @Override
    public void update(T t) {

        long hash = hashFunctions.hash(t)[0];
        long firstBits = ((long) hashFunctions.hash(t)[1]) << 32;
        hash += firstBits;

        int index = (int)(hash >>> (Long.SIZE - this.logRegNum));
        byte zeros = (byte) (Long.numberOfTrailingZeros(hash) + 1);
        if (zeros > this.registers[index]){
            this.registers[index] = zeros;
        }
    }

    /**
     * adds the long 'itemHash' to the data structure.
     * Uses the first bits to identify the register and then counts trailing zeros
     * @param itemHash already assumed to be a random hash of the item
     */
    private void add(long itemHash) {
        int index =  (int) itemHash >>> (Long.SIZE - this.logRegNum);
        byte zeros = (byte) (Long.numberOfTrailingZeros(itemHash) + 1);
        if (zeros > this.registers[index])
            this.registers[index] = zeros;
    }

    /**
     *
     * @param otherHLL
     * @return the merged HyperLogLog Datastructure
     */
    public HyperLogLog merge(HyperLogLog otherHLL) {
        if ((otherHLL.regNum != this.regNum) || (!otherHLL.getHashFunctions().equals(hashFunctions)))
            throw new IllegalArgumentException("attempted union of non matching HLogLog classes");
        for (int i = 0; i < this.regNum; i++)
            this.registers[i] = (byte) Integer.max(this.registers[i], otherHLL.registers[i]);
        this.distinctItemsEstimator();
        return this;
    }

    public int getRegNum() {
        return regNum;
    }

    public int getLogRegNum() {
        return logRegNum;
    }

    public byte[] getRegisters() {
        return registers;
    }

    public long getDistinctItemCount() {
        return distinctItemCount;
    }

    public PairwiseIndependentHashFunctions getHashFunctions() {
        return hashFunctions;
    }

    /**
     * @return an estimation of the number of distinct items
     */
    public long distinctItemsEstimator() {
        double alpha;
        switch (this.logRegNum) { //set the parameters for the log log estimator
            case 4: alpha = 0.673;
                break;
            case 5: alpha = 0.697;
                break;
            case 6: alpha = 0.709;
                break;
            default: alpha = 0.7213 / (1 + (1.079 / this.regNum));
        }
        double rawEstimate = 0;
        int zeroRegs = 0;
        for (int i = 0; i < this.regNum; i++) {
            rawEstimate += Math.pow(2, -this.registers[i]);
            if (this.registers[i] == 0)
                zeroRegs ++;
        }
        rawEstimate = 1 / rawEstimate;
        rawEstimate = rawEstimate * alpha * this.regNum * this.regNum;
        long result = Math.round(rawEstimate);
        if ((zeroRegs > 0) && (rawEstimate < (2.5 * this.regNum)))
            result = Math.round(this.regNum * (Math.log(this.regNum / (double) zeroRegs)));
        else if (rawEstimate > (Integer.MAX_VALUE / 30 ))
            result = (long) (- Math.pow(2,32) * Math.log(1 - (rawEstimate /  Math.pow(2,32))));
        this.distinctItemCount = result;
        return result;
    }

    /**
     * @param logSpaceSize the logarithm of the number of registers in the HLL structure.
     * Going beyond 16 is too big and does not
     * add accuracy. Going below 4 does not save Space.
     */
    public static void checkSpaceValid(int logSpaceSize) {
        if ((logSpaceSize > 16) || (logSpaceSize < 4))
            throw new IllegalArgumentException("HLogLog initialized with logSpaceSize out of range");
    }

}
