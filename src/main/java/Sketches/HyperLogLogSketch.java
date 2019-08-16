package Sketches;

import Sketches.HashFunctions.PairwiseIndependentHashFunctions;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;

public class HyperLogLogSketch implements Sketch<Object>, Serializable {

    private int regNum; //number of registers
    private int logRegNum;
    private byte[] registers;
    public long distinctItemCount; // Field so that value is accessible after serializing
    private PairwiseIndependentHashFunctions hashFunctions;

    /**
     * @param logRegNum the logarithm of the number of registers. should be in 4...16
     *        Going beyond 16 would make the data structure big for no good reason.
     *        Setting logRegNum in at 10-12 should give roughly 2% accuracy most of the time
     */
    public HyperLogLogSketch(Integer logRegNum, Long seed) {
        HyperLogLogSketch.checkSpaceValid(logRegNum);
        this.regNum = 1 << logRegNum;
        this.registers = new byte[this.regNum];
        this.logRegNum = logRegNum;
        this.hashFunctions = new PairwiseIndependentHashFunctions(2, seed);
    }

    @Override
    public void update(Object t) {

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
     * @param sketch the sketch to merge
     * @return the merged HyperLogLogSketch Datastructure
     */
    public Sketch<Object> merge(Sketch sketch) throws IllegalArgumentException{
        if (sketch.getClass().isInstance(HyperLogLogSketch.class)){
            throw new IllegalArgumentException("Sketches can only be merged with other Sketches of the same Type \n" +
                    "otherHLL.getClass() = " + sketch.getClass() + "\n" +
                    "otherHLL.getClass().isInstance(HyperLogLogSketch.class = false");
        }

        HyperLogLogSketch otherHLL = (HyperLogLogSketch) sketch;

        if ((otherHLL.getRegNum() != this.regNum) || (!otherHLL.getHashFunctions().equals(hashFunctions)))
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

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {

        out.writeInt(this.regNum);
        out.writeInt(this.logRegNum);
        out.write(this.registers);
        out.writeLong(this.distinctItemCount);
        out.writeObject(this.hashFunctions);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException{
        this.regNum = in.readInt();
        this.logRegNum = in.readInt();
        for (int i = 0; i < regNum; i++) {
            registers[i] = in.readByte();
        }
        this.distinctItemCount = in.readLong();
        this.hashFunctions = (PairwiseIndependentHashFunctions) in.readObject();
    }

    private void readObjectNoData() throws ObjectStreamException {

    }

    @Override
    public String toString(){
        String s = "------- HyperLogLogSketch Information ---------- \n" +
                "number of Registers: " + regNum + "\n" +
                "estimated distinct items: " + this.distinctItemsEstimator();
        return s;
    }
}
