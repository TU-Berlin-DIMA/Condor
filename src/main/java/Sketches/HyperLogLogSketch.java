package Sketches;

import Sketches.HashFunctions.PairwiseIndependentHashFunctions;
import Synopsis.Synopsis;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;

/**
 * Implementation of the classical HyperLogLog Synopsis for count distinct queries on data streams.
 * This implementation uses 64 bit hash values and should therefore be able to handle up to 2^64 / 30
 * distinct items with adequate accuracy (given acceptable number of registers).
 *
 * @param <T> the type of elements maintained by this sketch
 *
 * @author Joscha von Hein
 */
public class HyperLogLogSketch<T> implements Synopsis<T>, Serializable {

    // TODO: write method / constructor which selects the logRegNum according to estimated error or available memory
    // TODO: implement the Sparse Representation as given in the Google Paper!

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

    /**
     * Method which updates the sketch with the input object
     *
     * @param element Tuple, Field or Object with which to update the sketch
     */
    @Override
    public void update(T element) {

        long hash = hashFunctions.hash(element)[0];
        long firstBits = ((long) hashFunctions.hash(element)[1]) << 32;
        hash += firstBits;

        int index = (int)(hash >>> (Long.SIZE - this.logRegNum));
        byte zeros = (byte) (Long.numberOfTrailingZeros(hash) + 1);
        if (zeros > this.registers[index]){
            this.registers[index] = zeros;
        }
    }

    /**
     * @param synopsis the synopsis to merge
     * @return the merged HyperLogLogSketch Datastructure
     */
    public HyperLogLogSketch merge(Synopsis synopsis){
        if (synopsis.getClass().isInstance(HyperLogLogSketch.class)){
            throw new IllegalArgumentException("Sketches can only be merged with other Sketches of the same Type \n" +
                    "otherHLL.getClass() = " + synopsis.getClass() + "\n" +
                    "otherHLL.getClass().isInstance(HyperLogLogSketch.class = false");
        }

        HyperLogLogSketch otherHLL = (HyperLogLogSketch) synopsis;

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
        switch (this.logRegNum) { //set the constant alpha for the log log estimator based on the number of registers
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
        // Calculate the Harmonic mean of the m registers
        for (int i = 0; i < this.regNum; i++) {
            rawEstimate += Math.pow(2, -this.registers[i]);
            if (this.registers[i] == 0)
                zeroRegs ++;
        }
        rawEstimate = 1 / rawEstimate; // final harmonic mean
        // final estimate
        rawEstimate = rawEstimate * alpha * this.regNum * this.regNum;
        long result = Math.round(rawEstimate);
        // if rawEstimate is below threshold of 5/2 m resort to Linear Counting (E = m * log (m/V) with V being the number of zero registers)
        if ((zeroRegs > 0) && (rawEstimate < (2.5 * this.regNum)))
            result = Math.round(this.regNum * (Math.log(this.regNum / (double) zeroRegs)));
        else if (rawEstimate > (Long.MAX_VALUE / 30))
            throw new IllegalArgumentException("The Estimate approaches Long.MAX_VALUE which possible amount of hash values. This will lead to a bad estimator. \n" +
                    "In order to better estimate that many distinct values change increase the possible amount of hash values!");
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
        String s = null;
        try {
            s = "------- HyperLogLogSketch Information ---------- \n" +
                    "number of Registers: " + regNum + "\n" +
                    "estimated distinct items: " + this.distinctItemsEstimator();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return s;
    }
}
