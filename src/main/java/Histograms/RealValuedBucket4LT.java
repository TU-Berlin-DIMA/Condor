package Histograms;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;

public class RealValuedBucket4LT implements Serializable {
    int root, lowerLevels;
    double lowerBound, upperBound;

    /**
     * Initialise the 4LT Bucket with the given boundaries
     * @param lowerBound    inclusive
     * @param upperBound    exclusive
     * @throws IllegalArgumentException
     */
    public RealValuedBucket4LT(double lowerBound, double upperBound) throws IllegalArgumentException{
        if (upperBound < lowerBound){
            throw new IllegalArgumentException("upperBound must be greater than lowerBound!");
        }
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.root = 0;
        this.lowerLevels = 0;
    }

    /**
     * Method to build the Histogram Structure - specifically the lowerLevels
     * @param frequencies the respective frequency for the given low level bucket.
     */
    public void build(int[] frequencies) throws IllegalArgumentException, Exception {
        if(frequencies.length != 8){
            throw  new IllegalArgumentException("frequency array has to be of length 8!");
        }
        // set the frequency values for the root and lower levels
        int count3_1 = frequencies[0] + frequencies[1];
        int count3_3 = frequencies[4] + frequencies[5];
        int count2_1 = count3_1 + frequencies[2] + frequencies[3];
        root = count2_1 + count3_3 + frequencies[6] + frequencies[7];

        int delta2_1 = (int) Math.round(((double)(count2_1)/root) * (Math.pow(2, 6)-1)); // compute the first delta on the second level
        if (delta2_1 > 63){
            throw new Exception("Error in Code! -> delta2_1 takes more than six bits!!!");
        }
        lowerLevels = delta2_1 << 26; // store delta2_1 in the first six bits
        int delta3_1 = (int) Math.round(((double)(count3_1)/count2_1) * (Math.pow(2, 5)-1)); // compute the first delta on the third level
        lowerLevels += delta3_1 << 21; // store delta3_1 in the bits 6 to 11
        int delta3_3 = (int) Math.round(((double)(count3_3)/(root - count2_1)) * (Math.pow(2, 5)-1)); // compute the first delta on the third level
        lowerLevels += delta3_3 << 16; // store delta3_1 in the bits 11 to 16
        int delta4_1 = (int) Math.round(((double)(frequencies[0])/count3_1) * (Math.pow(2, 4)-1)); // compute the first delta on the fourth level
        lowerLevels += delta4_1 << 12; // store delta4_1 in the bits 16 to 20
        int delta4_3 = (int) Math.round(((double)(frequencies[2])/(count2_1 - count3_1)) * (Math.pow(2, 4)-1)); // compute the first delta on the fourth level
        lowerLevels += delta4_3 << 8; // store delta4_1 in the bits 16 to 20
        int delta4_5 = (int) Math.round(((double)(frequencies[4])/count3_3) * (Math.pow(2, 4)-1)); // compute the first delta on the fourth level
        lowerLevels += delta4_5 << 4; // store delta4_1 in the bits 16 to 20
        int delta4_7 = (int) Math.round(((double)(frequencies[6])/(root - count2_1 - count3_3)) * (Math.pow(2, 4)-1)); // compute the first delta on the fourth level
        lowerLevels += delta4_7;
    }

    public double getLowerBound() {
        return lowerBound;
    }

    public double getUpperBound() {
        return upperBound;
    }

    public int getRoot() {
        return root;
    }

    /**
     * Method which approximately computes the frequencies based on the given query-range.
     * The queries originally have to be based on equi-width buckets in order for this approximation to work!
     * @param queryLowerBound   lower bound of the query range inclusive
     * @param queryUpperBound   upper bound of the query range exclusive
     * @return  the approximate frequencies of the range query based on this 4LT Bucket
     */
    public int getFrequency(double queryLowerBound, double queryUpperBound){

        if (queryUpperBound < queryLowerBound){
            throw new IllegalArgumentException("upper Bound cannot be smaller than lower Bound!");
        }
        if (queryLowerBound <= lowerBound && queryUpperBound >= upperBound){
            return root; // if bounds contain bucket bounds completely simply return the root
        }
        if (queryLowerBound == queryUpperBound || queryLowerBound > this.upperBound || queryUpperBound < this.lowerBound){
            return 0; // if bounds are equal -> end of recursive calls
        }
        int frequency = 0;
        double distance = (upperBound-lowerBound) / 8d;
        double newQueryLowerBound = queryLowerBound;
        double newQueryUpperBound = queryUpperBound;

        double leftIndex = Math.max((queryLowerBound - lowerBound) * 8 / (upperBound-lowerBound), 0d);
        double rightIndex = Math.min((queryUpperBound - lowerBound) * 8 / (upperBound-lowerBound), 8d); // real valued right index exclusive

        int delta2_1 = lowerLevels >>> 26; // extract delta of second level - six bits
        int[] countL2 = new int[2];
        // compute the counts of the second level
        countL2[0] = (int) Math.round(delta2_1 / Math.pow(2, 6) * root);
        countL2[1] = root - countL2[0];
        if (leftIndex == 0 && rightIndex >= 4){ // first second level bucket fully contained in query range
            frequency += countL2[0];
            frequency += getFrequency((int) Math.ceil(distance*4) + lowerBound, queryUpperBound);
        }else if (leftIndex <= 4 && rightIndex == 8){ // second second level bucket fully contained in query range
            frequency += countL2[1];
            frequency += getFrequency(queryLowerBound,  (int) Math.floor(distance*4) + lowerBound);
        }else {
            // extract deltas of level 3 (5 bits each)
            int delta3_1 = (lowerLevels >>> 21) & 31;
            int delta3_3 = (lowerLevels >>> 16) & 31;
            // compute the counts of the third level
            int[] countL3 = new int[4];
            countL3[0] = (int) Math.round(delta3_1 / Math.pow(2, 5) * countL2[0]);
            countL3[1] = countL2[0] - countL3[0];
            countL3[2] = (int) Math.round(delta3_3 / Math.pow(2, 5) * countL2[1]);
            countL3[3] = countL2[1] - countL3[2];

            if ((Math.floor(rightIndex)-Math.ceil(leftIndex)) >= 3 ||
                    ((Math.floor(rightIndex)-Math.ceil(leftIndex)) >= 2 && Math.floor(rightIndex) % 2 == 0 )) { // A full 3rd level bucket can be added to frequency
                for (int i = 0; i < 4; i++) {
                    if (leftIndex <= i * 2 && rightIndex >= i * 2 + 2) {
                        frequency += countL3[i];
                        newQueryLowerBound = Math.max(newQueryLowerBound, (int) Math.ceil(distance * (i * 2 + 2)) + lowerBound);
                        newQueryUpperBound = Math.min(newQueryUpperBound, (int) Math.floor(distance * (i * 2)) + lowerBound);
                    }
                }
                frequency += getFrequency(queryLowerBound, newQueryUpperBound);
                frequency += getFrequency(newQueryLowerBound, queryUpperBound);
            } else {
                // extract the deltas of level 4 (4 bits each)
                int delta4_1 = (lowerLevels >>> 12) & 15;
                int delta4_3 = (lowerLevels >>> 8) & 15;
                int delta4_5 = (lowerLevels >>> 4) & 15;
                int delta4_7 = lowerLevels & 15;
                // compute the counts of the fourth level
                int[] countL4 = new int[8];
                countL4[0] = (int) Math.round(delta4_1 / Math.pow(2, 4) * countL3[0]);
                countL4[1] = countL3[0] - countL4[0];
                countL4[2] = (int) Math.round(delta4_3 / Math.pow(2, 4) * countL3[1]);
                countL4[3] = countL3[1] - countL4[2];
                countL4[4] = (int) Math.round(delta4_5 / Math.pow(2, 4) * countL3[2]);
                countL4[5] = countL3[2] - countL4[4];
                countL4[6] = (int) Math.round(delta4_7 / Math.pow(2, 4) * countL3[3]);
                countL4[7] = countL3[3] - countL4[6];

                if ((Math.floor(rightIndex)-Math.ceil(leftIndex)) >= 1){ // A full 4th level bucket can be added to frequency
                    for (int i = 0; i < 8; i++) {
                        if (leftIndex <= i && rightIndex >= i+1){
                            frequency += countL4[i];
                            newQueryLowerBound = Math.max(newQueryLowerBound, (int) Math.ceil(distance * i + 1 + lowerBound));
                            newQueryUpperBound = Math.min(newQueryUpperBound, (int) Math.floor(distance * i) + lowerBound);
                        }
                    }
                    frequency += getFrequency(queryLowerBound, newQueryUpperBound);
                    frequency += getFrequency(newQueryLowerBound, queryUpperBound);
                }else {
                    int bucketIndex = (int) Math.floor(leftIndex);
                    frequency += (rightIndex - leftIndex) * countL4[bucketIndex]; // add partial buckets to frequency
                }

            }
        }
        return frequency;
    }

    public String toString(){

        int delta2_1 = lowerLevels >>> 26; // extract delta of second level - six bits
        int[] countL2 = new int[2];
        // compute the counts of the second level
        countL2[0] = (int) Math.round(delta2_1 / Math.pow(2, 6) * root);
        countL2[1] = root - countL2[0];
        // extract deltas of level 3 (5 bits each)
        int delta3_1 = (lowerLevels >>> 21) & 31;
        int delta3_3 = (lowerLevels >>> 16) & 31;
        // compute the counts of the third level
        int[] countL3 = new int[4];
        countL3[0] = (int) Math.round(delta3_1 / Math.pow(2, 5) * countL2[0]);
        countL3[1] = countL2[0] - countL3[0];
        countL3[2] = (int) Math.round(delta3_3 / Math.pow(2, 5) * countL2[1]);
        countL3[3] = countL2[1] - countL3[2];
        // extract the deltas of level 4 (4 bits each)
        int delta4_1 = (lowerLevels >>> 12) & 15;
        int delta4_3 = (lowerLevels >>> 8) & 15;
        int delta4_5 = (lowerLevels >>> 4) & 15;
        int delta4_7 = lowerLevels & 15;
        // compute the counts of the fourth level
        int[] countL4 = new int[8];
        countL4[0] = (int) Math.round(delta4_1 / Math.pow(2, 4) * countL3[0]);
        countL4[1] = countL3[0] - countL4[0];
        countL4[2] = (int) Math.round(delta4_3 / Math.pow(2, 4) * countL3[1]);
        countL4[3] = countL3[1] - countL4[2];
        countL4[4] = (int) Math.round(delta4_5 / Math.pow(2, 4) * countL3[2]);
        countL4[5] = countL3[2] - countL4[4];
        countL4[6] = (int) Math.round(delta4_7 / Math.pow(2, 4) * countL3[3]);
        countL4[7] = countL3[3] - countL4[6];

        String s = "lower Bound: " + lowerBound + "  -  upper Bound: " + upperBound +"\n" +
                "root:                  " + root + "\n" +
                "          " + countL2[0] + "          |          " + countL2[1] +"\n" +
                "    " + countL3[0] + "     |     " + countL3[1] + "     |     " + countL3[2] + "     |     " + countL3[3] + "\n" +
                " "+countL4[0] + "  |  " + countL4[1] + "  |  " + countL4[2] + "  |  " + countL4[3] + "  |  " + countL4[4] + "  |  " + countL4[5] + "  |  " + countL4[6] + "  |  " + countL4[7];

        return s;
    }

    public RealValuedBucket4LT merge(RealValuedBucket4LT other){
        // TODO: implement this
        return null;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeInt(root);
        out.writeInt(lowerLevels);
        out.writeDouble(lowerBound);
        out.writeDouble(upperBound);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        root = in.readInt();
        lowerLevels = in.readInt();
        lowerBound = in.readDouble();
        upperBound = in.readDouble();
    }

    private void readObjectNoData() throws ObjectStreamException {
        System.out.println("readObjectNoData() called - should give an exception");
    }
}
