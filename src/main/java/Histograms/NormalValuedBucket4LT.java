package Histograms;

public class NormalValuedBucket4LT {
    int lowerBound, upperBound, root, lowerLevels;

    public NormalValuedBucket4LT(int lowerBound, int upperBound) throws IllegalArgumentException{
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
        lowerLevels += delta4_1 << 20; // store delta4_1 in the bits 16 to 20
        int delta4_3 = (int) Math.round(((double)(frequencies[2])/(count2_1 - count3_1)) * (Math.pow(2, 4)-1)); // compute the first delta on the fourth level
        lowerLevels += delta4_3 << 24; // store delta4_1 in the bits 16 to 20
        int delta4_5 = (int) Math.round(((double)(frequencies[4])/count3_3) * (Math.pow(2, 4)-1)); // compute the first delta on the fourth level
        lowerLevels += delta4_5 << 28; // store delta4_1 in the bits 16 to 20
        int delta4_7 = (int) Math.round(((double)(frequencies[6])/(root - count2_1 - count3_3)) * (Math.pow(2, 4)-1)); // compute the first delta on the fourth level
        lowerLevels += delta4_7;
    }

    public int getLowerBound() {
        return lowerBound;
    }

    public int getUpperBound() {
        return upperBound;
    }

    public int getRoot() {
        return root;
    }

    /**
     * Method which approximately computes the frequencies based on the given query-range.
     * The queries originally have to be based on equi-width buckets in order for this approximation to work!
     * @param queryLowerBound   lower bound of the query range inclusive
     * @param queryUpperBound   upper bound of the query range inclusive
     * @return  the approximate frequencies of the range query based on this 4LT Bucket
     */
    public int getFrequency(int queryLowerBound, int queryUpperBound){

        if (queryUpperBound < queryLowerBound){
            throw new IllegalArgumentException("upper Bound cannot be smaller than lower Bound!");
        }
        if (queryLowerBound < lowerBound && queryUpperBound > upperBound){
            return root; // if bounds contain bucket bounds completely simply return the root
        }
        int frequency = 0;
        double distance = (upperBound-lowerBound) / 8d;

        double leftIndex = Math.min((queryLowerBound - lowerBound) * 8 / (upperBound-lowerBound), 0d);
        double rightIndex = Math.max((queryUpperBound - lowerBound) * 8 / (upperBound-lowerBound), 8d); // real valued right index exclusive

        // get the respective deltas
        int delta2_1 = lowerLevels >> 26;
        int count2_1 = (int) Math.round(delta2_1 / Math.pow(2, 6) * root);
        if (leftIndex == 0 && rightIndex >= 4){
            frequency += count2_1;
            frequency += getFrequency((int) Math.ceil(distance*4) + lowerBound, queryUpperBound);
        }else if (leftIndex < 4 && rightIndex == 8){
            frequency += root-count2_1;
            frequency += getFrequency(queryLowerBound,  (int) Math.floor(distance*4) + lowerBound);
        }else {
            int delta3_1 = (lowerLevels >> 21) & 31;
            int delta3_3 = (lowerLevels >> 16) & 31;
            int count3_1 = (int) Math.round(delta3_1 / Math.pow(2, 5) * count2_1);
            int count3_3 = (int) Math.round(delta3_3 / Math.pow(2, 5) * (root - count2_1));
            if (leftIndex == 0 && rightIndex > 2){
                frequency += count3_1;
                frequency += getFrequency((int) Math.ceil(distance*2) + lowerBound, queryUpperBound);
            }else if (leftIndex < 2 && rightIndex >= 4){
                frequency += count2_1 - count3_1;
                frequency += getFrequency((int) Math.ceil(distance*4) + lowerBound, queryUpperBound);
                frequency += getFrequency(queryLowerBound,  (int) Math.floor(distance*2) + lowerBound);
            }else if (leftIndex < 4 && rightIndex >= 6){
                frequency += count3_3;
                frequency += getFrequency((int) Math.ceil(distance*6) + lowerBound, queryUpperBound);
                frequency += getFrequency(queryLowerBound,  (int) Math.floor(distance*4) + lowerBound);
            }else if (leftIndex < 6 && rightIndex == 8){
                frequency += root - count2_1 - count3_3;
                frequency += getFrequency(queryLowerBound,  (int) Math.floor(distance*6) + lowerBound);
            }else {
                int delta4_1 = (lowerLevels >> 12) & 15;
                int delta4_3 = (lowerLevels >> 12) & 15;
                int delta4_5 = (lowerLevels >> 12) & 15;
                int delta4_7 = (lowerLevels >> 12) & 15;
                int count4_1 = (int) Math.round(delta4_1 / Math.pow(2, 4) * count3_1);
                int count4_3 = (int) Math.round(delta4_3 / Math.pow(2, 4) * (count2_1 - count3_1));
                int count4_5 = (int) Math.round(delta4_5 / Math.pow(2, 4) * count3_3);
                int count4_7 = (int) Math.round(delta4_7 / Math.pow(2, 4) * (root - count2_1 - count3_3));

                if (leftIndex == 0 && rightIndex >= 1){
                    frequency += count4_1;
                    frequency += getFrequency((int) Math.ceil(distance) + lowerBound, queryUpperBound);
                }else if (leftIndex < 1 && rightIndex >= 2){
                    frequency += count3_1 - count4_1;
                    frequency += getFrequency((int) Math.ceil(distance * 2) + lowerBound, queryUpperBound);
                    frequency += getFrequency(queryLowerBound,  (int) Math.floor(distance) + lowerBound);
                }else if (leftIndex < 2 && rightIndex >= 3){
                    frequency += count4_3;
                    frequency += getFrequency((int) Math.ceil(distance * 3) + lowerBound, queryUpperBound);
                    frequency += getFrequency(queryLowerBound,  (int) Math.floor(distance*2) + lowerBound);
                }else if (leftIndex < 3 && rightIndex >= 4){
                    frequency += count2_1 - count3_1 - count4_3;
                    frequency += getFrequency((int) Math.ceil(distance * 4) + lowerBound, queryUpperBound);
                    frequency += getFrequency(queryLowerBound,  (int) Math.floor(distance*3) + lowerBound);
                }else if (leftIndex < 4 && rightIndex >= 5){
                    frequency += count4_5;
                    frequency += getFrequency((int) Math.ceil(distance * 5) + lowerBound, queryUpperBound);
                    frequency += getFrequency(queryLowerBound,  (int) Math.floor(distance*4) + lowerBound);
                }else if (leftIndex < 5 && rightIndex >= 6){
                    frequency += count3_3 - count4_5;
                    frequency += getFrequency((int) Math.ceil(distance * 6) + lowerBound, queryUpperBound);
                    frequency += getFrequency(queryLowerBound,  (int) Math.floor(distance*5) + lowerBound);
                }else if (leftIndex < 6 && rightIndex >= 7){
                    frequency += count4_7;
                    frequency += getFrequency((int) Math.ceil(distance * 2) + lowerBound, queryUpperBound);
                    frequency += getFrequency(queryLowerBound,  (int) Math.floor(distance*6) + lowerBound);
                }else if (leftIndex < 7 && rightIndex == 8){
                    frequency += root - count2_1 - count3_3 - count4_7;
                    frequency += getFrequency((int) Math.ceil(distance * 7) + lowerBound, queryUpperBound);
                    frequency += getFrequency(queryLowerBound,  (int) Math.floor(distance*7) + lowerBound);
                }else {
                    // TODO: use count[] in order to exploit the indices!
                }
            }
        }

        return frequency;
    }
}
