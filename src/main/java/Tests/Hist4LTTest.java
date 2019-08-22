package Tests;

import Histograms.RealValuedBucket4LT;

public class Hist4LTTest {

    public static void main(String[] args) throws Exception {
        int[] frequencies = {1,2,3,4,5,4,3,2};
        RealValuedBucket4LT bucket = new RealValuedBucket4LT(10, 18);

        bucket.build(frequencies);

        int range0 = bucket.getFrequency(5, 20);
        int range1 = bucket.getFrequency(10, 20);
        int range2 = bucket.getFrequency(12, 13);
        int range3 = bucket.getFrequency(12, 16);
        int range4 = bucket.getFrequency(5, 15);

        System.out.println("5-20: " + range0 + "\n10-20: " + range1 + "\n" +
                "12-20: " + range2 + "\n 12-15: " + range3 +"\n 5-15: " + range4);
        System.out.println(bucket);
    }
}
