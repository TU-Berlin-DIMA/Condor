package Tests;

import Sketches.HashFunctions.PairwiseIndependentHashFunctions;
import org.junit.Assert;
import org.junit.Test;
import Sketches.BloomFilter;
import Sketches.CountMinSketch;
import java.lang.Math;

import java.util.BitSet;

public class BloomFilterTest {

    @Test
    public void updateTest(){
        BloomFilter bloomFilter= new BloomFilter( 500, 2000,200);
        //check number of hash functions
        Assert.assertEquals(bloomFilter.getnHashFunctions(),3);
        //produce hash functions and their result
        PairwiseIndependentHashFunctions hashFunctions= new PairwiseIndependentHashFunctions(3, 200);
        int[] indices = hashFunctions.hash(12);
        BitSet hashmap = bloomFilter.getHashmap();
        for (int element:indices){
            Assert.assertFalse(hashmap.get(element%bloomFilter.getNumberBits()));

        }
        /*System.out.println(hashmap.get(0));
        System.out.println(hashmap.get(87));
        System.out.println(hashmap.get(1046));
        System.out.println(hashmap.get(509));*/

        bloomFilter.update(12);


        for (int element:indices){
            Assert.assertTrue(hashmap.get(element%bloomFilter.getNumberBits()));
            }
        /*System.out.println(hashmap.get(0));
        System.out.println(hashmap.get(87));
        System.out.println(hashmap.get(1046));
        System.out.println(hashmap.get(509));*/


    }
    @Test
    public void queryTest(){
        int positivecount=0;
        int trueNegativecount=0;
        BloomFilter bloomFilter= new BloomFilter( 1500, 5000,200);//what if it is 3000
        for (int i = 600; i< 3000; i++){
            bloomFilter.update(i);
        }

        for (int j=0;j<3000; j++){
           if(bloomFilter.query(j)) {
               positivecount++;
           }
        }
        for (int j=0;j<600; j++){
            if(!bloomFilter.query(j)) {
                trueNegativecount++;
            }
        }

        double ratioArgument= Math.exp(-(bloomFilter.getnHashFunctions()*bloomFilter.getElementsProcessed())/5000.0);
        double fprateEstimate= Math.pow((1-ratioArgument),bloomFilter.getnHashFunctions());
        double falsePositivecount= positivecount-bloomFilter.getElementsProcessed();
        double falsePositiverate= falsePositivecount/(falsePositivecount+trueNegativecount);
        System.out.println(positivecount);
        System.out.println(fprateEstimate);
        System.out.println(falsePositiverate);
        Assert.assertTrue(falsePositiverate<=fprateEstimate);
    }
    @Test(expected = Exception.class)
    public void illegalMergesizeTest() throws Exception {
        BloomFilter bloomFilter= new BloomFilter( 5, 20,200);
        BloomFilter other= new BloomFilter( 7, 18,200);
        bloomFilter.merge(other);
    }
    @Test(expected = Exception.class)
    public void illegalMergeSamplesTest() throws Exception {
        BloomFilter bloomFilter= new BloomFilter( 7, 20,200);
        CountMinSketch other= new CountMinSketch(10,20,(long)200);
        bloomFilter.merge(other);
    }
    @Test
    public void mergeTest() throws Exception{

    }
}
