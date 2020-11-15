package de.tub.dima.condor.core.Tests;

import de.tub.dima.condor.core.Synopsis.Sketches.HashFunctions.EfficientH3Functions;
import org.junit.Assert;
import org.junit.Test;
import de.tub.dima.condor.core.Synopsis.Sketches.BloomFilter;
import de.tub.dima.condor.core.Synopsis.Sketches.CountMinSketch;
import java.lang.Math;

import java.util.ArrayList;
import java.util.BitSet;
/**
 * @author Zahra Salmani
 */
public class BloomFilterTest {

    @Test
    public void updateTest(){
        BloomFilter bloomFilter= new BloomFilter( 800, 5000,  200L);
        Assert.assertEquals(bloomFilter.getnHashFunctions(),4);
        BitSet hashmap = bloomFilter.getHashmap();
        Assert.assertTrue(hashmap.equals(new BitSet(5000)));

        bloomFilter.update(12);
        BitSet testbitset = bloomFilter.getHashmap();
        Assert.assertEquals(testbitset.cardinality(),bloomFilter.getnHashFunctions());
        for (int i = 0; i<800; i++) {
            bloomFilter.update(12);
            Assert.assertTrue(hashmap.equals(testbitset));
        }

        Assert.assertTrue(bloomFilter.getHashmap().equals(testbitset));
    }
    @Test
    public void queryTest(){
        int positivecount=0;
        //int trueNegativecount=0;
        BloomFilter bloomFilter= new BloomFilter( 1500, 3000,200L);
        for (int i = 600; i< 2100; i++){
            bloomFilter.update(i);
        }

        for (int j=0;j<2100; j++){
           if(bloomFilter.query(j)) {
               positivecount++;
           }
        }
        /*for (int j=0;j<600; j++){
            if(!bloomFilter.query(j)) {
                trueNegativecount++;
            }
        }*/
        double ratioArgument= Math.exp(-(bloomFilter.getnHashFunctions()*1500)/3000.0);
        double fprateEstimate= Math.pow((1-ratioArgument),bloomFilter.getnHashFunctions());
        double falsePositivecount= positivecount-bloomFilter.getElementsProcessed();
        double falsePositiverate= falsePositivecount/(2100);//1500+trueNegativecount);
        Assert.assertTrue(falsePositiverate<=fprateEstimate);
    }
    @Test(expected = IllegalArgumentException.class)
    public void illegalMergesizeTest() throws Exception {
        BloomFilter bloomFilter= new BloomFilter( 5, 20,200L);
        BloomFilter other= new BloomFilter( 7, 18,200L);
        bloomFilter.merge(other);
    }
    @Test(expected = IllegalArgumentException.class)
    public void illegalMergeSamplesTest() throws Exception {
        BloomFilter bloomFilter= new BloomFilter( 7, 20,200L);
        CountMinSketch other= new CountMinSketch(10,20,(long)200);
        bloomFilter.merge(other);
    }
    @Test
    public void mergeTest() throws Exception{
        BloomFilter bloomFilter= new BloomFilter( 1000, 2000,200L);
        for(int i=13000;i<13870;i++){
            bloomFilter.update(i);
        }


        BloomFilter other= new BloomFilter( 950, 2000,200L);
        for(int i=1200;i<2000;i++){
            other.update(i);
        }
        int[] intersecresult= new int[]{1024, 514, 1539, 4, 517, 6, 1030, 1031, 521, 1035, 1039, 1551, 1553, 18, 532, 534, 535, 24, 1559, 538, 30, 34, 1571, 1572, 553, 554, 44, 1072, 49, 562, 1584, 1593, 58, 1595, 1084, 61, 1598, 65, 1090, 580, 69, 1094, 1607, 76, 1100, 1104, 1110, 1111, 1624, 89, 90, 1118, 1630, 1122, 100, 101, 1125, 1642, 623, 628, 117, 1140, 119, 1655, 635, 641, 1153, 1155, 1665, 133, 1666, 136, 649, 139, 1164, 1680, 657, 659, 1683, 1173, 1175, 666, 155, 1692, 157, 1695, 1699, 167, 1191, 1704, 1194, 692, 184, 1211, 700, 1213, 1215, 704, 203, 204, 1740, 1231, 1234, 1235, 212, 214, 216, 735, 225, 1250, 740, 741, 1252, 744, 1260, 237, 752, 1778, 244, 758, 1783, 1276, 1788, 766, 255, 259, 1284, 1797, 1287, 265, 777, 1290, 274, 1302, 281, 793, 1310, 288, 1830, 1833, 810, 300, 1839, 1840, 821, 1334, 311, 1848, 1343, 834, 835, 330, 1867, 847, 1878, 858, 347, 859, 1371, 1882, 863, 352, 1887, 358, 361, 1387, 877, 370, 882, 1910, 1911, 888, 1912, 380, 1406, 1407, 1918, 897, 1927, 1416, 905, 1417, 1929, 1931, 910, 913, 1940, 1431, 1435, 1948, 415, 416, 929, 931, 1957, 422, 1964, 429, 431, 1969, 434, 437, 950, 1974, 1466, 955, 1980, 1984, 450, 1476, 456, 968, 1992, 972, 973, 1486, 1494, 1504, 481, 482, 1505, 1507, 485, 1009, 1015, 503, 1535};
        BitSet intersectBitset= new BitSet(2000);
        for(int element:intersecresult){
            intersectBitset.set(element);
        }
        BitSet mergeresult= bloomFilter.merge(other).getHashmap();
        Assert.assertTrue(intersectBitset.equals(mergeresult));
        Assert.assertTrue(bloomFilter.getElementsProcessed()==1670);
    }
}
