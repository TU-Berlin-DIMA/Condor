package de.tub.dima.condor.core.tests;

import de.tub.dima.condor.core.synopsis.Sketches.HyperLogLogSketch;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Scanner;

/**
 * @author Zahra Salmani
 */
public class HyperLogLogSketchTest {

    @Test
    public void constructTest() {

        Assertions.assertThrows(IllegalArgumentException.class,()-> new HyperLogLogSketch(17, 7545465L));
        Assertions.assertThrows(IllegalArgumentException.class,()-> new HyperLogLogSketch(3, 7545465L));
    }

    @Test
    public void updateTest(){

        HyperLogLogSketch HPLLSketch= new HyperLogLogSketch(10, 7545465L);
        byte[] beforeUpdateReg= HPLLSketch.getRegisters().clone();
        //check all registers are initialized with 0
        for(byte b : beforeUpdateReg){
            Assertions.assertEquals(b,0);
        }

        //update sketch with data set
        File file= new File("data/dataset.csv");
        Scanner inputStream;
        try{
            inputStream = new Scanner(file);
            while(inputStream.hasNext()){
                String line= inputStream.next();
                HPLLSketch.update(Integer.parseInt(line));
            }
            inputStream.close();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        //check register's value has changed,after update
        byte[] afterUpdateReg= HPLLSketch.getRegisters().clone();
        Assertions.assertFalse(Arrays.equals(beforeUpdateReg,afterUpdateReg));

        //update a HyperLogLogSketch with same element multiple times
        HyperLogLogSketch HPLLSketchSame= new HyperLogLogSketch(10, 7545465L);
        //update sketch with one same element
        for(int i=0;i<1000;i++){
                HPLLSketchSame.update("90.5");
            }

        //check only one of the registers has changed
        byte[] afterUpdateRegSame= HPLLSketchSame.getRegisters().clone();
        int modifiedRegNum=0;
        for(byte b : afterUpdateRegSame){
                if(b!=0){ modifiedRegNum++;}
        }
        Assertions.assertEquals(modifiedRegNum,1);

    }

    @Test
    public void mergeTest()
    {
        HyperLogLogSketch HPLLSketch= new HyperLogLogSketch(10, 7545465L);

        //merge with illegal sketches should throws exception
        HyperLogLogSketch otherReg= new HyperLogLogSketch(15, 7545465L);
        HyperLogLogSketch otherSeed= new HyperLogLogSketch(10, 7545L);
        Assertions.assertThrows(IllegalArgumentException.class,()->HPLLSketch.merge(otherReg));
        Assertions.assertThrows(IllegalArgumentException.class,()->HPLLSketch.merge(otherSeed));

        HyperLogLogSketch otherHPLL = new HyperLogLogSketch(10, 7545465L);

        //update both sketches with same data set, so merge should not change the cardinality
        File file= new File("data/dataset.csv");
        Scanner inputStream;
        try{
            inputStream = new Scanner(file);
            while(inputStream.hasNext()){
                String line= inputStream.next();
                HPLLSketch.update(Integer.parseInt(line));
                otherHPLL.update(Integer.parseInt(line));
            }
            inputStream.close();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        long beforeMergeDistinct= HPLLSketch.distinctItemsEstimator();
        byte[] beforeMergeReg= HPLLSketch.getRegisters().clone();

        HyperLogLogSketch mergedHPLL=HPLLSketch.merge(otherHPLL);
        long afterMergeDistinct= mergedHPLL.getDistinctItemCount();
        byte[] afterMergeReg= mergedHPLL.getRegisters().clone();
        // check whether the cardinality has changed or not
        Assertions.assertTrue(Arrays.equals(beforeMergeReg,afterMergeReg));
        Assertions.assertEquals(beforeMergeDistinct,afterMergeDistinct);

        //merge with other sketch with non-overlapping domain. this leads to increment in cardinality
        HyperLogLogSketch nonOverlapHPLL = new HyperLogLogSketch(10, 7545465L);
        File file1= new File("data/nonoverlap.csv");
        Scanner inputStream1;
        try{
            inputStream1 = new Scanner(file1);
            while(inputStream1.hasNext()){
                String line1= inputStream1.next();

                nonOverlapHPLL.update(Integer.parseInt(line1));
            }
            inputStream1.close();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        HyperLogLogSketch mergedWithNonOverlap=HPLLSketch.merge(nonOverlapHPLL);
        long withNonOverlapDistinct= mergedWithNonOverlap.getDistinctItemCount();
        Assertions.assertTrue(withNonOverlapDistinct> beforeMergeDistinct);
    }
    @Test
    public void distinctItemEstimatorTest(){
        HyperLogLogSketch HPLLSketch= new HyperLogLogSketch(16, 126765676L);
        File file= new File("data/largdataset.csv");
        Scanner inputStream;
        try{
            inputStream = new Scanner(file);
            while(inputStream.hasNext()){
                String line= inputStream.next();
                HPLLSketch.update(Integer.parseInt(line));
            }
            inputStream.close();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }
//        File file1= new File("data/dataset.csv");
//        Scanner inputStream1;
//        try{
//            inputStream1 = new Scanner(file1);
//            while(inputStream1.hasNext()){
//                String line1= inputStream1.next();
//                HPLLSketch.update(Integer.parseInt(line1));
//            }
//            inputStream1.close();
//        }catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
        int regNum=HPLLSketch.getRegNum();
        double relativeAccuracy= 1.04/Math.sqrt(regNum);
        HPLLSketch.distinctItemsEstimator();
        long distinctItem=HPLLSketch.getDistinctItemCount();
        System.out.println(distinctItem);
        System.out.println(relativeAccuracy);
        System.out.println(regNum);

    }



}
