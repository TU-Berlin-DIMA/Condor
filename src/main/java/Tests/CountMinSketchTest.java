package Tests;

import Sketches.BloomFilter;
import org.junit.Assert;
import org.junit.Test;
import Sketches.CountMinSketch;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Arrays;
/**
 * @author Zahra Salmani
 */

public class CountMinSketchTest {
    @Test
    public void updateTest(){
       CountMinSketch countmin = new CountMinSketch(12,10,7136673L);
       int[][] frequencyArr= countmin.getArray();
       int [] rowsum= new int[frequencyArr.length];
       int count=0;
        for(int row=0; row< frequencyArr.length;row++){
            rowsum[row]=0;
            for (int col=0;col< frequencyArr[row].length;col++){
                rowsum[row]+=frequencyArr[row][col];
            }
        }
        //check does sum of rows in frequency matrix equal to zero, initially
        for (int element:rowsum){
            Assert.assertEquals(element,0);
        }
        //read data from file and update the sketch
        String fileName= "data/dataset.csv";
        File file= new File(fileName);
        // this gives you a 2-dimensional array of strings
        Scanner inputStream;
        try{
            inputStream = new Scanner(file);
            while(inputStream.hasNext()){
                String line= inputStream.next();
                countmin.update(line);
                for(int row=0; row< frequencyArr.length;row++){
                    int sum=0;
                    for (int col=0;col< frequencyArr[row].length;col++){
                        sum+=frequencyArr[row][col];
                    }
                    Assert.assertEquals(1, sum-rowsum[row]);
                    rowsum[row]=sum;
                }

                int processedelements= countmin.getElementsProcessed();
                for (int element:rowsum){
                    Assert.assertEquals(element,processedelements);
                }
            }
            inputStream.close();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        //end of reading from file
    }

    @Test
    public void updateWithSameTest() {
        CountMinSketch countmin = new CountMinSketch(12, 10, 7136673L);
        countmin.update(200);
        int[][] oldFrequencyArr = Arrays.stream(countmin.getArray()).map(int[]::clone).toArray(int[][]::new);

        int[][] newFrequencyArr = countmin.getArray();
        for (int i = 0; i < 1999; i++) {
            countmin.update(200);
            for (int row = 0; row < newFrequencyArr.length; row++) {

                for (int col = 0; col < newFrequencyArr[row].length; col++) {
                    if(newFrequencyArr[row][col]==0 || oldFrequencyArr[row][col]==0){
                        Assert.assertEquals(newFrequencyArr[row][col],oldFrequencyArr[row][col]);
                    }

                }
            }
        }
        int[] rowsum=new int [oldFrequencyArr.length];

        for (int row = 0; row < newFrequencyArr.length; row++) {
            int sum=0;
            for (int col = 0; col < newFrequencyArr[row].length; col++) {
                sum+=newFrequencyArr[row][col];
            }
            rowsum[row]=sum;
        }
        for(int element:rowsum){
            Assert.assertEquals(element,2000);
        }
        Assert.assertEquals(countmin.getElementsProcessed(),2000);
    }
    @Test
    public void queryTest(){
        CountMinSketch countmin = new CountMinSketch(150,8,7136673L);

        String fileName= "data/dataset.csv";
        File file= new File(fileName);
        // this gives you a 2-dimensional array of strings
        ArrayList<String> lines = new ArrayList<>();
        Scanner inputStream;
        try{
            inputStream = new Scanner(file);
            while(inputStream.hasNext()){
                String line= inputStream.next();
                countmin.update(line);
            }
            inputStream.close();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        double errorbound = 3173/150;
        int i = countmin.query(239);
        Assert.assertTrue(Math.abs(countmin.query(239)-17)<= errorbound);
        Assert.assertTrue(Math.abs(countmin.query(15)-13)<= errorbound);
        Assert.assertTrue(Math.abs(countmin.query(100)-16)<= errorbound);
        Assert.assertTrue(Math.abs(countmin.query(57)-10)<= errorbound);
    }

    @Test(expected = Exception.class)
    public void illegalMergesizeTest() throws Exception {
        CountMinSketch countmin = new CountMinSketch(12, 10, 7136673L);
        CountMinSketch other = new CountMinSketch(11, 10, 7136673L);
        countmin.merge(other);
    }
    @Test(expected = Exception.class)
    public void illegalMergeSamplesTest() throws Exception {
        CountMinSketch countmin = new CountMinSketch(12, 10, 7136673L);
        BloomFilter other= new BloomFilter( 7, 20,200);
        countmin.merge(other);
    }
    @Test
    public void mergeTest() throws Exception {
        CountMinSketch countmin = new CountMinSketch(12,8,7136673L);
        CountMinSketch other = new CountMinSketch(12,8,7136673L);
        for(int i=0;i<2500;i++){
            countmin.update(i);
            other.update(i);
        }
        int oldElementProcessed=countmin.getElementsProcessed();
        int [][] oldFrequencyArr= Arrays.stream(countmin.getArray()).map(int[]::clone).toArray(int[][]::new);

        countmin.merge(other);
        int [][] mergefrequency= countmin.getArray();

        Assert.assertEquals(countmin.getElementsProcessed(),5000);
        for(int row=0; row< oldFrequencyArr.length;row++){
            for (int col=0;col< oldFrequencyArr[row].length;col++){
                Assert.assertTrue(mergefrequency[row][col] == 2 * oldFrequencyArr[row][col]);
            }
        }
    }

}


