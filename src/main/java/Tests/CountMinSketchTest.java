package Tests;

import Sketches.HashFunctions.PairwiseIndependentHashFunctions;
import org.junit.Assert;
import org.junit.Test;
import Sketches.CountMinSketch;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.Arrays;
import java.util.stream.IntStream;

public class CountMinSketchTest {
    @Test
    public void updateTest(){
       CountMinSketch countmin = new CountMinSketch(12,10,7136673L);
       int[][] frequencyarr= countmin.getArray();
       int [] rowsum= new int[frequencyarr.length];
       int count=0;
        for(int row=0; row< frequencyarr.length;row++){
            rowsum[row]=0;
            for (int col=0;col< frequencyarr[row].length;col++){
                rowsum[row]+=frequencyarr[row][col];
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
        ArrayList<String> lines = new ArrayList<>();
        Scanner inputStream;
        try{
            inputStream = new Scanner(file);
            while(inputStream.hasNext()){
                String line= inputStream.next();
                countmin.update(line);
                int[][] newfrequencyarr= countmin.getArray();

                for(int row=0; row< newfrequencyarr.length;row++){
                    int sum=0;
                    for (int col=0;col< newfrequencyarr[row].length;col++){
                        sum+=newfrequencyarr[row][col];
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
    public void updateWithSameTest(){
        CountMinSketch countmin = new CountMinSketch(12,10,7136673L);


        countmin.update(200);
        int[][] frequencyarr=countmin.getArray();
        List<Integer> list= new ArrayList<Integer>();
        /*IntStream.range(0,list.size()).filter(i -> list.get(i).equals(0)).forEach(System.out::println);*/
        List<Integer>nonzero= new ArrayList();
        for(int row=0; row< frequencyarr.length;row++){

            for (int col=0;col< frequencyarr[row].length;col++){
               list.add(frequencyarr[row][col]);
            }


        }
        for (int i=0;i<list.size();i++){

            if(list.get(i)!=0){
                nonzero.add(i);

            }
        }

        for(int i=0;i<1999;i++){
            countmin.update(200);
        }




        int[][] newfrequencyarr=countmin.getArray();
        List<Integer> newlist= new ArrayList<Integer>();
        /*IntStream.range(0,list.size()).filter(i -> list.get(i).equals(0)).forEach(System.out::println);*/
        List<Integer>newnonzero= new ArrayList();
        for(int row=0; row< newfrequencyarr.length;row++){

            for (int col=0;col< newfrequencyarr[row].length;col++){
                newlist.add(newfrequencyarr[row][col]);
            }


        }
        for (int i=0;i<newlist.size();i++){

            if(newlist.get(i)!=0){
                newnonzero.add(i);

            }
        }
     Assert.assertEquals(nonzero,newnonzero);
    }

    @Test
    public void queryTest(){
        CountMinSketch countmin = new CountMinSketch(12,8,7136673L);

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
        double errorbound = 3695.0/12;
        Assert.assertTrue(Math.abs(countmin.query(239)-17)<= errorbound);
        Assert.assertTrue(Math.abs(countmin.query(15)-13)<= errorbound);
        Assert.assertTrue(Math.abs(countmin.query(100)-16)<= errorbound);
        Assert.assertTrue(Math.abs(countmin.query(57)-10)<= errorbound);
    }
    @Test
    public void mergeTest() throws Exception {
        CountMinSketch countmin = new CountMinSketch(12,8,7136673L);
        CountMinSketch other = new CountMinSketch(12,8,7136673L);
        for(int i=0;i<2500;i++){
            countmin.update(i);
            other.update(i);
        }
        int [][] frequencyarr= countmin.getArray();
        int [][] l=frequencyarr;
        countmin.merge(other);
        int [][] mergefrequency= countmin.getArray();


        //System.out.println(countmin.getElementsProcessed());
        for(int row=0; row< frequencyarr.length;row++){
            for (int col=0;col< frequencyarr[row].length;col++){
                //Assert.assertTrue(mergefrequency[row][col] == 2 * frequencyarr[row][col]);
                System.out.println(mergefrequency[row][col]);
                System.out.println(l[row][col]);
                System.out.println("----------------");
            }
        }
    }


}


