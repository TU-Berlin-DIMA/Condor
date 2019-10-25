package Tests;

import Sketches.HashFunctions.PairwiseIndependentHashFunctions;
import org.junit.Assert;
import org.junit.Test;
import Sketches.CountMinSketch;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

public class CountMinSketchTest {
    @Test
    public void updateTest(){
       CountMinSketch countmin = new CountMinSketch(12,10,7136673L);
       int[][] frequencyarr= countmin.getArray();
       int [] rowsum= new int[frequencyarr.length];
        for(int row=0; row< frequencyarr.length;row++){
            rowsum[row]=0;
            for (int col=0;col< frequencyarr[row].length;col++){
                rowsum[row]+=frequencyarr[row][col];
            }
        }

        //read data from file and update the sketch
        String fileName= "data/testdata.csv";
        File file= new File(fileName);
        // this gives you a 2-dimensional array of strings
        ArrayList<String> lines = new ArrayList<>();
        Scanner inputStream;
        try{
            inputStream = new Scanner(file);
            while(inputStream.hasNext()){
                String line= inputStream.next();
                // this adds the currently parsed line to the 2-dimensional string array
                countmin.update(line);
                for(int row=0; row< frequencyarr.length;row++){
                    int sum=0;
                    for (int col=0;col< frequencyarr[row].length;col++){
                        sum+=frequencyarr[row][col];
                    }
                    Assert.assertEquals(1, sum-rowsum[row]);
                    rowsum[row]=sum;
                }
            }
            inputStream.close();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        //end of reading from file


    }

}
