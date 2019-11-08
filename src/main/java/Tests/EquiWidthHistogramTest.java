package Tests;

import Histograms.EquiWidthHistogram;
import Sketches.CountMinSketch;
import org.junit.Test;
import org.junit.Assert;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.Arrays;
import java.io.File;
import java.lang.Double;
import java.io.FileNotFoundException;
/**
 * @author Zahra Salmani
 */
public class EquiWidthHistogramTest {
    @Test(expected = IllegalArgumentException.class)
    public void illegalboundTest(){
        Double lowerBound=12.2;
        Double upperBound=5.0;
        EquiWidthHistogram eqwHistogram = new EquiWidthHistogram(lowerBound,upperBound,10);}
    @Test(expected = IllegalArgumentException.class)
    public void illegalbucketnumTest(){
        Double lowerBound=12.2;
        Double upperBound=20.0;
        Integer bucket= -20;
        EquiWidthHistogram eqwHistogram = new EquiWidthHistogram(lowerBound,upperBound,bucket);}
    @Test
    public void bucketlengthTest(){
        Double lowerBound=12.6;
        Double upperBound=20.0;
        Integer bucket= 4;
        EquiWidthHistogram eqwHistogram = new EquiWidthHistogram(lowerBound,upperBound,bucket);
        //System.out.println(eqwHistogram.getBucketLength());
        Assert.assertTrue(eqwHistogram.getBucketLength() == 1.85);
    }
    @Test
    public void updateTest(){
        String fileName= "data/testdata.csv";
        File file= new File(fileName);
        Double lowerBound=0.0;
        Double upperBound=200.0;
        Integer numBucket= 10;
        int[] knownFrequency = new int[] {6,12,14,11,12,8,10,8,10,9};
        EquiWidthHistogram eqwHistogram = new EquiWidthHistogram(lowerBound,upperBound,numBucket);
        int [] computedFrequency=updateFrequencyFromFile( "data/testdata.csv",eqwHistogram);
        //for (Integer freq:eqwHistogram.getFrequency()){System.out.println(freq);}
        Assert.assertTrue(Arrays.equals(knownFrequency, computedFrequency));}

    @Test(expected = IllegalArgumentException.class)
    public void mergboundryTest(){
        EquiWidthHistogram eqwHistogram = new EquiWidthHistogram(12.0,50.0,5);
        EquiWidthHistogram other = new EquiWidthHistogram(12.5,50.0,5);
        eqwHistogram.merge(other);
    }
    @Test(expected = IllegalArgumentException.class)
    public void mergothertypeTest(){
        EquiWidthHistogram eqwHistogram = new EquiWidthHistogram(12.0,50.0,5);
        CountMinSketch other = new CountMinSketch(10,34,128L);
        eqwHistogram.merge(other);}
    @Test
    public void mergTest(){
        Double lowerBound=0.0;
        Double upperBound=200.0;
        Integer numBucket= 10;
        int[] knownFrequency;
        EquiWidthHistogram eqwHistogram = new EquiWidthHistogram(lowerBound,upperBound,numBucket);
        int[] computedFrequency=updateFrequencyFromFile( "data/testdata.csv",eqwHistogram);
        EquiWidthHistogram other = new EquiWidthHistogram(lowerBound,upperBound,numBucket);
        int[] otherComputedFrequency =updateFrequencyFromFile( "data/testdata2.csv",other);
        knownFrequency = new int[computedFrequency.length];
        Arrays.setAll(knownFrequency, i->computedFrequency[i]+otherComputedFrequency[i] );
        eqwHistogram.merge(other);
        Assert.assertTrue(Arrays.equals(knownFrequency,eqwHistogram.getFrequency()));

    }
    @Test
    public void rangeQueryTest(){
        Double lowerBound=0.0;
        Double upperBound=200.0;
        Integer numBucket= 10;
        //double knownFrequency=;
        EquiWidthHistogram eqwHistogram = new EquiWidthHistogram(lowerBound,upperBound,numBucket);
        int[] computedFrequency=updateFrequencyFromFile( "data/testdata.csv",eqwHistogram);
        Assert.assertTrue(eqwHistogram.rangeQuery(10,20)==3.0);
        Assert.assertTrue(eqwHistogram.rangeQuery(23.5,50)==16.9);
        Assert.assertTrue(eqwHistogram.rangeQuery(24.5,30)==3.3000000000000003);
        Assert.assertTrue(eqwHistogram.rangeQuery(-50,-17)==0.0);
        Assert.assertTrue(eqwHistogram.rangeQuery(202,300)==0.0);

    }

    public int[] updateFrequencyFromFile(String Name,EquiWidthHistogram eqwHistogram){
        String fileName= Name;
        File file= new File(fileName);
        EquiWidthHistogram histogram = eqwHistogram;

        // this gives you a 2-dimensional array of strings
        ArrayList<Double> lines = new ArrayList<>();
        Scanner inputStream;

        try{
            inputStream = new Scanner(file);

            while(inputStream.hasNext()){
                String line= inputStream.next();
                double values = Double.parseDouble(line);//line.split(",")[0]
                // this adds the currently parsed line to the 2-dimensional string array
                lines.add(values);
            }

            inputStream.close();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        for (Double element: lines) {
            histogram.update(element);
        }
        //for (Integer freq:eqwHistogram.getFrequency()){System.out.println(freq);}
        return histogram.getFrequency();
    }

}