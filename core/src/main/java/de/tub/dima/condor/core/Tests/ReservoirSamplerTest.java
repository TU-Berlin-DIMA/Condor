package de.tub.dima.condor.core.Tests;
import de.tub.dima.condor.core.Synopsis.Sampling.ReservoirSampler;
import org.hamcrest.core.IsEqual;
import org.hamcrest.core.IsNot;
import org.junit.Assert;
import org.junit.Test;
import java.util.Scanner;
import java.util.ArrayList;
import java.io.File;
import java.lang.Math;
import java.io.FileNotFoundException;
/**
 * @author Zahra Salmani
 */

public class ReservoirSamplerTest {
    @Test
    public void updateTest() {
        ReservoirSampler reservoirSampler= new ReservoirSampler(10);

        String fileName= "data/testdata.csv";
        File file= new File(fileName);

        ArrayList<String> lines = new ArrayList<>();
        Scanner inputStream;

        try{
            inputStream = new Scanner(file);
            while(inputStream.hasNext()){
                String line= inputStream.next();
                lines.add(line);
            }
            inputStream.close();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        for (int i=0;i<10;i++) {
            reservoirSampler.update(lines.get(i));
        }
        String [] fixedSample = new String [] {"103", "52", "161", "25", "188", "19", "48", "93", "50", "143"};

        Object notfullsample = reservoirSampler.getSample();
        Assert.assertArrayEquals((Object[]) notfullsample,fixedSample);

//        for (int i=10;i<lines.size();i++) {
//            reservoirSampler.update(lines.get(i));
//        }
//        Object fullSample = reservoirSampler.getSample();
//        Assert.assertThat(fullSample, new IsNot(new IsEqual(fixedSample)));//not(equalTo(...))
    }
    @Test(expected = Exception.class)
    public void illegalmergesamplesizeTest() throws Exception {
        ReservoirSampler Reservoir= new ReservoirSampler(12);
        ReservoirSampler other= new ReservoirSampler(10);
        Reservoir.merge(other);
    }
    @Test
    public void mergeTest() throws Exception {
        int samplesize=2000;
        ReservoirSampler reservoirSampler= new ReservoirSampler(samplesize);
        ReservoirSampler other= new ReservoirSampler(samplesize);
        int processedZero=3000;
        int processedOne=12000;
        int numberOne = 0,numberZero =0;
        double fractionOne;
        double fractionZero;
        for (int i=0;i<processedOne;i++){
            reservoirSampler.update(1);
        }
        for (int i=0;i<processedZero;i++) {
            other.update(0);
        }
        reservoirSampler.merge(other);

        for(Object element : reservoirSampler.getSample()){
            if(element.equals(1)){numberOne++;}
            if(element.equals(0)){numberZero++;}
        }

//        fractionOne= (double)samplesize*((double)processedOne/(processedOne+processedZero));
//        double errorRateOne=(Math.abs(fractionOne-numberOne)/fractionOne)*100;
//        fractionZero= samplesize-fractionOne;
//        double errorRateZero=(Math.abs(fractionZero-numberZero)/fractionZero)*100;

        fractionOne= (double)processedOne/(processedOne+processedZero);
        fractionZero= samplesize-fractionOne;

        double mergeOneFraction = (double)numberOne/samplesize;
        double mergeZeroFraction = 1-mergeOneFraction;
        double errorRateZero=Math.abs(fractionZero-mergeZeroFraction)/fractionZero;
        double errorRateOne=Math.abs(fractionOne-mergeOneFraction)/fractionOne;
        Assert.assertTrue(errorRateOne <=5);
        Assert.assertTrue(errorRateZero<=5);

    }
}


