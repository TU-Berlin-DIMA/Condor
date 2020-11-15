package de.tub.dima.condor.core.Tests;
import  de.tub.dima.condor.core.Synopsis.Sampling.BiasedReservoirSampler;
import de.tub.dima.condor.core.Synopsis.Sampling.TimestampedElement;
import org.junit.jupiter.api.Assertions;
import org.hamcrest.core.IsEqual;
import org.hamcrest.core.IsNot;
import org.junit.Assert;
import org.junit.Test;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.Arrays;
import java.io.File;
import java.io.FileNotFoundException;
/**
 * @author Zahra Salmani
 */

public class BiasedReservoirSamplerTest {
    @Test
    public void updateTest() {
        BiasedReservoirSampler bReservoirSampler= new BiasedReservoirSampler(10);

        //read data from file
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

        //update sampler from file and stop before it cause replacement( less than sample size)
        for (int i=0;i<10;i++) {
            bReservoirSampler.update(new TimestampedElement(Integer.parseInt(lines.get(i)),i+3));
        }

        // create the sample that we expect after update
        ArrayList<TimestampedElement> fixedSample =  new ArrayList(Arrays.asList( new TimestampedElement(103, 3),
                new TimestampedElement(52, 4), new TimestampedElement(161, 5),
                new TimestampedElement(25, 6), new TimestampedElement(188, 7),
                new TimestampedElement(19, 8), new TimestampedElement(48, 9),
                new TimestampedElement(93, 10), new TimestampedElement(50, 11),
                new TimestampedElement(143, 12)));
        //check expected and actual samples
        ArrayList<TimestampedElement> notFullSample=  new ArrayList(Arrays.asList(bReservoirSampler.getSample()));
        Assert.assertTrue(notFullSample.equals(fixedSample));

        //add an element with timestamp earlier than last element in sampler

        bReservoirSampler.update( new TimestampedElement(12, 2));
        ArrayList<TimestampedElement> addEarlierElSample=  new ArrayList(Arrays.asList(bReservoirSampler.getSample()));
        Assert.assertTrue(addEarlierElSample.equals(fixedSample));

        //update sampler from file and continue after exceeding sample size
        for (int i=10;i<lines.size();i++) {
            bReservoirSampler.update(new TimestampedElement(Integer.parseInt(lines.get(i)),i));
        }
        ArrayList<TimestampedElement> fullSample = new ArrayList(Arrays.asList(bReservoirSampler.getSample()));
        //check that the sample has changed, because should give priority to new arrivals
        Assert.assertThat(fullSample, new IsNot(new IsEqual(fixedSample)));
    }

    @Test
    public void mergeTest() throws Exception {

        BiasedReservoirSampler reservoirMergeWithEmpty= new BiasedReservoirSampler(15);
        BiasedReservoirSampler otherWithDifferentSize= new BiasedReservoirSampler(12);
        Assertions.assertThrows(IllegalArgumentException.class,()->reservoirMergeWithEmpty.merge(otherWithDifferentSize)); //merge 2 sampler with different size results in exceptions

        ArrayList<TimestampedElement> sample1 =  new ArrayList(Arrays.asList( new TimestampedElement(103, 10),
                new TimestampedElement(52, 13), new TimestampedElement(188, 17),
                new TimestampedElement(19, 25), new TimestampedElement(48, 29),
                new TimestampedElement(161, 35),new TimestampedElement(25, 65)));
        //update reservoirMergeWithEmpty with samples
        for (TimestampedElement el : sample1){
            reservoirMergeWithEmpty.update(el);
        }
        ArrayList<TimestampedElement> notMergedSample= new ArrayList(Arrays.asList(reservoirMergeWithEmpty.getSample().clone()));
        //construct other sampler
        BiasedReservoirSampler other= new BiasedReservoirSampler(15);
        //merge reservoir with other that is empty
        BiasedReservoirSampler mergeEmpty=reservoirMergeWithEmpty.merge(other);
        ArrayList<TimestampedElement> mergeEmptySample=  new ArrayList(Arrays.asList(mergeEmpty.getSample()));
        //check merge with empty did not effect sample
        Assert.assertTrue(notMergedSample.equals(mergeEmptySample));
        Assertions.assertEquals(mergeEmpty.getMerged(),2);

        //add element to other but not as many as to full merge sample
        BiasedReservoirSampler otherNotFull= new BiasedReservoirSampler(10);
        BiasedReservoirSampler reservoirMergeNotFull= new BiasedReservoirSampler(10);
        for (TimestampedElement el : sample1){
            reservoirMergeNotFull.update(el);
        }
        ArrayList<TimestampedElement> sampleOtherFirst =  new ArrayList(Arrays.asList( new TimestampedElement(45, 7),
                new TimestampedElement(34, 9),new TimestampedElement(45, 47)));
        for (TimestampedElement el : sampleOtherFirst){
            otherNotFull.update(el);
        }
        BiasedReservoirSampler mergeNotFull=reservoirMergeNotFull.merge(otherNotFull);
        ArrayList<TimestampedElement> mergeNotFullSample=  new ArrayList(Arrays.asList(mergeNotFull.getSample()));
        ArrayList<TimestampedElement> notFullSample =  new ArrayList(Arrays.asList( new TimestampedElement(45, 7),
                new TimestampedElement(34, 9),new TimestampedElement(103, 10),
                new TimestampedElement(52, 13), new TimestampedElement(188, 17),
                new TimestampedElement(19, 25), new TimestampedElement(48, 29),
                new TimestampedElement(161, 35),new TimestampedElement(45, 47)
                ,new TimestampedElement(25, 65)));

        //check merge with empty did not effect sample
        Assert.assertTrue(notFullSample.equals(mergeNotFullSample));
        Assertions.assertEquals(mergeNotFull.getMerged(),2);

        //update a new reservoir
        BiasedReservoirSampler reservoir= new BiasedReservoirSampler(15);
        for (TimestampedElement el : sample1){
            reservoir.update(el);
        }

        ArrayList<TimestampedElement> sampleOtherSecond =  new ArrayList(Arrays.asList(  new TimestampedElement(45, 7),
                new TimestampedElement(34, 9),new TimestampedElement(12, 37),
                new TimestampedElement(25, 45), new TimestampedElement(18, 67),
                new TimestampedElement(189, 70), new TimestampedElement(40, 88),
                new TimestampedElement(293, 105), new TimestampedElement(54, 115),
                new TimestampedElement(143, 125)));
        //update other with some elements
        for (TimestampedElement el : sampleOtherSecond){
            other.update(el);
        }

       BiasedReservoirSampler mergeResult=reservoir.merge(other);//merge reservoir and non-empty other
       ArrayList<TimestampedElement> mergeResultSample=  new ArrayList(Arrays.asList(mergeResult.getSample()));

       //compare expected result and actual one from merge result
        Assertions.assertTrue(mergeResultSample.contains(new TimestampedElement(143, 125)));
        Assertions.assertEquals(mergeResult.getMerged(),2);
    }
}


