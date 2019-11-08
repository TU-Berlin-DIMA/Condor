package Tests;
import Histograms.RealValuedBucket4LT;
import org.junit.Test;
import org.junit.Assert;
import java.util.Arrays;
import java.util.stream.*;
/**
 * @author Zahra Salmani
 */
public class RealValuedBucket4LTTest {

    @Test(expected = IllegalArgumentException.class)
    public void illegalBoundry(){
        RealValuedBucket4LT Histogram4LT = new RealValuedBucket4LT(50.0,35.0);}
    @Test(expected = IllegalArgumentException.class)
    public void frequencylengthTest() throws Exception {
        RealValuedBucket4LT Histogram4LT = new RealValuedBucket4LT(1.0,10.0);
        int [] frequencies = new int [] {12,18,16,6,6,14,13,15,22};
        Histogram4LT.build(frequencies);}
    @Test
    public void buildTest() throws Exception {
        RealValuedBucket4LT Histogram4LT = new RealValuedBucket4LT(0.0,18.0);
        int [] frequencies = new int [] {12,18,16,6,6,14,13,15};
        Histogram4LT.build(frequencies);
        String lowerLevel = Integer.toBinaryString(Histogram4LT.getLowerLevels());
        Assert.assertEquals("10000110010011010110101101010111",lowerLevel);}
    @Test(expected = IllegalArgumentException.class)
    public void illegalQueryboundry(){
        RealValuedBucket4LT Histogram4LT = new RealValuedBucket4LT(0.0,18.0);
        Histogram4LT.getFrequency(15,12);}

    @Test
    public void getFrequencyTest() throws Exception {
        RealValuedBucket4LT Histogram4LT = new RealValuedBucket4LT(0.0,240.0);
        int [] frequencies = new int [] {481,477,520,492,552,489,474,515};//{10,13,16,16,9,12,14,10};
        //int totalfrequency= IntStream.of(frequencies).sum();
        Arrays.sort(frequencies);
        int maxfrquency= frequencies[7];
        double worstCaseError= (30*(Histogram4LT.getUpperBound()-Histogram4LT.getLowerBound()))/(double)32;// 30 is the highest frequency of discrete integers in 0,1,2,...,240(using numpy.unique on dataset)
        Histogram4LT.build(frequencies);
        System.out.println(Histogram4LT.toString());

        Assert.assertTrue(Math.abs(Histogram4LT.getFrequency(-25,100)-1635) <=worstCaseError);
        Assert.assertTrue(Math.abs(Histogram4LT.getFrequency(180,300)-989) <=worstCaseError);
        Assert.assertTrue(Math.abs(Histogram4LT.getFrequency(90,150)-1044) <=worstCaseError);
        Assert.assertTrue(Math.abs(Histogram4LT.getFrequency(185,200)-228) <=worstCaseError);
        Assert.assertTrue(Math.abs(Histogram4LT.getFrequency(75,85)-189)<=worstCaseError);
        Assert.assertTrue(Math.abs(Histogram4LT.getFrequency(-10,25)-403)<=worstCaseError);
        Assert.assertTrue(Math.abs(Histogram4LT.getFrequency(230,250)-170)<=worstCaseError);
        Assert.assertTrue(Math.abs(Histogram4LT.getFrequency(87,185)-1662)<=worstCaseError);
        Assert.assertTrue(Histogram4LT.getFrequency(0,240)==4000);
        Assert.assertTrue(Histogram4LT.getFrequency(-80,0)==0);
        Assert.assertTrue(Histogram4LT.getFrequency(600,1000)==0);
        Assert.assertTrue(Histogram4LT.getFrequency(155,155)==0);


    }
    /*public double errorComputation(int estimated,int actuall){
        return Math.abs(estimated-actuall)/(double)actuall;

    }*/

}
