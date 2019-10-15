package Tests;
import Histograms.RealValuedBucket4LT;
import org.junit.Test;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.After;
import org.decimal4j.util.DoubleRounder;

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
        System.out.println(lowerLevel);
        Assert.assertEquals("10000110010011010110101101010111",lowerLevel);}
    @Test(expected = IllegalArgumentException.class)
    public void illegalQueryboundry(){
        RealValuedBucket4LT Histogram4LT = new RealValuedBucket4LT(0.0,18.0);
        Histogram4LT.getFrequency(15,12);}

    @Test
    public void getFrequencyTest() throws Exception {
        RealValuedBucket4LT Histogram4LT = new RealValuedBucket4LT(0.0,200.0);
        int [] frequencies = new int [] {10,13,16,16,9,12,14,10};
        Histogram4LT.build(frequencies);
        System.out.println(Histogram4LT.toString());
        //System.out.println(Histogram4LT.getFrequency(5,10));
        System.out.println(Histogram4LT.getFrequency(0,75));
       // System.out.println(Histogram4LT.getFrequency(51,75));

    }

}
