package Tests;
import Histograms.EquiDepthHistogram;
import org.junit.Test;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.After;
import org.decimal4j.util.DoubleRounder;
/**
 * @author Zahra Salmani
 */
public class EquiDepthHistogramTest {

   //@Before
   //public void setup(){
        double [] leftBoundaries= new double[] {0, 46, 75.48, 115, 156.56};//{0.  ,  51.45,  89.  , 147.1 };
        EquiDepthHistogram equiDepthHistogram = new EquiDepthHistogram(leftBoundaries, 200, 100);
   // }
    @Test(expected = IllegalArgumentException.class)
    public void illegalqueryboundorderTest(){
        equiDepthHistogram.rangeQuery(50,24);
    }
    @Test(expected = IllegalArgumentException.class)
    public void queryboundoutofrangeTest(){
        equiDepthHistogram.rangeQuery(-10,-2);
    }
    @Test
    public void querybucketTest(){
        Assert.assertTrue(0.0==equiDepthHistogram.rangeQuery(-247,0));
        Assert.assertTrue(100==equiDepthHistogram.rangeQuery(0,200));
        Assert.assertTrue(100==equiDepthHistogram.rangeQuery(-10,270));
        Assert.assertTrue(60==equiDepthHistogram.rangeQuery(-10,115));
        Assert.assertTrue(86.19==DoubleRounder.round(equiDepthHistogram.rangeQuery(-10,170),2));
        Assert.assertTrue(10.87==DoubleRounder.round(equiDepthHistogram.rangeQuery(-10,25),2));
        Assert.assertTrue(20==equiDepthHistogram.rangeQuery(46,75.48));
        Assert.assertTrue(40==equiDepthHistogram.rangeQuery(75.48,156.56));
        Assert.assertTrue(19.95==DoubleRounder.round(equiDepthHistogram.rangeQuery(147.1,190),2));
        Assert.assertTrue(5.27==DoubleRounder.round(equiDepthHistogram.rangeQuery(156.56,168),2));
        Assert.assertTrue(39.69==DoubleRounder.round(equiDepthHistogram.rangeQuery(50,120),2));
        Assert.assertTrue(2.61==DoubleRounder.round(equiDepthHistogram.rangeQuery(20,26),2));
        Assert.assertTrue(8.75==DoubleRounder.round(equiDepthHistogram.rangeQuery(170,189),2));

    }



}