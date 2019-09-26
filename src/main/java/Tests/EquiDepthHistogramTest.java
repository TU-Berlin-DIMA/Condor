package Tests;
import Histograms.EquiDepthHistogram;
import Sketches.CountMinSketch;
import org.junit.Test;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.After;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.Arrays;
import java.io.File;
import java.lang.Double;
import java.io.FileNotFoundException;

public class EquiDepthHistogramTest {

    //@Before
   // public void setup(){
        double [] leftBoundaries= new double[] {0, 46, 75.48, 115, 156.56};
        EquiDepthHistogram equiDepthHistogram = new EquiDepthHistogram(leftBoundaries, 200, 100);
   // }
    @Test(expected = IllegalArgumentException.class)
    public void illegalqueryboundorderTest(){
        equiDepthHistogram.rangeQuery(50,24);
        equiDepthHistogram.rangeQuery(-10,-2);
    }
    @Test(expected = IllegalArgumentException.class)
    public void queryboundoutofrangeTest(){
        equiDepthHistogram.rangeQuery(-10,-2);
    }
    @Test()
    public void querylastbucketTest(){
        System.out.println(equiDepthHistogram.rangeQuery(168,190));
        Assert.assertEquals(10.128913443830573,equiDepthHistogram.rangeQuery(168,190));
    }



}