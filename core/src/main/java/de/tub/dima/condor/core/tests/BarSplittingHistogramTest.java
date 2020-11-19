package de.tub.dima.condor.core.tests;

import de.tub.dima.condor.core.synopsis.Histograms.EquiDepthHistogram;
import de.tub.dima.condor.core.synopsis.Histograms.EquiWidthHistogram;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import de.tub.dima.condor.core.synopsis.Histograms.BarSplittingHistogram;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

/**
 * @author Zahra Salmani
 */
public class BarSplittingHistogramTest {

    @Test
    public void updateTest()
    {
        BarSplittingHistogram BASHistogram= new BarSplittingHistogram(9,10);
        //read from file and update with read elements
        File file= new File("data/dataset.csv");
        Scanner inputStream;
        try{
            inputStream = new Scanner(file);
            while(inputStream.hasNext()){
                String line= inputStream.next();
                BASHistogram.update(Integer.parseInt(line));
            }
            inputStream.close();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        //check the frequency of each bar,
        TreeMap<Integer, Float> bars=BASHistogram.getBars();
        Assertions.assertTrue(bars.size()<=90);
        for(Map.Entry<Integer,Float> element:bars.entrySet()){

                Assertions.assertTrue(element.getValue()<= 75.56);

        }

        EquiDepthHistogram equiDepthHistogram= BASHistogram.buildEquiDepthHistogram();

        double errorBound= (4.0/BASHistogram.getP())*(BASHistogram.getTotalFrequencies()/BASHistogram.getNumBuckets());
        // double [] boundries = equiDepthHistogram.getLeftBoundaries();
//        Assertions.assertTrue(Math.abs(481-equiDepthHistogram.rangeQuery(0.0, 29.06908892805006)) <=errorBound);
//        Assertions.assertTrue(Math.abs(400-equiDepthHistogram.rangeQuery(29.06908892805006, 54.43088446195934) )<=errorBound);
//        Assertions.assertTrue(Math.abs(425-equiDepthHistogram.rangeQuery(54.43088446195934, 79.34602667515135)) <=errorBound);
//        Assertions.assertTrue(Math.abs(408-equiDepthHistogram.rangeQuery(79.34602667515135, 104.54059090481564)) <=errorBound);
//        Assertions.assertTrue(Math.abs(410-equiDepthHistogram.rangeQuery(104.54059090481564, 128.16725341562798) )<=errorBound);
//        Assertions.assertTrue(Math.abs(385-equiDepthHistogram.rangeQuery(128.16725341562798, 148.6070893468689)) <=errorBound);
//        Assertions.assertTrue(Math.abs(404-equiDepthHistogram.rangeQuery(148.6070893468689, 172.38918153458843) )<=errorBound);
//        Assertions.assertTrue(Math.abs(403-equiDepthHistogram.rangeQuery(172.38918153458843, 199.22379903590425)) <=errorBound);
//        Assertions.assertTrue(Math.abs(384-equiDepthHistogram.rangeQuery(199.22379903590425, 222.9701722420302) )<=errorBound);
//        Assertions.assertTrue(Math.abs(300-equiDepthHistogram.rangeQuery(222.9701722420302, 239) )<=errorBound);

        Assertions.assertTrue(Math.abs(403-equiDepthHistogram.rangeQuery(0.0, 24.770805301880852)) <=errorBound);
        Assertions.assertTrue(Math.abs(396-equiDepthHistogram.rangeQuery(24.770805301880852, 50.74404908720416) )<=errorBound);
        Assertions.assertTrue(Math.abs(413-equiDepthHistogram.rangeQuery(50.74404908720416, 74.03145461751883)) <=errorBound);
        Assertions.assertTrue(Math.abs(391-equiDepthHistogram.rangeQuery(74.03145461751883, 97.78014234004232)) <=errorBound);
        Assertions.assertTrue(Math.abs(406-equiDepthHistogram.rangeQuery(97.78014234004232, 121.633294864397) )<=errorBound);
        Assertions.assertTrue(Math.abs(409-equiDepthHistogram.rangeQuery(121.633294864397, 143.33745009315945)) <=errorBound);
        Assertions.assertTrue(Math.abs(392-equiDepthHistogram.rangeQuery(143.33745009315945, 167.49265477439664) )<=errorBound);
        Assertions.assertTrue(Math.abs(400-equiDepthHistogram.rangeQuery(167.49265477439664, 192.2378206976074)) <=errorBound);
        Assertions.assertTrue(Math.abs(401-equiDepthHistogram.rangeQuery(192.2378206976074, 216.18081861728064) )<=errorBound);
        Assertions.assertTrue(Math.abs(389-equiDepthHistogram.rangeQuery(216.18081861728064, 239) )<=errorBound);


    }

    @Test
    public void mergeTest()
    {
        BarSplittingHistogram BASHistogram= new BarSplittingHistogram(9,10);
        //read from file and update with read elements
        File file= new File("data/data.csv");
        Scanner inputStream;
        try{
            inputStream = new Scanner(file);
            while(inputStream.hasNext()){
                String line= inputStream.next();
                BASHistogram.update(Integer.parseInt(line));
            }
            inputStream.close();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        EquiWidthHistogram illegalMergeWith= new EquiWidthHistogram(10.0,20.0,2);
        Assertions.assertThrows(IllegalArgumentException.class,()->BASHistogram.merge(illegalMergeWith));
        BarSplittingHistogram otherBASHistogram= new BarSplittingHistogram(4,8);
        //read from file and update with read elements
        File ofile= new File("data/testdata.csv");
        Scanner oinputStream;
        try{
            oinputStream = new Scanner(ofile);
            while(oinputStream.hasNext()){
                String oline= oinputStream.next();
                otherBASHistogram.update(Integer.parseInt(oline));
            }
            oinputStream.close();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }

//        BarSplittingHistogram BASHistogram= new BarSplittingHistogram(2,2);
//        TreeMap<Integer,Float> BASHbars= BASHistogram.getBars();//new TreeMap<>();
//        BASHbars.put(20, (float) 10.0);
//        BASHbars.put(30, (float) 10.0);
//        BASHbars.put(35, (float)10.0);
//        BASHbars.put(40, (float) 10.0);
////        BASHbars.put(42, (float) 10.0);
//        BASHistogram.setRight(45);
//        BASHistogram.setTotalFrequencies(40);
//
//        BarSplittingHistogram oBASHistogram= new BarSplittingHistogram(2,2);
//        TreeMap<Integer,Float> oBASHbars= oBASHistogram.getBars();//new TreeMap<>();
//        oBASHbars.put(0, (float) 5.0);
//        oBASHbars.put(18, (float) 5.0);
//        oBASHbars.put(22, (float) 5.0);
//        oBASHbars.put(30, (float) 5.0);
////        oBASHbars.put(60, (float) 5.0);
//        oBASHistogram.setRight(65);
//        oBASHistogram.setTotalFrequencies(20);
//
        BarSplittingHistogram mergeResultHistogram=BASHistogram.merge(otherBASHistogram);
        EquiDepthHistogram equiDepthHistogram= mergeResultHistogram.buildEquiDepthHistogram();
//
        double mergeTotalFrequency= BASHistogram.getTotalFrequencies()+otherBASHistogram.getTotalFrequencies();
        double errorBound= (4.0/BASHistogram.getP())*(mergeTotalFrequency/BASHistogram.getNumBuckets());
        Assertions.assertTrue(Math.abs(451-equiDepthHistogram.rangeQuery(0.0, 87.42878641314908)) <=errorBound);
        Assertions.assertTrue(Math.abs(472-equiDepthHistogram.rangeQuery(87.42878641314908, 92.00617609422699) )<=errorBound);
        Assertions.assertTrue(Math.abs(423-equiDepthHistogram.rangeQuery(92.00617609422699, 95.20326677381922)) <=errorBound);
        Assertions.assertTrue(Math.abs(306-equiDepthHistogram.rangeQuery(95.20326677381922, 97.92367802990043)) <=errorBound);
        Assertions.assertTrue(Math.abs(482-equiDepthHistogram.rangeQuery(97.92367802990043, 100.4430366285962) )<=errorBound);
        Assertions.assertTrue(Math.abs(333-equiDepthHistogram.rangeQuery(100.4430366285962, 102.9459715073611)) <=errorBound);
        Assertions.assertTrue(Math.abs(439-equiDepthHistogram.rangeQuery(102.9459715073611, 105.75260507445601) )<=errorBound);
        Assertions.assertTrue(Math.abs(460-equiDepthHistogram.rangeQuery(105.75260507445601, 109.18309716285403)) <=errorBound);
        Assertions.assertTrue(Math.abs(349-equiDepthHistogram.rangeQuery(109.18309716285403, 113.61787174032952) )<=errorBound);
        Assertions.assertTrue(Math.abs(384-equiDepthHistogram.rangeQuery(113.61787174032952,198.0) )<=errorBound);

    }

}
