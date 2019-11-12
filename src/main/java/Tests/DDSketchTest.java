package Tests;

import Sketches.DDSketch;
import Sketches.CountMinSketch;
//import org.junit.Assert;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

/**
 * @author Zahra Salmani
 */
public class DDSketchTest {
    @Test
    public void constructionTest(){
        Assertions.assertThrows(IllegalArgumentException.class,()->new DDSketch(1.0,650));
        Assertions.assertThrows(IllegalArgumentException.class,()->new DDSketch(0.0,650));
        Assertions.assertThrows(IllegalArgumentException.class,()->new DDSketch(12.0,650));
        Assertions.assertThrows(IllegalArgumentException.class,()->new DDSketch(-12.0,650));
    }

    @Test
    public void updateTest(){
        DDSketch ddSketch=new DDSketch(0.01,2000);
        //System.out.println(ddSketch.minIndexableValue()); //2.2700248455477507E-308, 1.7798941929329858
        for(int i=0;i<200;i++) {
            ddSketch.update(800);
        }
        TreeMap<Integer, Integer> counts= ddSketch.getCounts();
        Assertions.assertTrue(counts.size()==1);
        for(Map.Entry<Integer,Integer> element:counts.entrySet()){
            Assertions.assertTrue(element.getValue()==200);
        }
        Assertions.assertThrows(IllegalArgumentException.class,()->ddSketch.update(-30));


        for(int i=0;i<10;i++){
            ddSketch.update(0);
        }

        Assertions.assertTrue(counts.size()==1);
        Assertions.assertTrue(ddSketch.getZeroCount()==10);

        int[]  indexarray= new int[]{0,34,54,69,80,89,97,103,109,115,119,124,128,131,135,138,141,144,147,
                149,152,154,156,158,160,162,164,166,168,170,171,173,174,176,177,179,180,181,183,184,185,186,
                188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,
                211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,
                234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,
                258,259,260,261,262,263,264,265,266,267,268,269,270,271,272,273};

        int[] countarray= new int[]{18,19,10,18,12,14,16,24,17,16,18,15,15,26,13,18,22,13,15,17,16,11,10,
                16,15,15,16,10,22,11,10,14,18,9,16,18,19,15,25,15,21,17,13,19,9,8,21,20,10,10,21,19,18,24,
                15,11,10,41,19,13,20,32,17,16,35,17,18,42,13,27,19,45,15,33,44,18,25,33,40,17,29,24,34,32,28,
                30,37,37,30,49,38,28,58,39,33,41,51,33,64,58,37,67,56,60,44,49,48,40,60,60,69,59,47,59,44,68,54,51,69,65,78,71,82,57,76,87,83,71};

        TreeMap<Integer, Integer> refrenceCount = new TreeMap<>();
        for(int i=0;i<indexarray.length;i++){
            refrenceCount.put(indexarray[i],countarray[i]);
        }

        DDSketch ddSketch1=new DDSketch(0.01,2000);
        String fileName= "data/dataset.csv";
        File file= new File(fileName);
        // this gives you a 2-dimensional array of strings
        Scanner inputStream;
        try{
            inputStream = new Scanner(file);
            while(inputStream.hasNext()){
                String line= inputStream.next();
                ddSketch1.update(Integer.parseInt(line));
            }
            inputStream.close();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        TreeMap<Integer, Integer> counts1=ddSketch1.getCounts();
        Assertions.assertEquals(counts1,refrenceCount);


    }
    @Test
    public void getValueAtQuantileTest(){
        double relativeAccuracy= 0.01;
        DDSketch ddSketch=new DDSketch(relativeAccuracy,2000);
        String fileName= "data/dataset.csv";
        File file= new File(fileName);
        // this gives you a 2-dimensional array of strings
        Scanner inputStream;
        try{
            inputStream = new Scanner(file);
            while(inputStream.hasNext()){
                String line= inputStream.next();
                ddSketch.update(Integer.parseInt(line));
            }
            inputStream.close();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        ddSketch.getValueAtQuantile(0.01);
        System.out.println(ddSketch.getValueAtQuantile(0.0));
        System.out.println(ddSketch.getValueAtQuantile(0.01));
        System.out.println(ddSketch.getValueAtQuantile(0.05));
        System.out.println(ddSketch.getValueAtQuantile(0.2));
        System.out.println(ddSketch.getValueAtQuantile(0.5));
        System.out.println(ddSketch.getValueAtQuantile(0.75));
        System.out.println(ddSketch.getValueAtQuantile(1));
        System.out.println(ddSketch.getCounts().size());
        Assertions.assertTrue(Math.abs(ddSketch.getValueAtQuantile(0.0)-0)<=(relativeAccuracy*0));
        Assertions.assertTrue(Math.abs(ddSketch.getValueAtQuantile(0.01)-2)<=(relativeAccuracy*2));
        Assertions.assertTrue(Math.abs(ddSketch.getValueAtQuantile(0.05)-12)<=(relativeAccuracy*12));
        Assertions.assertTrue(Math.abs(ddSketch.getValueAtQuantile(0.2)-51)<=(relativeAccuracy*51));
        Assertions.assertTrue(Math.abs(ddSketch.getValueAtQuantile(0.5)-121)<=(relativeAccuracy*121));
        Assertions.assertTrue(Math.abs(ddSketch.getValueAtQuantile(0.75)-179)<=(relativeAccuracy*179));
        Assertions.assertTrue(Math.abs(ddSketch.getValueAtQuantile(1)-239)<=(relativeAccuracy*239));
    }
}
