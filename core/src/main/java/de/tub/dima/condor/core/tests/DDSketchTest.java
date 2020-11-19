package de.tub.dima.condor.core.tests;

import de.tub.dima.condor.core.synopsis.Sketches.DDSketch;
import de.tub.dima.condor.core.synopsis.Sketches.CountMinSketch;
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
        for(int i=0;i<200;i++) {
            ddSketch.update(800);
        }
        TreeMap<Integer, Integer> counts= ddSketch.getCounts();
        Assertions.assertTrue(counts.size()==1);
        for(Map.Entry<Integer,Integer> element:counts.entrySet()){
            Assertions.assertTrue(element.getValue()==200);
        }
        //test adding negative values
        Assertions.assertThrows(IllegalArgumentException.class,()->ddSketch.update(-30));

        //Test adding zero and zerocounts
        for(int i=0;i<10;i++){
            ddSketch.update(0);
        }
        Assertions.assertTrue(counts.size()==1);
        Assertions.assertTrue(ddSketch.getZeroCount()==10);

        //create the expected index=value tree corresponding to the data in file
        int[]  indexarray= new int[]{0,34,54,69,80,89,97,103,109,115,119,124,128,131,135,138,141,144,147,
                149,152,154,156,158,160,162,164,166,168,170,171,173,174,176,177,179,180,181,183,184,185,186,
                188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,
                211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,
                234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,
                258,259,260,261,262,263,264,265,266,267,268,269,270,271,272,273};

        int[] countarray= new int[]{18,19,10,18,12,14,16,24,17,16,18,15,15,26,13,18,22,13,15,17,16,11,10,
                16,15,15,16,10,22,11,10,14,18,9,16,18,19,15,25,15,21,17,13,19,9,8,21,20,10,10,21,19,18,24,
                15,11,10,41,19,13,20,32,17,16,35,17,18,42,13,27,19,45,15,33,44,18,25,33,40,17,29,24,34,32,
                28,30,37,37,30,49,38,28,58,39,33,41,51,33,64,58,37,67,56,60,44,49,48,40,60,60,69,59,47,59,
                44,68,54,51,69,65,78,71,82,57,76,87,83,71};

        TreeMap<Integer, Integer> refrenceCount = new TreeMap<>();
        for(int i=0;i<indexarray.length;i++){
            refrenceCount.put(indexarray[i],countarray[i]);
        }

        //update DDsketch with data have been read from file
        DDSketch ddSketchobject=new DDSketch(0.01,2000);
        DDSketch ddSketch1=updateFromFile(ddSketchobject,"data/dataset.csv");

        //compare update results with expected one
        TreeMap<Integer, Integer> counts1=ddSketch1.getCounts();
        Assertions.assertEquals(counts1,refrenceCount);
    }

    @Test
     public void updateWithCollapseTest(){
        DDSketch ddSketchobject=new DDSketch(0.01,2000);//DDsketch whose maxNumBin is large enough to not collapse lower buckets
        DDSketch ddSketch=updateFromFile(ddSketchobject,"data/dataset.csv");
        DDSketch collapseobject=new DDSketch(0.01,120);
        DDSketch collapseDDSketch=updateFromFile(collapseobject,"data/dataset.csv");// equall DDsketch whose number of buckets exceeds maxNumBins
        int collapsedElemetNum= ddSketch.getCounts().size()-collapseDDSketch.getMaxNumBins()+1;
        int collapsedValue=0;
        int lastCollapsedkey=0;
        for(int i=0;i<collapsedElemetNum;i++){
            Map.Entry<Integer, Integer> bin = ddSketch.getCounts().pollFirstEntry();
            collapsedValue+=bin.getValue();
            lastCollapsedkey=bin.getKey();
        }
        Assertions.assertEquals(collapseDDSketch.getCounts().firstKey(),lastCollapsedkey);
        Assertions.assertEquals(collapseDDSketch.getCounts().firstEntry().getValue(),collapsedValue);
        Assertions.assertTrue(Math.abs(collapseDDSketch.getValueAtQuantile(0.5)-121)<=(0.01*121));
        Assertions.assertTrue(Math.abs(ddSketch.getValueAtQuantile(0.75)-179)<=(0.01*179));

        //Assertions.assertEquals(collapseDDSketch.getCounts().firstKey(),109);
        //Assertions.assertEquals(collapseDDSketch.getCounts().firstEntry().getValue(),148);
    }
    @Test
    public void getValueAtQuantileTest(){
        double relativeAccuracy= 0.01;
        DDSketch ddSketchobject=new DDSketch(relativeAccuracy,2000);
        DDSketch ddSketch=updateFromFile(ddSketchobject,"data/dataset.csv");
//        DDSketch ddSketch=updateFromFile(ddSketchobject,"data/data.csv");
//        Assertions.assertTrue(Math.abs(ddSketch.getValueAtQuantile(0.0)-66)<=(relativeAccuracy*66));
//        Assertions.assertTrue(Math.abs(ddSketch.getValueAtQuantile(0.05)-84)<=(relativeAccuracy*84));
//        Assertions.assertTrue(Math.abs(ddSketch.getValueAtQuantile(0.2)-92)<=(relativeAccuracy*92));
//        Assertions.assertTrue(Math.abs(ddSketch.getValueAtQuantile(0.5)-100)<=(relativeAccuracy*100));
//        Assertions.assertTrue(Math.abs(ddSketch.getValueAtQuantile(0.75)-107)<=(relativeAccuracy*107));
//        Assertions.assertTrue(Math.abs(ddSketch.getValueAtQuantile(1.0)-141)<=(relativeAccuracy*141));

        Assertions.assertThrows(IllegalArgumentException.class,()->ddSketch.getValueAtQuantile(2));
        Assertions.assertThrows(IllegalArgumentException.class,()->ddSketch.getValueAtQuantile(-2));
        Assertions.assertTrue(Math.abs(ddSketch.getValueAtQuantile(0.05)-12.0)/12.0<=(relativeAccuracy));
        Assertions.assertTrue(Math.abs(ddSketch.getValueAtQuantile(0.0)-0)<=(relativeAccuracy*0));
        Assertions.assertTrue(Math.abs(ddSketch.getValueAtQuantile(0.05)-12)<=(relativeAccuracy*12));
        Assertions.assertTrue(Math.abs(ddSketch.getValueAtQuantile(0.2)-51)<=(relativeAccuracy*51));
        Assertions.assertTrue(Math.abs(ddSketch.getValueAtQuantile(0.5)-121)<=(relativeAccuracy*121));
        Assertions.assertTrue(Math.abs(ddSketch.getValueAtQuantile(0.75)-179)<=(relativeAccuracy*179));
        Assertions.assertTrue(Math.abs(ddSketch.getValueAtQuantile(1.0)-239)<=(relativeAccuracy*239));
    }

    @Test
    public void invertTest(){
        double relativeAccuracy= 0.01;
        //Test different illegal merge Scenarios
        DDSketch ddSketchObject=new DDSketch(relativeAccuracy,2000);
        DDSketch illegalDDSketch1=new DDSketch(0.5,2000);
        DDSketch illegalDDSketch2=new DDSketch(relativeAccuracy,1000);
        CountMinSketch countMinSketch= new CountMinSketch(12,10,7136673L);
        Assertions.assertThrows(Exception.class,()->ddSketchObject.invert(countMinSketch));
        Assertions.assertThrows(Exception.class,()->ddSketchObject.invert(illegalDDSketch1));
        Assertions.assertThrows(Exception.class,()->ddSketchObject.invert(illegalDDSketch2));

        // Test invert an empty DDSketch
        DDSketch ddSketch=updateFromFile(ddSketchObject,"data/dataset.csv");
        int oldGlobalCount=ddSketch.getGlobalCount();
        int oldZeroCount=ddSketch.getZeroCount();
        TreeMap<Integer, Integer> ddSketchCount=new TreeMap<>();
        ddSketchCount.putAll(ddSketch.getCounts());
        DDSketch otherObj=new DDSketch(relativeAccuracy,2000);
        Assertions.assertEquals( ddSketchCount,ddSketch.invert(otherObj).getCounts());
        Assertions.assertEquals(oldGlobalCount,ddSketch.getGlobalCount());
        Assertions.assertEquals(oldZeroCount,ddSketch.getZeroCount());

        //Test subtracting  a the same ddSketch
        DDSketch other=updateFromFile(otherObj,"data/dataset.csv");
        TreeMap<Integer, Integer> invertTheSameCount =ddSketch.invert(other).getCounts();
        Assertions.assertTrue(invertTheSameCount.isEmpty());

        //Test subtracting different element
        int[]  indexarray= new int[]{0, 34, 54, 69, 80, 89, 97, 103, 109, 115, 119, 124, 128, 131, 135, 138, 141,
                144, 147, 149, 152, 154, 156, 158, 160, 162, 164, 166, 168, 170, 171, 173, 174, 176, 177, 179, 180,
                181, 183, 184, 185, 186, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202,
                203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 239, 240, 241, 242, 243,
                244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255, 256, 257, 258, 259, 260, 261, 262, 263,
                264, 265, 266, 267, 268, 269, 270, 271, 272, 273};

        int[] countarray= new int[]{18, 19, 10, 18, 12, 14, 16, 24, 17, 16, 18, 15, 15, 26, 13, 18, 22,
                13, 15, 17, 16, 11, 10, 16, 15, 15, 16, 10, 22, 11, 10, 14, 18, 9, 16, 18, 19, 15, 25,
                15, 21, 17, 13, 19, 9, 8, 21, 20, 10, 10, 21, 19, 18, 24, 15, 11, 10, 41, 19, 12, 20, 31,
                17, 16, 35, 14, 17, 41, 9, 18, 12, 27, 8, 5, 17, 39, 29, 63, 56, 37, 66, 56, 60, 44, 49, 48,
                40, 60, 60, 69, 59, 47, 59, 44, 68, 54, 51, 69, 65, 78, 71, 82, 57, 76, 87, 83, 71};

        TreeMap<Integer, Integer> refrenceCount = new TreeMap<>();
        for(int i=0;i<indexarray.length;i++){
            refrenceCount.put(indexarray[i],countarray[i]);
        }

        DDSketch ddSketchWithDifferentObj=new DDSketch(relativeAccuracy,2000);
        DDSketch ddSketchWithDifferent=updateFromFile(ddSketchWithDifferentObj,"data/dataset.csv");
        DDSketch differentOtherObj=new DDSketch(relativeAccuracy,2000);
        DDSketch differentOtherobj=updateFromFile(differentOtherObj,"data/data.csv");
        TreeMap<Integer, Integer> differentOtherCounts =ddSketchWithDifferent.invert(differentOtherobj).getCounts();
        Assertions.assertEquals(differentOtherCounts,refrenceCount);

//        Set result = new HashSet(ddSketchCount.keySet());
//        result.addAll(otherCount.keySet());
//        Assertions.assertEquals(invertCount.keySet(),result); //whether mergeresult contains both sketches keys
//        for(Map.Entry<Integer,Integer> element:invertCount.entrySet()){
//            if(otherCount.containsKey(element.getKey())){
//                Assertions.assertTrue(element.getValue()==ddsketchCount.get(element.getKey())+otherCount.get(element.getKey()));
//            }
//            else{
//                Assertions.assertTrue(element.getValue()==ddsketchCount.get(element.getKey()));
//            }
//        }
//        Assertions.assertEquals(ddSketch.getGlobalCount(),oldGlobalCount+other.getGlobalCount());
//        Assertions.assertEquals(ddSketch.getZeroCount(),oldZeroCount+other.getZeroCount());

    }
//    @Test
//    public void decrementTest(){
//
//    }
    @Test
    public void mergeTest() throws Exception {
        double relativeAccuracy= 0.01;

        //Test different illegal merge Scenarios
        DDSketch ddSketchobject=new DDSketch(relativeAccuracy,2000);
        DDSketch illegalDDSketch1=new DDSketch(0.5,2000);
        DDSketch illegalDDSketch2=new DDSketch(relativeAccuracy,1000);
        CountMinSketch countMinSketch= new CountMinSketch(12,10,7136673L);
        Assertions.assertThrows(Exception.class,()->ddSketchobject.merge(countMinSketch));
        Assertions.assertThrows(Exception.class,()->ddSketchobject.merge(illegalDDSketch1));
        Assertions.assertThrows(Exception.class,()->ddSketchobject.merge(illegalDDSketch2));

        // Test merge with empty DDsketch
        DDSketch ddSketch=updateFromFile(ddSketchobject,"data/dataset.csv");
        int oldGlobalCount=ddSketch.getGlobalCount();
        int oldZeroCount=ddSketch.getZeroCount();
        TreeMap<Integer, Integer> ddsketchCount=new TreeMap<>();
        ddsketchCount.putAll(ddSketch.getCounts());

        DDSketch otherobj=new DDSketch(relativeAccuracy,2000);
        Assertions.assertEquals( ddsketchCount,ddSketch.merge(otherobj).getCounts());

        //Test merging with non empty ddsketch
        DDSketch other=updateFromFile(otherobj,"data/data.csv");
        TreeMap<Integer, Integer> otherCount=other.getCounts();
        TreeMap<Integer, Integer> mergeCount =ddSketch.merge(other).getCounts();
        Set result = new HashSet(ddsketchCount.keySet());
        result.addAll(otherCount.keySet());
        Assertions.assertEquals(mergeCount.keySet(),result); //whether mergeresult contains both sketches keys
        for(Map.Entry<Integer,Integer> element:mergeCount.entrySet()){
            if(otherCount.containsKey(element.getKey())){
            Assertions.assertTrue(element.getValue()==ddsketchCount.get(element.getKey())+otherCount.get(element.getKey()));
            }
            else{
                Assertions.assertTrue(element.getValue()==ddsketchCount.get(element.getKey()));
            }
        }
        Assertions.assertEquals(ddSketch.getGlobalCount(),oldGlobalCount+other.getGlobalCount());
        Assertions.assertEquals(ddSketch.getZeroCount(),oldZeroCount+other.getZeroCount());
    }

    @Test
    public void mergeWithCollapse(){
        // construct a sketch and update it, its normal number of mins without collapsing is 68
        DDSketch ddSketchObj=new DDSketch(0.01,68);
        DDSketch ddSketch=updateFromFile(ddSketchObj,"data/testdata.csv");
        TreeMap<Integer, Integer> ddsketchOldCount=new TreeMap<>();
        ddsketchOldCount.putAll(ddSketch.getCounts());

        // construct another sketch, choose it such that the result of its merge with the first sketch needs more than 68 bins
        DDSketch otherobj=new DDSketch(0.01,68);
        DDSketch other=updateFromFile(otherobj,"data/testdata2.csv");
        Set result = new HashSet(ddSketch.getCounts().keySet());
        result.addAll(other.getCounts().keySet());
        Assertions.assertTrue(result.size() > 68);//merge it with our larger sketch and test the condition

        //check merge results
        ddSketch.merge(other);
        Assertions.assertTrue(ddSketch.getCounts().firstKey() != ddsketchOldCount.firstKey());
        Assertions.assertTrue(ddSketch.getCounts().size()==68 && result.size()>ddSketch.getCounts().size());


    }

    public DDSketch updateFromFile(DDSketch ddSketch,String fileName){
        //String fileName= file;
        File file= new File(fileName);
        // this gives you a 2-dimensional array of strings
        Scanner inputStream;
        try{
            inputStream = new Scanner(file);
            while(inputStream.hasNext()){
                String line= inputStream.next();
                //line=line.substring(0, line.length() - 1);
                ddSketch.update(Integer.parseInt(line));
            }
            inputStream.close();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return ddSketch;
    }
}
