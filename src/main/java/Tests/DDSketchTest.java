package Tests;

import Sketches.DDSketch;
import Sketches.CountMinSketch;
//import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

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

        //DDSketch zeroDDSketch=new DDSketch(0.01,2000);
        for(int i=0;i<10;i++){
            ddSketch.update(0);
        }
        //TreeMap<Integer, Integer> countszeroSketchcount= zeroDDSketch.getCounts();
        Assertions.assertTrue(counts.size()==1);
        Assertions.assertTrue(ddSketch.getZeroCount()==10);





    }
}
