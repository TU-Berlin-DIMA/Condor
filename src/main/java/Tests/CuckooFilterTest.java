package Tests;

import Sketches.CuckooFilter;
import Sketches.CountMinSketch;
//import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
/**
 * @author Zahra Salmani
 */
public class CuckooFilterTest {

    @Test
    public void updateTest(){
        CuckooFilter cuckooFilter= new CuckooFilter(5,1000,653476235L);
        int [] sample= new int[]{196,  95, 178, 191, 219,  46, 158,  34,  45,  50};
        for(int element:sample){
            cuckooFilter.update(element);
        }
        Assertions.assertFalse(cuckooFilter.getBucketStatus());

        for(int i=0;i<12;i++) {
            cuckooFilter.update(34);
        }
        Assertions.assertTrue(cuckooFilter.getBucketStatus());
    }

    @Test
    public void lookupTest(){
        CuckooFilter cuckooFilter= new CuckooFilter(5,1000,653476235L);
        for(int i=1000;i< 3000;i++){
            cuckooFilter.update(i);
        }
        int positiveCount=0;
        for(int j=0; j<3000;j++){
            if(cuckooFilter.lookup(j)){
                positiveCount++;
            }
        }
        int falsePositiveCount=positiveCount-2000;
        double falsePositiveBound= (2*5)/(256.0*3);
        Assertions.assertTrue(falsePositiveCount/3000.0 <= falsePositiveBound);

        int truePositivecount=0;
        for(int j=1000; j<3000;j++){
            if(cuckooFilter.lookup(j)){
                truePositivecount++;
            }
        }
        if(!cuckooFilter.getBucketStatus()){
            Assertions.assertEquals(2000,truePositivecount);
            Assertions.assertTrue(cuckooFilter.lookup(2789));
            Assertions.assertTrue(cuckooFilter.lookup(1500));
            Assertions.assertTrue(cuckooFilter.lookup(1967));
        }
    }
    @Test
    public void deleteTest(){
        CuckooFilter cuckooFilter= new CuckooFilter(5,1000,653476235L);
        for(int i=0;i<10;i++){
            cuckooFilter.update(13);
        }
        if(cuckooFilter.getFingerprint(13)!=cuckooFilter.getFingerprint(45)) {
            Assertions.assertFalse(cuckooFilter.delete(45));
        }

        for(int i=0;i<10;i++){
            Assertions.assertTrue(cuckooFilter.delete(13));
        }
        Assertions.assertFalse(cuckooFilter.delete(13));
    }

    @Test
    public void illegalArgMergeTest() throws Exception {
        CuckooFilter cuckooFilter= new CuckooFilter(5,1000,653476235L);
        CountMinSketch otherType = new CountMinSketch(12,10,7136673L);
        CuckooFilter otherSize= new CuckooFilter(600,1000,653476235L);
        CuckooFilter otherLength =  new CuckooFilter(5,1500,653476235L);

        Assertions.assertThrows(Exception.class,()->cuckooFilter.merge(otherType));
        Assertions.assertThrows(Exception.class,()->cuckooFilter.merge(otherSize));
        Assertions.assertThrows(Exception.class,()->cuckooFilter.merge(otherLength));
    }

    @Test
    public void mergeTest() throws Exception{
        CuckooFilter cuckooFilter= new CuckooFilter(5,1000,653476235L);
        CuckooFilter other =  new CuckooFilter(5,1000,653476235L);
        for(int i=0;i<5;i++){
            cuckooFilter.update(200);
        }
        for(int j=0;j<5;j++){
            other.update(200);
        }
        Assertions.assertTrue(cuckooFilter.merge(other) instanceof CuckooFilter);
        Assertions.assertTrue(cuckooFilter.getElementsProcessed()==10);


        CuckooFilter overFlowCuckooFilter= new CuckooFilter(5,1000,653476235L);

        for(int i=0;i<7;i++){
            overFlowCuckooFilter.update(200);
        }
        Assertions.assertThrows(Exception.class,()->overFlowCuckooFilter.merge(other));
    }

}
