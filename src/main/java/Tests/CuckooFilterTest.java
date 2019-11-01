package Tests;

import Sketches.CuckooFilter;
import org.junit.Assert;
import org.junit.Test;

public class CuckooFilterTest {

    @Test
    public void updateTest(){
        CuckooFilter cuckooFilter= new CuckooFilter(5,1000,653476235L);
        int [] sample= new int[]{196,  95, 178, 191, 219,  46, 158,  34,  45,  50};
        for(int element:sample){
            cuckooFilter.update(element);
        }
        Assert.assertFalse(cuckooFilter.getBucketStatus());

        for(int i=0;i<12;i++) {
            cuckooFilter.update(34);
        }
        Assert.assertTrue(cuckooFilter.getBucketStatus());
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
        double falsePositiveBound= (2*5)/256.0;
        Assert.assertTrue(falsePositiveCount/3000.0 <= falsePositiveBound);

        int truePositivecount=0;
        for(int j=1000; j<3000;j++){
            if(cuckooFilter.lookup(j)){
                truePositivecount++;
            }
        }

        if(!cuckooFilter.getBucketStatus()){
            Assert.assertEquals(2000,truePositivecount);
            Assert.assertTrue(cuckooFilter.lookup(2789));
            Assert.assertTrue(cuckooFilter.lookup(1500));
            Assert.assertTrue(cuckooFilter.lookup(1967));
        }
    }
}
