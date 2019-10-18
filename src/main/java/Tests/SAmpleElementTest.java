package Tests;
import Sampling.SampleElement;
import org.junit.Assert;
import org.junit.Test;
import java.util.Date;

public class SAmpleElementTest {
    @Test
    public void comparetoTest(){
        Date date= new Date();
        long timestamp= date.getTime();
        SampleElement sample = new SampleElement("TestValue",timestamp);
        SampleElement sameSample = new SampleElement("TestValue", timestamp);
        SampleElement diffSample = new SampleElement("differentTestValue", timestamp);
        Assert.assertEquals(sample.compareTo(sameSample),0);
        Assert.assertEquals(sample.compareTo(diffSample),-1);

        for(int i=0;i<=100000000;i++);
        Date date1= new Date();
        long newTimeStamp = date1.getTime();

        SampleElement newSameSample = new SampleElement("TestValue", newTimeStamp);
        SampleElement newDiffSample = new SampleElement(12, newTimeStamp);
        Assert.assertNotEquals(sample.compareTo(newSameSample),0);
        Assert.assertNotEquals(sample.compareTo(newDiffSample),-1);
        Assert.assertNotEquals(sample.compareTo(newDiffSample),0);




        //Assert.assertEquals(sample.compareTo(newSameSample),0);

    }
}
