package Tests;
import Sampling.ReservoirSampler;
import org.junit.Test;
import org.junit.Assert;
import org.decimal4j.util.DoubleRounder;

public class ReservoirSamplerTest {
    @Test(expected = Exception.class)
    public void illegalsamplesizeTest() throws Exception {
        ReservoirSampler Reservoir= new ReservoirSampler(12);
        ReservoirSampler other= new ReservoirSampler(10);
        Reservoir.merge(other);
    }
}
