package Tests;

import Histograms.EquiWidthHistogram;
import Histograms.EquiWidthHistogram4LT;
import Synopsis.BuildSynopsis;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.junit.Assert;
import org.testng.junit.IJUnitTestRunner;

import javax.annotation.Nullable;
import java.nio.file.ClosedWatchServiceException;

public class EquiWidthHistogramTest {
  @Test(expected = IllegalArgumentException.class)
    public void illegalboundTest(){
      Double lowerBound=12.2;
      Double upperBound=5.0;
      EquiWidthHistogram eqwhistogram = new EquiWidthHistogram(lowerBound,upperBound,10);}
    @Test(expected = IllegalArgumentException.class)
    public void illegalbucketnumTest(){
        Double lowerBound=12.2;
        Double upperBound=20.0;
        Integer bucket= -20;
        EquiWidthHistogram eqwhistogram = new EquiWidthHistogram(lowerBound,upperBound,bucket);}
    @Test(expected = IllegalArgumentException.class)
    public void nullbucketnumTest(){
        Double lowerBound=12.2;
        Double upperBound=20.0;
        Integer bucket= null;
        EquiWidthHistogram eqwhistogram = new EquiWidthHistogram(lowerBound,upperBound,bucket);}
    @Test
    public void bucketlengthTest(){
        Double lowerBound=12.6;
        Double upperBound=20.0;
        Integer bucket= 4;
        EquiWidthHistogram eqwhistogram = new EquiWidthHistogram(lowerBound,upperBound,bucket);
        System.out.println(eqwhistogram.getbucketLength());
        Assert.assertTrue(eqwhistogram.getbucketLength() == 1.85);
    }

}
