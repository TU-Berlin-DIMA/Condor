package de.tub.dima.condor.core.tests;
/*

import Synopsis.Sketches.FastAGMS;
import FlinkScottyConnector.BuildSynopsis;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.Environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import javax.annotation.Nullable;

public class FastAGMSTest {
    public static void main(String[] args) throws Exception {


        // set up the streaming execution Environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        int keyField = 0;

        Integer width = 64;
        Integer height = 32;
        Long seed = 11l;

        Object[] parameters = new Object[]{width, height, seed};
        Class<FastAGMS> sketchClass = FastAGMS.class;

        Time windowTime = Time.minutes(1);

        DataStream<String> line = env.readTextFile("data/timestamped.csv");
        DataStream<Tuple3<Integer, Integer, Long>> timestamped = line.flatMap(new BashHistogramTest.CreateTuplesFlatMap()) // Create the tuples from the incoming Data
                .assignTimestampsAndWatermarks(new BashHistogramTest.CustomTimeStampExtractor()); // extract the timestamps and add watermarks
>>>>>>> be65f853870a90847f1e5b9e6d956d878a141ee7:EDADS/src/main/java/Tests/FastAGMSTest.java

/**
 * @author Zahra Salmani
 */
/*
<<<<<<< HEAD:EDADS/src/main/java/Tests/FastAMSTest.java
=======
        SingleOutputStreamOperator<FastAGMS> fastAMS = BuildSynopsis.timeBasedRebalanced(timestamped, windowTime, keyField, sketchClass, parameters);
>>>>>>> be65f853870a90847f1e5b9e6d956d878a141ee7:EDADS/src/main/java/Tests/FastAGMSTest.java

public class FastAMSTest {

<<<<<<< HEAD:EDADS/src/main/java/Tests/FastAMSTest.java
    @Test
    public void updateTest(){
        int width=200;
        int height=10;
        FastAMS fastAMS=updateFromFile(new FastAMS(width,height,345345L),"data/dataset.csv");

        int [][] fastAMSArray =fastAMS.getArray();
        int [] rowSumArray= new int [height];
        int rowSum=0;
        for(int i=0; i< height;i++){
            rowSum=0;
            for (int j=0; j<width;j++){
                rowSum+=Math.abs(fastAMSArray[i][j]);
=======
        DataStream<Long> queryResult = fastAMS.map(new MapFunction<FastAGMS, Long>() {
            @Override
            public Long map(FastAGMS sketch) throws Exception {
                return sketch.estimateF2();
>>>>>>> be65f853870a90847f1e5b9e6d956d878a141ee7:EDADS/src/main/java/Tests/FastAGMSTest.java
            }
            rowSumArray[i]=rowSum;
        }
        int correct=0;
        double error_bound= Math.sqrt(51467/200d);
        Environment.out.println(error_bound);
        for (int element :rowSumArray){
            Environment.out.println(element);
            Environment.out.println(Math.abs(element-3173));
            Environment.out.println("------------------------");
            //Assertions.assertTrue(element==4000);
        }



    }

    private FastAMS updateFromFile(FastAMS fastAMS, String fileName){
        //String fileName= file;
        File file= new File(fileName);
        // this gives you a 2-dimensional array of strings
        Scanner inputStream;
        try{
            inputStream = new Scanner(file);
            while(inputStream.hasNext()){
                String line= inputStream.next();
                //line=line.substring(0, line.length() - 1);
                fastAMS.update(Integer.parseInt(line));
            }
            inputStream.close();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return fastAMS;
    }

}
*/