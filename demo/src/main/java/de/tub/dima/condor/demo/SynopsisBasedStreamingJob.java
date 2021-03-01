package de.tub.dima.condor.demo;

import de.tub.dima.condor.core.synopsis.Sketches.CountMinSketch;
import de.tub.dima.condor.core.synopsis.WindowedSynopsis;
import de.tub.dima.condor.flinkScottyConnector.evaluator.ApproximateDataAnalytics;
import de.tub.dima.condor.flinkScottyConnector.evaluator.utils.QueryResult;
import de.tub.dima.condor.flinkScottyConnector.processor.SynopsisBuilder;
import de.tub.dima.condor.flinkScottyConnector.processor.configs.BuildConfiguration;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Skeleton for a Condor Synopsis-based Streaming Job.
 *
 * For more examples check the benchmark package. There you will find a vast collection of examples.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class SynopsisBasedStreamingJob {
    public static void main(String[] args) throws Exception {
        // Set up the streaming execution Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Initialize Input stream
//        DataStream<> inputStream = env
//                .addSource(new DataSource())
//                .assignTimestampsAndWatermarks(new TimestampsAndWatermarks());

        //-------- PROCESSING --------
        // Set up other configuration parameters - More documentation on the BuildConfiguration class
//        Class<CountMinSketch> synopsisClass = CountMinSketch.class;
//        Window[] windows = {new TumblingWindow(WindowMeasure.Time, 5000)};
//        Object[] synopsisParameters = new Object[]{65536, 5, 7L};

        // Create a new BuildConfiguration
//        BuildConfiguration config = new BuildConfiguration(inputStream, synopsisClass, windows, synopsisParameters, parallelism);

        // Build the synopses
//        SingleOutputStreamOperator<WindowedSynopsis<CountMinSketch>> synopsesStream = SynopsisBuilder.build(env, config);


        //-------- EVALUATION --------
        // Initialize Query stream
//        DataStream<Integer> queryStream = env.addSource(new IPQuerySource(Time.seconds(20), queryThroughput, Time.seconds(40)));

        // Evaluate the synopsis stream based on the query stream
//        SingleOutputStreamOperator<QueryResult<Integer, Integer>> resultStream = ApproximateDataAnalytics.queryLatest(synopsesStream, queryStream, new QueryCountMin());

        // Output the result stream
//        resultStream.addSink(new SinkFunction() {
//            @Override
//            public void invoke(final Object value) throws Exception {
//                //Environment.out.println(value);
//            }
//        });

        // Execute Condor's Synopsis-based Streaming Job
        env.execute("My Condor Synopsis-based Streaming Job");
    }
}
