/*
 * Copyright 2015 data Artisans GmbH, 2019 Ververica GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.tub.dima.condor.benchmark.sources.input;

import de.tub.dima.condor.benchmark.throughputUtils.ThroughputStatistics;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * This SourceFunction generates a data stream of TaxiRide records which are
 * read from a gzipped input file. Each record has a time stamp and the input file must be
 * ordered by this time stamp.
 * <p>
 * In order to simulate a realistic stream source, the SourceFunction serves events proportional to
 * their timestamps. In addition, the serving of events can be delayed by a bounded random delay
 * which causes the events to be served slightly out-of-order of their timestamps.
 * <p>
 * The serving speed of the SourceFunction can be adjusted by a serving speed factor.
 * A factor of 60.0 increases the logical serving time by a factor of 60, i.e., events of one
 * minute (60 seconds) are served in 1 second.
 * <p>
 * This SourceFunction is an EventSourceFunction and does continuously emit watermarks.
 * Hence it is able to operate in event time mode which is configured as follows:
 * <p>
 * StreamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
 */
public class UniformFileDistributionSource implements ParallelSourceFunction<Tuple3<Integer, Integer, Long>> {

    private final String dataFilePath;

    private transient BufferedReader reader;
    private transient InputStream gzipStream;


    private final long runtime;

    private final int throughput;
    private boolean running = true;

    private final List<Tuple2<Long, Long>> gaps;
    private int currentGapIndex;

    private long nextGapStart = 0;
    private long nextGapEnd;



    /**
     * Serves the TaxiRide records from the specified and ordered gzipped input file.
     * Rides are served exactly in order of their time stamps
     * in a serving speed which is proportional to the specified serving speed factor.
     *
     * @param dataFilePath       The gzipped input file from which the TaxiRide records are read.
     */
    public UniformFileDistributionSource(String dataFilePath, long runtime, int throughput) {
        this(dataFilePath, runtime, throughput, new ArrayList<>());
    }

    /**
     * Serves the TaxiRide records from the specified and ordered gzipped input file.
     * Rides are served out-of time stamp order with specified maximum random delay
     * in a serving speed which is proportional to the specified serving speed factor.
     *
     * @param dataFilePath       The gzipped input file from which the TaxiRide records are read.
     *
     */
    public UniformFileDistributionSource(String dataFilePath, long runtime, int throughput, final List<Tuple2<Long, Long>> gaps) {
        this.dataFilePath = dataFilePath;
        this.throughput = throughput;
        this.gaps = gaps;
        this.runtime = runtime;
    }

    /**
     * Serves the TaxiRide records from the specified and ordered gzipped input file.
     * Rides are served out-of time stamp order with specified maximum random delay
     * in a serving speed which is proportional to the specified serving speed factor.
     */
    public UniformFileDistributionSource(long runtime, int throughput, final List<Tuple2<Long, Long>> gaps) {
        this.dataFilePath = "/share/hadoop/EDADS/uniformTimestamped.gz";
//        this.dataFilePath = "EDADS/Data/uniformTimestamped.gz";
        this.throughput = throughput;
        this.gaps = gaps;
        this.runtime = runtime;
    }

    public UniformFileDistributionSource(long runtime, int throughput) {
        this.dataFilePath = "/share/hadoop/EDADS/uniformTimestamped.gz";
//        this.dataFilePath = "EDADS/Data/uniformTimestamped.gz";
        this.throughput = throughput;
        this.gaps = new ArrayList<>();
        this.runtime = runtime;
    }

    @Override
    public void run(SourceContext<Tuple3<Integer, Integer, Long>> sourceContext) throws Exception {

        gzipStream = new GZIPInputStream(new FileInputStream(dataFilePath));
        reader = new BufferedReader(new InputStreamReader(gzipStream, "UTF-8"));

        long startTime = System.currentTimeMillis();
        long endTime = startTime + runtime;
        loop:
        while (running) {
            long startTs = System.currentTimeMillis();

            for (int i = 0; i < throughput; i++) {
                Tuple3<Integer, Integer, Long> tuple = readNextTuple();
                if (tuple == null){
                    break loop;
                }
                sourceContext.collect(tuple);
            }

            while (System.currentTimeMillis() < startTs + 1000) {
                // active waiting
            }

            if(endTime <= System.currentTimeMillis())
                running = false;
        }

        this.reader.close();
        this.reader = null;
        this.gzipStream.close();
        this.gzipStream = null;

    }

    private void emitValue(final Tuple3<Integer, Integer, Long> tuple, final SourceContext<Tuple3<Integer, Integer, Long>> ctx) {

        if (tuple.f2 > nextGapStart) {
            ThroughputStatistics.getInstance().pause(true);
            //Environment.out.println("in Gap");
            if (tuple.f2 > this.nextGapEnd) {
                ThroughputStatistics.getInstance().pause(false);
                this.currentGapIndex++;
                if (currentGapIndex < gaps.size()) {
                    this.nextGapStart = this.gaps.get(currentGapIndex).f0;
                    this.nextGapEnd = this.nextGapStart + this.gaps.get(currentGapIndex).f1;
                }
            } else
                return;
        }
        ctx.collect(tuple);
    }

    private Tuple3<Integer, Integer, Long> readNextTuple() throws Exception {
        String line;
        if (reader.ready() && (line = reader.readLine()) != null) {
            // read first tuple
            return fromString(line);
        } else {
            return null;
        }
    }

    /**
     * f0:  key            : Integer      // a random integer selected from a ZIPF distribution
     * f1:  value          : Integer      // a random integer
     * f2:  eventTime      : Long      // a unique id for each driver
     */
    public Tuple3<Integer, Integer, Long> fromString(String line) {

        String[] tokens = line.split(",");
        if (tokens.length != 3) {
            throw new RuntimeException("Invalid record: " + line);
        }

        Tuple3<Integer, Integer, Long> tuple = new Tuple3<>();

        try {
            tuple.f0 = Integer.parseInt(tokens[0]);
            tuple.f1 = Integer.parseInt(tokens[1]);
//            tuple.f2 = Long.parseLong(tokens[2]);
            tuple.f2 = System.currentTimeMillis();
        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid record: " + line, nfe);
        }

        return tuple;
    }

    @Override
    public void cancel() {
        try {
            if (this.reader != null) {
                this.reader.close();
            }
            if (this.gzipStream != null) {
                this.gzipStream.close();
            }
        } catch (IOException ioe) {
            throw new RuntimeException("Could not cancel SourceFunction", ioe);
        } finally {
            this.reader = null;
            this.gzipStream = null;
            this.running = false;
        }
    }

}

