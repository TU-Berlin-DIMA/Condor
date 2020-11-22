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
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.jetbrains.annotations.NotNull;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.*;
import java.net.URI;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
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
public class NYCTaxiRideSource implements ParallelSourceFunction<Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short>> {
    private static transient DateTimeFormatter timeFormatter =
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC();
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
     * @param dataFilePath The gzipped input file from which the TaxiRide records are read.
     */
    public NYCTaxiRideSource(String dataFilePath, long runtime, int throughput) {
        this(dataFilePath, runtime, throughput, new ArrayList<>());
    }

    /**
     * Serves the TaxiRide records from the specified and ordered gzipped input file.
     * Rides are served out-of time stamp order with specified maximum random delay
     * in a serving speed which is proportional to the specified serving speed factor.
     *
     * @param dataFilePath The gzipped input file from which the TaxiRide records are read.
     */
    public NYCTaxiRideSource(String dataFilePath, long runtime, int throughput, final List<Tuple2<Long, Long>> gaps) {
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
    public NYCTaxiRideSource(long runtime, int throughput, final List<Tuple2<Long, Long>> gaps) {
        this.dataFilePath = "/Users/joschavonhein/Workspace/Condor/data/nycTaxiRides.gz";
//        this.dataFilePath = "EDADS/Data/nycTaxiRides.gz";
        this.throughput = throughput;
        this.gaps = gaps;
        this.runtime = runtime;
    }

    public NYCTaxiRideSource(long runtime, int throughput) {
        this.dataFilePath = "/share/hadoop/EDADS/nycTaxiRides.gz";
//        this.dataFilePath = "EDADS/Data/nycTaxiRides.gz";
        this.throughput = throughput;
        this.gaps = new ArrayList<>();
        this.runtime = runtime;
    }

    @Override
    public void run(SourceContext<Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short>> sourceContext) throws Exception {

        gzipStream = new GZIPInputStream(new FileInputStream(dataFilePath));
        reader = new BufferedReader(new InputStreamReader(gzipStream, "UTF-8"));

        long startTime = System.currentTimeMillis();
        long endTime = startTime + runtime;
        loop:
        while (running) {
            long startTs = System.currentTimeMillis();

            for (int i = 0; i < throughput; i++) {
                Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short> ride = readNextTuple();
                if (ride == null) {
                    break loop;
                }
                sourceContext.collect(ride);
            }

            if (runtime != -1) {
                while (System.currentTimeMillis() < startTs + 1000) {
                    // active waiting
                }

                if (endTime <= System.currentTimeMillis())
                    running = false;
            }
        }

        this.reader.close();
        this.reader = null;
        this.gzipStream.close();
        this.gzipStream = null;

    }

    private void emitValue(final Tuple11<Long, Long, Long, Boolean, Long, Long, Float, Float, Float, Float, Short> ride, final SourceContext<Tuple11<Long, Long, Long, Boolean, Long, Long, Float, Float, Float, Float, Short>> ctx) {

        if (getEventTime(ride) > nextGapStart) {
            ThroughputStatistics.getInstance().pause(true);
            //Environment.out.println("in Gap");
            if (getEventTime(ride) > this.nextGapEnd) {
                ThroughputStatistics.getInstance().pause(false);
                this.currentGapIndex++;
                if (currentGapIndex < gaps.size()) {
                    this.nextGapStart = this.gaps.get(currentGapIndex).f0;
                    this.nextGapEnd = this.nextGapStart + this.gaps.get(currentGapIndex).f1;
                }
            } else
                return;
        }
        ctx.collect(ride);
    }

    private Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short> readNextTuple() throws Exception {
        String line;
        Tuple11<Long, Long, Long, Boolean, Long, Long, Float, Float, Float, Float, Short> ride;
        if (reader.ready() && (line = reader.readLine()) != null) {
            // read first ride
            return fromString(line);
        } else {
            return null;
        }
    }

    /**
     * Total number of bytes 75.
     * <p>
     * f0:  rideId         : Long      // a unique id for each ride
     * f1:  taxiId         : Long      // a unique id for each taxi
     * f2:  driverId       : Long      // a unique id for each driver
     * f3:  isStart        : Boolean   // TRUE for ride start events, FALSE for ride end events
     * f4:  startTime      : Long      // the start time of a ride
     * f5:  endTime        : Long      // the end time of a ride, "1970-01-01 00:00:00" for start events
     * f6:  startLon       : Double     // the longitude of the ride start location
     * f7:  startLat       : Double     // the latitude of the ride start location
     * f8:  endLon         : Double     // the longitude of the ride end location
     * f9:  endLat         : Double     // the latitude of the ride end location
     * f10:  passengerCnt  : Short     // number of passengers on the ride
     */
    public Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short> fromString(String line) {

        String[] tokens = line.split(",");
        if (tokens.length != 11) {
            throw new RuntimeException("Invalid record: " + line);
        }

        Tuple11 ride = new Tuple11();

        try {
            ride.f0 = Long.parseLong(tokens[0]);

            switch (tokens[1]) {
                case "START":
                    ride.f3 = true;
//                    ride.f4 = DateTime.parse(tokens[2], timeFormatter).getMillis();
//                    ride.f5 = DateTime.parse(tokens[3], timeFormatter).getMillis();
                    break;
                case "END":
                    ride.f3 = false;
//                    ride.f5 = DateTime.parse(tokens[2], timeFormatter).getMillis();
//                    ride.f4 = DateTime.parse(tokens[3], timeFormatter).getMillis();
                    break;
                default:
                    throw new RuntimeException("Invalid record: " + line);
            }

            if (runtime == -1) {
                ride.f4 = 1603388370564L;
                ride.f5 = 1603388370564L;
            } else {
                ride.f4 = System.currentTimeMillis();
                ride.f5 = System.currentTimeMillis();
            }

            ride.f6 = tokens[4].length() > 0 ? Double.parseDouble(tokens[4]) : 0.0f;
            ride.f7 = tokens[5].length() > 0 ? Double.parseDouble(tokens[5]) : 0.0f;
            ride.f8 = tokens[6].length() > 0 ? Double.parseDouble(tokens[6]) : 0.0f;
            ride.f9 = tokens[7].length() > 0 ? Double.parseDouble(tokens[7]) : 0.0f;
            ride.f10 = Short.parseShort(tokens[8]);
            ride.f1 = Long.parseLong(tokens[9]);
            ride.f2 = Long.parseLong(tokens[10]);

        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid record: " + line, nfe);
        }

        return ride;
    }

    public long getEventTime(Tuple11<Long, Long, Long, Boolean, Long, Long, Float, Float, Float, Float, Short> ride) {
        if (ride.f3) {
            return ride.f4;
        } else {
            return ride.f5;
        }
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

