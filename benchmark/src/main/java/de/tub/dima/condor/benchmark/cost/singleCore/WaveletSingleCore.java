package de.tub.dima.condor.benchmark.cost.singleCore;

import de.tub.dima.condor.core.synopsis.Histograms.EquiWidthHistogram;
import de.tub.dima.condor.core.synopsis.MergeableSynopsis;
import de.tub.dima.condor.core.synopsis.Sampling.ReservoirSampler;
import de.tub.dima.condor.core.synopsis.Sketches.CountMinSketch;
import de.tub.dima.condor.core.synopsis.StratifiedSynopsis;
import de.tub.dima.condor.core.synopsis.Synopsis;
import de.tub.dima.condor.core.synopsis.Wavelets.WaveletSynopsis;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.XORShiftRandom;

import java.io.*;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

/*java -jar target/benchmark-0.1.jar ...parameters*/

public class WaveletSingleCore {
    public static void run(String outputDir, String uniformTimestampedPath) throws Exception {
        // Please set up the correct path to the uniformTimestamped.gz
        String dataFilePath = uniformTimestampedPath;

        try {

            PrintWriter out = new PrintWriter(new FileOutputStream(new File(outputDir + "/wavelet-single-core.txt"), true), true);
            String line;
            String result = "Throughput=";
            String numRecords = "";
            WaveletSynopsis wavesynopsis = new WaveletSynopsis(10000);
            for (int i = 0; i < 10; i++) {
                GZIPInputStream gzipStream = new GZIPInputStream(new FileInputStream(dataFilePath));
                BufferedReader reader = new BufferedReader(new InputStreamReader(gzipStream, "UTF-8"));
                long number = 0;
                long startTime = System.currentTimeMillis();
                System.out.println("iteration: " + i);
                while (reader.ready() && (line = reader.readLine()) != null) {

                    String[] tokens = line.split(",");

                    ((WaveletSynopsis<Double>) wavesynopsis).update(Double.parseDouble(tokens[0]));

                    number++;
                }

                long endTime = System.currentTimeMillis();
                float timeElapsed = (endTime - startTime) / 1000f;
                float throughput = (number / timeElapsed);
                result += throughput + ",";
                numRecords += (number + ",");

                System.out.println(startTime);
                System.out.println(endTime);
                System.out.println(timeElapsed);
                System.out.println("Number of records: " + number);
                System.out.println("Throughput: " + throughput);
                System.out.println("---------------------");
            }

            System.out.println(result);
            System.out.println("Number of records: " + numRecords);
            out.append("\nSource: Uniform, Order-Based Synopsis: ");
            out.append("\n" + result);
            out.append("\nNumber of records: " + numRecords);
            out.append("\n--------------------------------------------------------------------");
            out.flush();
            out.close();
        } catch (FileNotFoundException e) {
            System.out.println("Please set up the correct path to the uniformTimestamped.gz");
            e.printStackTrace();
        }

    }

    private static MergeableSynopsis getSynopsis(String syn) {
        if (syn.equals("CountMinSketch")) {
            return new CountMinSketch(65536, 5, 7L);
        } else if (syn.equals("ReservoirSampler")) {
            return new ReservoirSampler(10000);
        } else if (syn.equals("EquiWidthHistogram")) {
            return new EquiWidthHistogram(0.0, 1001.0, 10);
        }


        throw new IllegalArgumentException(syn + " is not a valid synopsis for benchmarking");
    }
}
