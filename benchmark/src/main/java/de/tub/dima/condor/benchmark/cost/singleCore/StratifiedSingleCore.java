package de.tub.dima.condor.benchmark.cost.singleCore;

import de.tub.dima.condor.core.synopsis.Sketches.CountMinSketch;
import de.tub.dima.condor.core.synopsis.Synopsis;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.XORShiftRandom;

import java.io.*;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

public class StratifiedSingleCore {
    public static void run(String outputDir) throws Exception {
        // Please set up the correct path to the uniformTimestamped.gz
        String dataFilePath = "<PATH-TO>/uniformTimestamped.gz";

        try {

            PrintWriter out = new PrintWriter(new FileOutputStream(new File(outputDir + "/single-core.txt"), true), true);
            String line;
            String result = "Throughput=";
            String numRecords = "";

            XORShiftRandom random = new XORShiftRandom(42);
            for (int i = 0; i < 10; i++) {
                GZIPInputStream gzipStream = new GZIPInputStream(new FileInputStream(dataFilePath));
                BufferedReader reader = new BufferedReader(new InputStreamReader(gzipStream, "UTF-8"));
                long number = 0;
                long startTime = System.currentTimeMillis();
                HashMap<Integer, Synopsis> stratifiedSynopses = new HashMap<Integer, Synopsis>();
                for (int j = 0; j < 256; j++) {
                    stratifiedSynopses.put(j, new CountMinSketch(65536, 5, 7L));
                }

                System.out.println("iteration: " + i);
                while (reader.ready() && (line = reader.readLine()) != null) {
                    Tuple3<Integer, Integer, Long> tuple = new Tuple3<>(random.nextInt(256 * 1000), random.nextInt(10), System.currentTimeMillis());

                    int key = tuple.f0 / 1000;
                    stratifiedSynopses.get(key).update(tuple.f0);
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
            out.append("\nSource: Uniform, Synopsis: CountMinSketch");
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
}
