package de.tub.dima.condor.benchmark.cost.singleCore;


import de.tub.dima.condor.core.synopsis.Sketches.CountMinSketch;

import java.io.*;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

/*java -jar target/benchmark-0.1.jar ...parameters*/

public class MergeableSingleCore {
    public static void run(String outputDir) throws Exception {
        // Please set up the correct path to the uniformTimestamped.gz
        String dataFilePath = "<PATH-TO>/uniformTimestamped.gz";

        try {

            PrintWriter out = new PrintWriter(new FileOutputStream(new File(outputDir + "/single-core.txt"), true), true);
            String line;
            String result = "Throughput=";
            String numRecords = "";
            CountMinSketch synopsis = new CountMinSketch(65536, 5, 7L);

            for (int i = 0; i < 10; i++) {
                GZIPInputStream gzipStream = new GZIPInputStream(new FileInputStream(dataFilePath));
                BufferedReader reader = new BufferedReader(new InputStreamReader(gzipStream, "UTF-8"));
                long number = 0;
                long startTime = System.currentTimeMillis();
                System.out.println("iteration: " + i);
                while (reader.ready() && (line = reader.readLine()) != null) {

                    String[] tokens = line.split(",");

                    synopsis.update(tokens[0]);


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
            out.append("\nSource: Uniform, Mergeable Synopsis: ");
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
