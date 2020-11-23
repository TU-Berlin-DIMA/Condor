package de.tub.dima.condor.benchmark;

import de.tub.dima.condor.benchmark.reliablility.*;
import de.tub.dima.condor.benchmark.scalability.evaluation.QueryLatest;
import de.tub.dima.condor.benchmark.scalability.evaluation.QueryLatestStratified;
import de.tub.dima.condor.benchmark.scalability.evaluation.QueryTimestamped;
import de.tub.dima.condor.benchmark.scalability.evaluation.QueryTimestampedStratified;
import de.tub.dima.condor.benchmark.scalability.processing.bucketing.CountMinBucketing;
import de.tub.dima.condor.benchmark.scalability.processing.bucketing.EquiWidthHistogramBucketing;
import de.tub.dima.condor.benchmark.scalability.processing.bucketing.HaarWaveletsBucketing;
import de.tub.dima.condor.benchmark.scalability.processing.bucketing.ReservoirSamplingBucketing;
import de.tub.dima.condor.benchmark.scalability.processing.generalSlicing.CountMinSlicing;
import de.tub.dima.condor.benchmark.scalability.processing.generalSlicing.EquiWidthHistogramSlicing;
import de.tub.dima.condor.benchmark.scalability.processing.generalSlicing.HaarWaveletsSlicing;
import de.tub.dima.condor.benchmark.scalability.processing.generalSlicing.ReservoirSamplingSlicing;
import org.apache.flink.api.java.utils.ParameterTool;

public class Runner {
    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String outputDir = parameterTool.get("o");
        int parallelism = parameterTool.getInt("p");
        int targetThroughput = parameterTool.getInt("t", -1);
        int runtime = parameterTool.getInt("runtime", 60000); // runtime in ms - default 1 min

        // reliability
        if (parameterTool.has("cma"))
            CountMinAccuracy.run(parallelism, outputDir);

        if (parameterTool.has("ewha"))
            EquiWidthHistogramAccuracy.run(parallelism, outputDir);

        if (parameterTool.has("hwa"))
            HaarWaveletsAccuracy.run(parallelism, outputDir);

        if (parameterTool.has("hlla"))
            HLLSketchAccuracy.run(parallelism, outputDir);

        if (parameterTool.has("rsa"))
            ReservoirSamplingAccuracy.run(parallelism, outputDir);


        // scalability
        //   processing
        //     bucketing
        if (parameterTool.has("cmb"))
            CountMinBucketing.run(parallelism, runtime,targetThroughput);

        if (parameterTool.has("ewhb"))
            EquiWidthHistogramBucketing.run(parallelism, runtime, targetThroughput);

        if (parameterTool.has("hwb"))
            HaarWaveletsBucketing.run(parallelism, runtime, targetThroughput);

        if (parameterTool.has("rsb"))
            ReservoirSamplingBucketing.run(parallelism, runtime, targetThroughput);

        //     streamSlicing
        if (parameterTool.has("cms"))
            CountMinSlicing.run(parallelism, runtime, targetThroughput);
        if (parameterTool.has("ewhs"))
            EquiWidthHistogramSlicing.run(parallelism, runtime, targetThroughput);
        if (parameterTool.has("hws"))
            HaarWaveletsSlicing.run(parallelism, runtime, targetThroughput);
        if (parameterTool.has("rss"))
            ReservoirSamplingSlicing.run(parallelism, runtime, targetThroughput);


        //   evaluation
        int queryThroughput = parameterTool.getInt("queryThroughput", 10000);
        if (parameterTool.has("ql"))
            QueryLatest.run(parallelism, runtime, queryThroughput);
        if (parameterTool.has("qls"))
            QueryLatestStratified.run(parallelism, runtime, queryThroughput);
        if (parameterTool.has("qt"))
            QueryTimestamped.run(parallelism, runtime, queryThroughput);
        if (parameterTool.has("qts"))
            QueryTimestampedStratified.run(parallelism, runtime, queryThroughput);

    }
}
