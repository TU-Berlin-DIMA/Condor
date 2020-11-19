package de.tub.dima.condor.benchmark;

import de.tub.dima.condor.benchmark.reliablility.*;
import de.tub.dima.condor.benchmark.scalability.processing.bucketing.CountMinBucketing;
import de.tub.dima.condor.benchmark.scalability.processing.bucketing.EquiWidthHistogramBucketing;
import de.tub.dima.condor.benchmark.scalability.processing.bucketing.HaarWaveletsBucketing;
import de.tub.dima.condor.benchmark.scalability.processing.bucketing.ReservoirSamplingBucketing;
import de.tub.dima.condor.benchmark.scalability.processing.streamSlicing.CountMinSlicing;
import de.tub.dima.condor.benchmark.scalability.processing.streamSlicing.EquiWidthHistogramSlicing;
import de.tub.dima.condor.benchmark.scalability.processing.streamSlicing.HaarWaveletsSlicing;
import de.tub.dima.condor.benchmark.scalability.processing.streamSlicing.ReservoirSamplingSlicing;
import org.apache.flink.api.java.utils.ParameterTool;

public class Runner {
    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String outputDir = parameterTool.get("o");
        int parallelism = parameterTool.getInt("p");
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


        // scalability.processing
        // bucketing
        if (parameterTool.has("cmb"))
            CountMinBucketing.run(parallelism, runtime);

        if (parameterTool.has("ewhb"))
            EquiWidthHistogramBucketing.run(parallelism, runtime);

        if (parameterTool.has("hwb"))
            HaarWaveletsBucketing.run(parallelism, runtime);

        if (parameterTool.has("rsb"))
            ReservoirSamplingBucketing.run(parallelism, runtime);

        // streamSlicing
        if (parameterTool.has("cms"))
            CountMinSlicing.run(parallelism, runtime);
        if (parameterTool.has("ewhs"))
            EquiWidthHistogramSlicing.run(parallelism, runtime);
        if (parameterTool.has("hws"))   // TODO: fix
            HaarWaveletsSlicing.run(parallelism, runtime);
        if (parameterTool.has("rss"))
            ReservoirSamplingSlicing.run(parallelism, runtime);

    }
}
