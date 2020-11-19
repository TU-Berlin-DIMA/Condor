package de.tub.dima.condor.benchmark;

import de.tub.dima.condor.benchmark.reliablility.*;
import org.apache.flink.api.java.utils.ParameterTool;

public class Runner {
    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String outputDir = parameterTool.get("o");
        int parallelism = parameterTool.getInt("p");

        if (parameterTool.has("cma")){
            CountMinAccuracy.run(parallelism, outputDir);
        }
        if (parameterTool.has("ewha")){
            EquiWidthHistogramAccuracy.run(parallelism, outputDir);
        }
        if (parameterTool.has("hwa")){
            HaarWaveletsAccuracy.run(parallelism, outputDir);
        }
        if (parameterTool.has("hlla")){
            HLLSketchAccuracy.run(parallelism, outputDir);
        }
        if (parameterTool.has("rsa")){
            ReservoirSamplingAccuracy.run(parallelism, outputDir);
        }
    }
}
