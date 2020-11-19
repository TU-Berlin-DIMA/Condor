package de.tub.dima.condor.benchmark;

import de.tub.dima.condor.benchmark.reliablility.CountMinAccuracy;
import de.tub.dima.condor.benchmark.reliablility.EquiWidthHistogramAccuracy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Runner {
    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        int parallelism = parameterTool.getInt("p");

        if (parameterTool.has("cma")){
            CountMinAccuracy.run(parallelism);
        }
        if (parameterTool.has("ewha")){
            EquiWidthHistogramAccuracy.run(parallelism);
        }
    }
}
