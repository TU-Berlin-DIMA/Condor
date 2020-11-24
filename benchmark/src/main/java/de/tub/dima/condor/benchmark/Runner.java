package de.tub.dima.condor.benchmark;

import de.tub.dima.condor.benchmark.cost.condor.Mergeable;
import de.tub.dima.condor.benchmark.cost.condor.OrderBased;
import de.tub.dima.condor.benchmark.cost.condor.Stratified;
import de.tub.dima.condor.benchmark.cost.singleCore.MergeableSingleCore;
import de.tub.dima.condor.benchmark.cost.singleCore.OrderBasedSingleCore;
import de.tub.dima.condor.benchmark.cost.singleCore.StratifiedSingleCore;
import de.tub.dima.condor.benchmark.efficiency.streamApprox.Parallelism;
import de.tub.dima.condor.benchmark.efficiency.streamApprox.SampleSize;
import de.tub.dima.condor.benchmark.efficiency.streamApprox.StratifiedSampling;
import de.tub.dima.condor.benchmark.efficiency.yahoo.*;
import de.tub.dima.condor.benchmark.reliablility.*;
import de.tub.dima.condor.benchmark.scalability.dataSources.global.NYCTaxiGlobal;
import de.tub.dima.condor.benchmark.scalability.dataSources.global.UniformGlobal;
import de.tub.dima.condor.benchmark.scalability.dataSources.global.ZipfGlobal;
import de.tub.dima.condor.benchmark.scalability.dataSources.stratified.NYCTaxiStratified;
import de.tub.dima.condor.benchmark.scalability.dataSources.stratified.UniformStratified;
import de.tub.dima.condor.benchmark.scalability.dataSources.stratified.ZipfStratified;
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
import de.tub.dima.condor.benchmark.windowing.Bucketing;
import de.tub.dima.condor.benchmark.windowing.GeneralStreamSlicing;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * @author Joscha von Hein
 */
public class Runner {
    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String outputDir = parameterTool.get("o");
        int parallelism = parameterTool.getInt("p");
        int targetThroughput = parameterTool.getInt("t", -1);
        int runtime = parameterTool.getInt("runtime", 60000); // runtime in ms - default 1 min
        int sampleSize = parameterTool.getInt("sampleSize", 1000);
        int stratification = parameterTool.getInt("stratification", parallelism);
        int nConcurrentWindows = parameterTool.getInt("windows", 100);

        // cost
        // cost condor
        if (parameterTool.has("ccm"))
            Mergeable.run(parallelism, targetThroughput);
        if (parameterTool.has("cco"))
            OrderBased.run(parallelism, targetThroughput);
        if (parameterTool.has("ccs"))
            Stratified.run(parallelism, targetThroughput);

        // Cost singleCore
        if (parameterTool.has("cscm"))
            MergeableSingleCore.run(outputDir);
        if (parameterTool.has("csco"))
            OrderBasedSingleCore.run(outputDir);
        if (parameterTool.has("cscs"))
            StratifiedSingleCore.run(outputDir);

        // efficiency
        // efficiency streamApprox
        if (parameterTool.has("esap"))
            Parallelism.run(parallelism, runtime, targetThroughput);
        if (parameterTool.has("esas"))
            SampleSize.run(parallelism, runtime, targetThroughput, sampleSize);
        if (parameterTool.has("esass"))
            StratifiedSampling.run(parallelism, runtime, targetThroughput, stratification);

        // efficiency yahoo
        if (parameterTool.has("hllc"))
            CondorHLL.run(parallelism, runtime, targetThroughput);
        if (parameterTool.has("hllcs"))
            CondorHLLStratified.run(parallelism, runtime, targetThroughput);
        if (parameterTool.has("hlly"))
            YahooHLLOnCondor.run(parallelism, runtime, targetThroughput);
        if (parameterTool.has("hllys"))
            YahooHLLOnCondorStratified.run(parallelism, runtime, targetThroughput);

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
        // scalability dataSources
        // scalability dataSources global
        if (parameterTool.has("nycg"))
            NYCTaxiGlobal.run(parallelism, runtime, targetThroughput);
        if (parameterTool.has("unig"))
            UniformGlobal.run(parallelism, runtime, targetThroughput);
        if (parameterTool.has("zipfg"))
            ZipfGlobal.run(parallelism, runtime, targetThroughput);

        // scalability dataSources stratified
        if (parameterTool.has("nycs"))
            NYCTaxiStratified.run(parallelism, runtime, targetThroughput);
        if (parameterTool.has("unis"))
            UniformStratified.run(parallelism, runtime, targetThroughput);
        if (parameterTool.has("zipfs"))
            ZipfStratified.run(parallelism, runtime, targetThroughput);

        // scalability evaluation
        if (parameterTool.has("ql"))
            QueryLatest.run(parallelism, targetThroughput);
        if (parameterTool.has("qls"))
            QueryLatestStratified.run(parallelism, targetThroughput);
        if (parameterTool.has("qt"))
            QueryTimestamped.run(parallelism, targetThroughput);
        if (parameterTool.has("qts"))
            QueryTimestampedStratified.run(parallelism, targetThroughput);

        // scalability processing
        // scalability processing bucketing
        if (parameterTool.has("cmb"))
            CountMinBucketing.run(parallelism, runtime,targetThroughput);
        if (parameterTool.has("ewhb"))
            EquiWidthHistogramBucketing.run(parallelism, runtime, targetThroughput);
        if (parameterTool.has("hwb"))
            HaarWaveletsBucketing.run(parallelism, runtime, targetThroughput);
        if (parameterTool.has("rsb"))
            ReservoirSamplingBucketing.run(parallelism, runtime, targetThroughput);

        //  scalability processing generalSlicing
        if (parameterTool.has("cms"))
            CountMinSlicing.run(parallelism, runtime, targetThroughput);
        if (parameterTool.has("ewhs"))
            EquiWidthHistogramSlicing.run(parallelism, runtime, targetThroughput);
        if (parameterTool.has("hws"))
            HaarWaveletsSlicing.run(parallelism, runtime, targetThroughput);
        if (parameterTool.has("rss"))
            ReservoirSamplingSlicing.run(parallelism, runtime, targetThroughput);

        // windowing
        if (parameterTool.has("wb"))
            Bucketing.run(parallelism, targetThroughput, nConcurrentWindows);
        if (parameterTool.has("wgss"))
            GeneralStreamSlicing.run(parallelism, targetThroughput, nConcurrentWindows);
    }
}
