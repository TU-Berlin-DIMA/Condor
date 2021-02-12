package de.tub.dima.condor.benchmark;

import de.tub.dima.condor.benchmark.cost.condor.*;
import de.tub.dima.condor.benchmark.cost.singleCore.*;
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

        // Data-Paths - need to be correct in order for some tests to run
        String uniformTimestampedDataPath = "/Users/joschavonhein/Data/EDADS/data/uniformTimestamped.gz";

        // cost
        // cost condor - arg-prefix 'cc'
        if (parameterTool.has("cchbs"))
            HistBarSplitting.run(parallelism, targetThroughput);
        if (parameterTool.has("cchew"))
            HistEquiWidth.run(parallelism, targetThroughput);
        if (parameterTool.has("cchdd"))
            HistWithDDSketch.run(parallelism, targetThroughput);
        if (parameterTool.has("ccsbr"))
            SamplerBiasedReservoir.run(parallelism, targetThroughput);
        if (parameterTool.has("ccsfifo"))
            SamplerFiFo.run(parallelism, targetThroughput);
        if (parameterTool.has("ccsr"))
            SamplerReservoir.run(parallelism, targetThroughput);
        if (parameterTool.has("ccsbf"))
            SketchBloomFilter.run(parallelism, targetThroughput);
        if (parameterTool.has("ccscm"))
            SketchCountMin.run(parallelism, targetThroughput);
        if (parameterTool.has("ccscf"))
            SketchCuckooFilter.run(parallelism, targetThroughput);
        if (parameterTool.has("ccsdd"))
            SketchDD.run(parallelism, targetThroughput);
        if (parameterTool.has("ccsagm"))
            SketchFastAGM.run(parallelism, targetThroughput);
        if (parameterTool.has("ccshll"))
            SketchHyperLogLog.run(parallelism, targetThroughput);
        if (parameterTool.has("ccstratified"))
            StratifiedCountMin.run(parallelism, targetThroughput);
        if (parameterTool.has("ccwavelet"))
            Wavelet.run(parallelism, targetThroughput);

        // Cost singleCore
        if (parameterTool.has("cshbs"))
            HistBarSplitSC.run(outputDir, uniformTimestampedDataPath);
        if (parameterTool.has("cshew"))
            HistEquiWidthSC.run(outputDir, uniformTimestampedDataPath);
        if (parameterTool.has("cshdd"))
            HistWithDDSC.run(outputDir, uniformTimestampedDataPath);
        if (parameterTool.has("cssbr"))
            SamplerBiasedReservoirSC.run(outputDir, uniformTimestampedDataPath);
        if (parameterTool.has("cssfifo"))
            SamplerFiFoSC.run(outputDir, uniformTimestampedDataPath);
        if (parameterTool.has("cssr"))
            SamplerReservoirSC.run(outputDir, uniformTimestampedDataPath);
        if (parameterTool.has("cssbf"))
            SketchBloomFilterSC.run(outputDir, uniformTimestampedDataPath);
        if (parameterTool.has("csscm"))
            SketchCountMinSC.run(outputDir, uniformTimestampedDataPath);
        if (parameterTool.has("csscf"))
            SketchCuckooFilterSC.run(outputDir, uniformTimestampedDataPath);
        if (parameterTool.has("cssdd"))
            SketchDDSC.run(outputDir, uniformTimestampedDataPath);
        if (parameterTool.has("cssagm"))
            SketchFastAGMSC.run(outputDir, uniformTimestampedDataPath);
        if (parameterTool.has("csshll"))
            SketchHLLSC.run(outputDir, uniformTimestampedDataPath);
        if (parameterTool.has("csstratified"))
            StratifiedCountMinSingleCore.run(outputDir, uniformTimestampedDataPath);
        if (parameterTool.has("cswavelet"))
            WaveletSingleCore.run(outputDir, uniformTimestampedDataPath);

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
