package de.tub.dima.condor.benchmark.sources.utils.queries;

import de.tub.dima.condor.core.synopsis.Sketches.CountMinSketch;
import de.tub.dima.condor.core.synopsis.WindowedSynopsis;
import de.tub.dima.condor.flinkScottyConnector.evaluator.utils.QueryFunction;

public class QueryCountMin implements QueryFunction<Integer, CountMinSketch, Integer> {
    @Override
    public Integer query(Integer query, CountMinSketch synopsis) {
        return synopsis.query(query);
    }
}
