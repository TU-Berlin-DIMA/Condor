package de.tub.dima.condor.benchmark.sources.utils;

import de.tub.dima.condor.core.synopsis.Sketches.CountMinSketch;
import de.tub.dima.condor.core.synopsis.WindowedSynopsis;
import de.tub.dima.condor.flinkScottyConnector.evaluator.utils.QueryFunction;
import de.tub.dima.condor.flinkScottyConnector.evaluator.utils.StratifiedQueryResult;
import de.tub.dima.condor.flinkScottyConnector.evaluator.utils.TimestampedQuery;
import org.apache.flink.api.java.tuple.Tuple2;

public class QueryCountMinTimestampedStratified implements QueryFunction<Tuple2<Integer, TimestampedQuery<Integer>>, WindowedSynopsis<CountMinSketch>, StratifiedQueryResult<TimestampedQuery<Integer>, Integer, Integer>> {
    @Override
    public StratifiedQueryResult<TimestampedQuery<Integer>, Integer, Integer> query(Tuple2<Integer, TimestampedQuery<Integer>> query, WindowedSynopsis<CountMinSketch> synopsis) {
        Integer result = synopsis.getSynopsis().query(query.f1.getQuery());
        return new StratifiedQueryResult<TimestampedQuery<Integer>, Integer, Integer>(result, query, synopsis);
    }
}
