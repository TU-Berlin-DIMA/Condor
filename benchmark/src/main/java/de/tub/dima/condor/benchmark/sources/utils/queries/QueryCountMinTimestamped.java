package de.tub.dima.condor.benchmark.sources.utils.queries;

import de.tub.dima.condor.core.synopsis.Sketches.CountMinSketch;
import de.tub.dima.condor.core.synopsis.WindowedSynopsis;
import de.tub.dima.condor.flinkScottyConnector.evaluator.utils.QueryFunction;
import de.tub.dima.condor.flinkScottyConnector.evaluator.utils.QueryResult;
import de.tub.dima.condor.flinkScottyConnector.evaluator.utils.TimestampedQuery;

public class QueryCountMinTimestamped implements QueryFunction<TimestampedQuery<Integer>, WindowedSynopsis<CountMinSketch>, QueryResult<TimestampedQuery<Integer>, Integer>>{
    @Override
    public QueryResult<TimestampedQuery<Integer>, Integer> query(TimestampedQuery<Integer> query, WindowedSynopsis<CountMinSketch> synopsis) {
        return new QueryResult<>(synopsis.getSynopsis().query(query.getQuery()), query, synopsis);
    }
}
