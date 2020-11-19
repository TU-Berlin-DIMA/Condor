package de.tub.dima.condor.flinkScottyConnector.evaluator.utils;

import de.tub.dima.condor.core.synopsis.WindowedSynopsis;
import java.io.Serializable;

/**
 * Generic Query Result Object containing query and Window Information.
 *
 * @param <Q>   Query Type
 * @param <R>   Query Result Type
 */
public class QueryResult<Q extends Serializable, R extends Serializable> implements Serializable {
    private R queryResult;
    private Q query;
    private long windowStart;
    private long windowEnd;

    public QueryResult(R queryResult, Q query, long windowStart, long windowEnd) {
        this.queryResult = queryResult;
        this.query = query;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    public QueryResult(R queryResult, Q query, WindowedSynopsis synopsis) {
        this.queryResult = queryResult;
        this.query = query;
        this.windowStart = synopsis.getWindowStart();
        this.windowEnd = synopsis.getWindowEnd();
    }

    public R getQueryResult() {
        return queryResult;
    }

    public Q getQuery() {
        return query;
    }

    public long getWindowStart() {
        return windowStart;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    @Override
    public String toString() {
        return "QueryResult{" +
                "queryResult=" + queryResult +
                ", query=" + query +
                ", windowStart=" + windowStart +
                ", windowEnd=" + windowEnd +
                '}';
    }
}
