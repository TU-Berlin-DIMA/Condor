package de.tub.dima.condor.core.ApproximateDataAnalytics;

import de.tub.dima.condor.core.Synopsis.WindowedSynopsis;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;

/**
 * Query Result Object in case of Stratified Synopsis Streams.
 * This Class behaves exactly as the normal QueryResult but also contains the Partition / Key of the Query.
 *
 * @param <Q>   Query Type
 * @param <O>   Query Result Type
 * @param <P>   Partition / Key Type
 */
public class StratifiedQueryResult<Q extends Serializable, O extends Serializable, P extends Serializable> implements Serializable {
    private O queryResult;
    private Q query;
    private P partition;
    private long windowStart;
    private long windowEnd;

    public StratifiedQueryResult(O queryResult, Q query, P partition, long windowStart, long windowEnd) {
        this.queryResult = queryResult;
        this.query = query;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.partition = partition;
    }

    public StratifiedQueryResult(O queryResult, Tuple2<P,Q> query, WindowedSynopsis synopsis) {
        this.queryResult = queryResult;
        this.query = query.f1;
        this.partition = query.f0;
        this.windowStart = synopsis.getWindowStart();
        this.windowEnd = synopsis.getWindowEnd();
    }

    public O getQueryResult() {
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

    public P getPartition() {
        return partition;
    }

    @Override
    public String toString() {
        return "StratifiedQueryResult{" +
                "queryResult=" + queryResult +
                ", query=" + query +
                ", partition=" + partition +
                ", windowStart=" + windowStart +
                ", windowEnd=" + windowEnd +
                '}';
    }
}
