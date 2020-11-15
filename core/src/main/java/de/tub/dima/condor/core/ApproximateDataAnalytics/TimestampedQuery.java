package de.tub.dima.condor.core.ApproximateDataAnalytics;

import java.io.Serializable;

/**
 * TimestampedQuery Class which is used by the ApproximateDataAnalytics Class in queries which pertain to a specific time.
 *
 * @param <Q>   Query Type
 */
public class TimestampedQuery<Q extends Serializable> implements Serializable{
    private final Q query;
    private final long timeStamp;

    public TimestampedQuery(Q query, long timeStamp) {
        this.query = query;
        this.timeStamp = timeStamp;
    }

    public Q getQuery() {
        return query;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public String toString(){
        String string = "timebasedQuery { query = " + query + " , timestamp = " + timeStamp + "} ";
        return string;
    }
}
