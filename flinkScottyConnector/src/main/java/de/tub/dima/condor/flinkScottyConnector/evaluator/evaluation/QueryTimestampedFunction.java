package de.tub.dima.condor.flinkScottyConnector.evaluator.evaluation;

import de.tub.dima.condor.core.synopsis.Synopsis;
import de.tub.dima.condor.core.synopsis.WindowedSynopsis;
import de.tub.dima.condor.flinkScottyConnector.evaluator.utils.QueryFunction;
import de.tub.dima.condor.flinkScottyConnector.evaluator.utils.QueryResult;
import de.tub.dima.condor.flinkScottyConnector.evaluator.utils.TimestampedQuery;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.TreeSet;

/**
 * Function used by the ApproximateDataAnalytics Class to query the synopsisStream based on a specific timestamp.
 * The user can specify how many synopsis to 'store' in memory.
 *
 * @param <Q>   Query Type
 * @param <S>   Synopsis Type
 * @param <O>   Query Result Type
 *
 * @author Joscha von Hein
 */
public class QueryTimestampedFunction<Q extends Serializable, S extends Synopsis, O extends Serializable> extends
        BroadcastProcessFunction<TimestampedQuery<Q>, WindowedSynopsis<S>, QueryResult<TimestampedQuery<Q>, O>> {

    final int maxSynopsisCount;
    final QueryFunction<Q, S, O> queryFunction;
    final MapStateDescriptor<Boolean, TreeSet<WindowedSynopsis<S>>> synopsisMapStateDescriptor = new MapStateDescriptor<Boolean, TreeSet<WindowedSynopsis<S>>>(
            "SynopsisArchive",
            BasicTypeInfo.BOOLEAN_TYPE_INFO,
            TypeInformation.of(new TypeHint<TreeSet<WindowedSynopsis<S>>>() {
            }));

    ArrayList<TimestampedQuery<Q>> queryList = new ArrayList<TimestampedQuery<Q>>();

    public QueryTimestampedFunction(QueryFunction<Q, S, O> queryFunction, int maxSynopsisCount) {
        this.queryFunction = queryFunction;
        this.maxSynopsisCount = maxSynopsisCount;
    }


    @Override
    public void processElement(TimestampedQuery<Q> value, ReadOnlyContext ctx, Collector<QueryResult<TimestampedQuery<Q>, O>> out) throws Exception {

        if (ctx.getBroadcastState(synopsisMapStateDescriptor).contains(true)){
            WindowedSynopsis<S> querySynopsis = ctx.getBroadcastState(synopsisMapStateDescriptor).get(true)
                    .floor(new WindowedSynopsis<S>(null, value.getTimeStamp(), Long.MAX_VALUE));

            if (querySynopsis != null && querySynopsis.getWindowEnd() >= value.getTimeStamp()){ // synopsis with correct window exists

                final O result = queryFunction.query(value.getQuery(), querySynopsis.getSynopsis());
                QueryResult<TimestampedQuery<Q>, O> queryResult= new QueryResult<TimestampedQuery<Q>, O>(result, value, querySynopsis);
                out.collect(queryResult);
            }

        } else {
            queryList.add(value);
        }
    }

    /**
     * Processes the incoming Synopses by storing them in the BroadcastState
     */
    @Override
    public void processBroadcastElement(WindowedSynopsis<S> value, Context ctx, Collector<QueryResult<TimestampedQuery<Q>, O>> out) throws Exception {
        TreeSet<WindowedSynopsis<S>> windowedSynopses;
        if (ctx.getBroadcastState(synopsisMapStateDescriptor).contains(true)){
            windowedSynopses = ctx.getBroadcastState(synopsisMapStateDescriptor).get(true);
            if (windowedSynopses.size() >= maxSynopsisCount){
                windowedSynopses.pollFirst();
            }
            windowedSynopses.add(value);
            ctx.getBroadcastState(synopsisMapStateDescriptor).put(true, windowedSynopses);
        } else {
            windowedSynopses = new TreeSet<WindowedSynopsis<S>>(new Comparator<WindowedSynopsis<S>>() {
                @Override
                public int compare(WindowedSynopsis<S> o1, WindowedSynopsis<S> o2) {
                    return Long.compare(o1.getWindowStart(), o2.getWindowStart());
                }
            });
            windowedSynopses.add(value);
            ctx.getBroadcastState(synopsisMapStateDescriptor).put(true, windowedSynopses);
            queryList.stream().filter(query -> query.getTimeStamp() >= value.getWindowStart() && query.getTimeStamp() <= value.getWindowEnd())
                    .forEach(query -> {
                        O result = queryFunction.query(query.getQuery(), value.getSynopsis());
                        out.collect(new QueryResult<TimestampedQuery<Q>, O>(result, query, value));
                    });
        }
    }
}