package de.tub.dima.condor.core.ApproximateDataAnalytics;

import de.tub.dima.condor.core.Synopsis.Synopsis;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.function.Consumer;

import de.tub.dima.condor.core.Synopsis.WindowedSynopsis;

import javax.jws.Oneway;

/**
 * Function used by the ApproximateDataAnalytics Class to query the latest Synopsis seen in the SynopsisStream.
 *
 * @param <Q>   Query Type
 * @param <S>   Synopsis Type
 * @param <O>   Query Output Type
 * @author Joscha von Hein
 */
public class QueryLatestFunction<Q extends Serializable, S extends Synopsis, O extends Serializable> extends BroadcastProcessFunction<Q, WindowedSynopsis<S>, QueryResult<Q, O>> {

    private final MapStateDescriptor<Boolean, WindowedSynopsis<S>> synopsisMapStateDescriptor = new MapStateDescriptor<Boolean, WindowedSynopsis<S>>(
            "latestSynopsis",
            BasicTypeInfo.BOOLEAN_TYPE_INFO,
            TypeInformation.of(new TypeHint<WindowedSynopsis<S>>() {
            }));

    ArrayList<Q> queryList = new ArrayList<>();
    QueryFunction<Q, S, O> queryFunction;

    public QueryLatestFunction(QueryFunction<Q, S, O> queryFunction) {
        this.queryFunction = queryFunction;
    }

    @Override
    public void processElement(Q query, ReadOnlyContext ctx, Collector<QueryResult<Q, O>> out) throws Exception {
        ReadOnlyBroadcastState<Boolean, WindowedSynopsis<S>> broadcastState = ctx.getBroadcastState(synopsisMapStateDescriptor);

        if (broadcastState.contains(true)) {
            O result = queryFunction.query(query, broadcastState.get(true).getSynopsis());
            out.collect(new QueryResult<Q, O>(result, query, broadcastState.get(true)));
        } else {
            queryList.add(query);
        }
    }

    @Override
    public void processBroadcastElement(WindowedSynopsis<S> synopsis, Context ctx, Collector<QueryResult<Q, O>> out) throws Exception {
        if (!ctx.getBroadcastState(synopsisMapStateDescriptor).contains(true) && queryList.size() > 0) {
            queryList.forEach(query -> out.collect(new QueryResult<Q, O>(queryFunction.query(query, synopsis.getSynopsis()), query, synopsis)));
            queryList.clear();
        }
        ctx.getBroadcastState(synopsisMapStateDescriptor).put(true, synopsis);
    }
}
