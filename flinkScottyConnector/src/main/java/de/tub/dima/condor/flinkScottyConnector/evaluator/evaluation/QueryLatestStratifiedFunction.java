package de.tub.dima.condor.flinkScottyConnector.evaluator.evaluation;

import de.tub.dima.condor.core.synopsis.StratifiedSynopsisWrapper;
import de.tub.dima.condor.core.synopsis.Synopsis;
import de.tub.dima.condor.core.synopsis.WindowedSynopsis;
import de.tub.dima.condor.flinkScottyConnector.evaluator.utils.QueryFunction;
import de.tub.dima.condor.flinkScottyConnector.evaluator.utils.StratifiedQueryResult;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.Consumer;

/**
 * Function used by the ApproximateDataAnalytics Class to query the latest stratified synopsis stream.
 *
 * @param <P>   Strata Type (the element type by which the synopis stream is keyed on)
 * @param <Q>   Query Type
 * @param <S>   Synopsis Type
 * @param <O>   Query Result Type
 * @author Joscha von Hein
 */
public class QueryLatestStratifiedFunction<P extends Serializable, Q extends Serializable, S extends Synopsis, O extends Serializable> extends KeyedBroadcastProcessFunction<P, Tuple2<P,Q>, StratifiedSynopsisWrapper<P, WindowedSynopsis<S>>, StratifiedQueryResult<Q, O, P>> {

    HashMap<P, ArrayList<Tuple2<P,Q>>> queryList;
    QueryFunction<Q, S, O> queryFunction;
    Class<P> partitionClass;

    private final MapStateDescriptor<P, WindowedSynopsis<S>> synopsisMapStateDescriptor;


    public QueryLatestStratifiedFunction(QueryFunction<Q, S, O> queryFunction, Class<P> partitionClass, MapStateDescriptor<P, WindowedSynopsis<S>> mapStateDescriptor) {
        this.queryFunction = queryFunction;
        queryList = new HashMap<>();
        this.partitionClass = partitionClass;
        synopsisMapStateDescriptor = mapStateDescriptor;
    }


    @Override
    public void processElement(Tuple2<P,Q> query, ReadOnlyContext ctx, Collector<StratifiedQueryResult<Q, O, P>> out) throws Exception {
        ReadOnlyBroadcastState<P, WindowedSynopsis<S>> broadcastState = ctx.getBroadcastState(synopsisMapStateDescriptor);

        // using query.f0 very strangely did not work, using context here was the best workaround i found
        P key = ctx.getCurrentKey();


        if (broadcastState.contains(key)){
            WindowedSynopsis<S> windowedSynopsis = broadcastState.get(key);
            O queryResult = queryFunction.query(query.f1, windowedSynopsis.getSynopsis());
            out.collect(new StratifiedQueryResult<Q, O, P>(queryResult, query, windowedSynopsis));
        } else if (queryList.containsKey(key)){
            queryList.get(key).add(query);
        } else {
            ArrayList<Tuple2<P,Q>> queries = new ArrayList<>();

            queries.add(query);
            queryList.put(query.f0, queries);
        }
    }

    @Override
    public void processBroadcastElement(StratifiedSynopsisWrapper<P, WindowedSynopsis<S>> stratifiedSynopsisWrapper, Context ctx, Collector<StratifiedQueryResult<Q, O, P>> out) throws Exception {

        final P key = stratifiedSynopsisWrapper.getKey();
        final WindowedSynopsis<S> windowedSynopsis = stratifiedSynopsisWrapper.getSynopsis();

        if (!ctx.getBroadcastState(synopsisMapStateDescriptor).contains(key) &&
                queryList.containsKey(key)) {

            queryList.get(key).forEach(new Consumer<Tuple2<P,Q>>() {
                @Override
                public void accept(Tuple2<P,Q> query) {
                    O queryResult = queryFunction.query(query.f1, windowedSynopsis.getSynopsis());
                    out.collect(new StratifiedQueryResult<Q, O, P>(queryResult, query, windowedSynopsis));
                }
            });
            queryList.remove(key);
        }
        ctx.getBroadcastState(synopsisMapStateDescriptor).put(key, windowedSynopsis);
    }
}
