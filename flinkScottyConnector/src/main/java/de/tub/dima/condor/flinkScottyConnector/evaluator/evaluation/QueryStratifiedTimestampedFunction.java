package de.tub.dima.condor.flinkScottyConnector.evaluator.evaluation;

import de.tub.dima.condor.core.synopsis.Synopsis;
import de.tub.dima.condor.core.synopsis.StratifiedSynopsisWrapper;
import de.tub.dima.condor.core.synopsis.WindowedSynopsis;
import de.tub.dima.condor.flinkScottyConnector.evaluator.utils.QueryFunction;
import de.tub.dima.condor.flinkScottyConnector.evaluator.utils.StratifiedQueryResult;
import de.tub.dima.condor.flinkScottyConnector.evaluator.utils.TimestampedQuery;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import java.io.Serializable;
import java.util.*;

/**
 * KeyedBroadcastFunction which combines a keyed stream of queries with a Stream of Synopsis which are based on the same keys.
 * The queries are timestamped and if a synopsis with the correct key and window time exists will be answered, otherwise dismissed.
 *
 * @author Joscha von Hein
 * @param <P>   Partition / Key Type
 * @param <Q>   Query Type
 * @param <S>   Synopsis Type
 * @param <O>   Query Result Type
 */
public class QueryStratifiedTimestampedFunction<P extends Serializable, Q extends Serializable, S extends Synopsis, O extends Serializable> extends KeyedBroadcastProcessFunction<P, Tuple2<P, TimestampedQuery<Q>>, StratifiedSynopsisWrapper<P, WindowedSynopsis<S>>, StratifiedQueryResult<TimestampedQuery<Q>, O, P>> {

    final int maxSynopsisCount; // maximum synopsis count per strata / key
    final QueryFunction<Tuple2<P, TimestampedQuery<Q>>, WindowedSynopsis<S>, StratifiedQueryResult<TimestampedQuery<Q>, O, P>> queryFunction;
    final MapStateDescriptor<P, TreeSet<WindowedSynopsis<S>>> synopsisMapStateDescriptor;
    HashMap<P, LinkedList<Tuple2<P, TimestampedQuery<Q>>>> queryHashMap = new HashMap<P, LinkedList<Tuple2<P, TimestampedQuery<Q>>>>();

    public QueryStratifiedTimestampedFunction(int maxSynopsisCount,
                                              QueryFunction<Tuple2<P, TimestampedQuery<Q>>, WindowedSynopsis<S>, StratifiedQueryResult<TimestampedQuery<Q>, O, P>> queryFunction,
                                              MapStateDescriptor<P, TreeSet<WindowedSynopsis<S>>> synopsisMapStateDescriptor) {
        this.maxSynopsisCount = maxSynopsisCount;
        this.queryFunction = queryFunction;
        this.synopsisMapStateDescriptor = synopsisMapStateDescriptor;
    }

    @Override
    public void processElement(Tuple2<P, TimestampedQuery<Q>> value, ReadOnlyContext ctx, Collector<StratifiedQueryResult<TimestampedQuery<Q>, O, P>> out) throws Exception {
        P key = ctx.getCurrentKey();
        if (ctx.getBroadcastState(synopsisMapStateDescriptor).contains(key)){

            WindowedSynopsis<S> querySynopsis = ctx.getBroadcastState(synopsisMapStateDescriptor).get(key)
                    .floor(new WindowedSynopsis<S>(null, value.f1.getTimeStamp(), Long.MAX_VALUE));

            if (querySynopsis != null && querySynopsis.getWindowEnd() >= value.f1.getTimeStamp()){ // synopsis with correct window exists

                out.collect(queryFunction.query(value, querySynopsis));
            }
        }else {
            // store the query
            LinkedList<Tuple2<P, TimestampedQuery<Q>>> map;
            if (queryHashMap.containsKey(key)){
                map = queryHashMap.get(key);
            } else {
                map = new LinkedList<Tuple2<P, TimestampedQuery<Q>>>();
            }
            map.add(value);
            queryHashMap.put(key, map);
        }
    }

    @Override
    public void processBroadcastElement(StratifiedSynopsisWrapper<P, WindowedSynopsis<S>> value, Context ctx, Collector<StratifiedQueryResult<TimestampedQuery<Q>, O, P>> out) throws Exception {
        TreeSet<WindowedSynopsis<S>> windowedSynopses;
        P key = value.getKey();
        WindowedSynopsis synopsis = value.getSynopsis();
        if (ctx.getBroadcastState(synopsisMapStateDescriptor).contains(key)){
            windowedSynopses = ctx.getBroadcastState(synopsisMapStateDescriptor).get(key);
            if (windowedSynopses.size() >= maxSynopsisCount){
                windowedSynopses.pollFirst();
            }
            windowedSynopses.add(synopsis);
            ctx.getBroadcastState(synopsisMapStateDescriptor).put(key, windowedSynopses);
        } else {
            windowedSynopses = new TreeSet<WindowedSynopsis<S>>(Comparator.comparing(WindowedSynopsis::getWindowStart));

            windowedSynopses.add(synopsis);
            ctx.getBroadcastState(synopsisMapStateDescriptor).put(key, windowedSynopses);
            if (queryHashMap.containsKey(key)){
                queryHashMap.get(key).stream()
                        .filter(query -> query.f1.getTimeStamp() >= synopsis.getWindowStart() && query.f1.getTimeStamp() <= synopsis.getWindowEnd())
                        .forEach(query -> out.collect(queryFunction.query(query, synopsis)));
            }
        }
    }
}
