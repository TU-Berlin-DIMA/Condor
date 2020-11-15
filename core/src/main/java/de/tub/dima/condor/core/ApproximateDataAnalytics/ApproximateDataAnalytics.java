package de.tub.dima.condor.core.ApproximateDataAnalytics;

import de.tub.dima.condor.core.Synopsis.Synopsis;
import de.tub.dima.condor.core.Synopsis.StratifiedSynopsis;
import de.tub.dima.condor.core.Synopsis.StratifiedSynopsisWrapper;
import de.tub.dima.condor.core.Synopsis.WindowedSynopsis;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;

import java.io.Serializable;
import java.util.TreeSet;

/**
 * Class which enables querying a stream of Synopsis easily for the user.
 * It takes both the synopsis stream and the query stream as inputs, as well as a synopsis specific query function
 * and uses a BroadCastProcessFunction to 'join' the two.
 * All Methods will produce a Stream of QueryResults.
 *
 * @author Joscha von Hein
 * @author Rudi Poepsel Lemaitre
 */
public final class ApproximateDataAnalytics {

    public static <Q extends Serializable, S extends Synopsis, O extends Serializable> SingleOutputStreamOperator<QueryResult<Q, O>> queryLatest
            (DataStream<WindowedSynopsis<S>> synopsesStream, DataStream<Q> queryStream, QueryFunction<Q, S, O> queryFunction) {

        MapStateDescriptor<Boolean, WindowedSynopsis<S>> synopsisMapStateDescriptor = new MapStateDescriptor<Boolean, WindowedSynopsis<S>>(
                "latestSynopsis",
                BasicTypeInfo.BOOLEAN_TYPE_INFO,
                TypeInformation.of(new TypeHint<WindowedSynopsis<S>>() {
                }));

        BroadcastStream<WindowedSynopsis<S>> broadcast = synopsesStream.broadcast(synopsisMapStateDescriptor);
        return queryStream.connect(broadcast)
                .process(new QueryLatestFunction<Q, S, O>(queryFunction));
    }

    /**
     * This applies the timestamped queries in the queryStream to the the corresponding synopsis (if it exists).
     *
     * @param synopsesStream    DataStream which contains the timestamped synopsis
     * @param queryStream       DataStream which contains query elements (each element corresponds to a different timestamped query)
     * @param queryFunction     User code which defines how the Synopsis is queried. Takes the Synopsis and Query as Input and Generates O as output
     * @param <Q>   Query Element Type
     * @param <S>   Synopsis Type
     * @param <O>   Query Output
     * @return
     */
    public static <Q extends Serializable, S extends Synopsis, O extends Serializable> SingleOutputStreamOperator<QueryResult<TimestampedQuery<Q>, O>> queryTimestamped
        (DataStream<WindowedSynopsis<S>> synopsesStream, DataStream<TimestampedQuery<Q>> queryStream, QueryFunction<TimestampedQuery<Q>, WindowedSynopsis<S>, QueryResult<TimestampedQuery<Q>, O>> queryFunction, int maxSynopsisCount) {

        MapStateDescriptor<Boolean, TreeSet<WindowedSynopsis<S>>> synopsisMapStateDescriptor = new MapStateDescriptor<Boolean, TreeSet<WindowedSynopsis<S>>>(
                "SynopsisArchive",
                BasicTypeInfo.BOOLEAN_TYPE_INFO,
                TypeInformation.of(new TypeHint<TreeSet<WindowedSynopsis<S>>>() {
                }));

        BroadcastStream<WindowedSynopsis<S>> synopsisBroadcastStream = synopsesStream.broadcast(synopsisMapStateDescriptor);
        return queryStream.connect(synopsisBroadcastStream)
                .process(new QueryTimestampedFunction<Q, S, O>(queryFunction, maxSynopsisCount));
    }


    public static <P extends Serializable, Q extends Serializable, S extends Synopsis, O extends Serializable> SingleOutputStreamOperator<StratifiedQueryResult<Q, O, P>> queryLatestStratified
            (DataStream<StratifiedSynopsisWrapper<P, WindowedSynopsis<S>>> synopsesStream, DataStream<Tuple2<P, Q>> queryStream, QueryFunction<Q, S, O> queryFunction, Class<P> partitionClass) {

        MapStateDescriptor<P, WindowedSynopsis<S>> synopsisMapStateDescriptor = new MapStateDescriptor<P, WindowedSynopsis<S>>(
                "latestSynopsis",
                TypeInformation.of(partitionClass),
                TypeInformation.of(new TypeHint<WindowedSynopsis<S>>() {}));

        BroadcastStream<StratifiedSynopsisWrapper<P, WindowedSynopsis<S>>> broadcast = synopsesStream.broadcast(synopsisMapStateDescriptor);

        KeyedStream<Tuple2<P, Q>, Tuple> keyedQueryStream = queryStream.keyBy(0);

        return keyedQueryStream.connect(broadcast)
                .process(new QueryLatestStratifiedFunction<P, Q, S, O>(queryFunction, partitionClass, synopsisMapStateDescriptor));
    }


    public static <P extends Serializable, Q extends Serializable, S extends Synopsis, O extends Serializable> SingleOutputStreamOperator<StratifiedQueryResult<TimestampedQuery<Q>, O, P>> queryTimestampedStratified
            (DataStream<StratifiedSynopsisWrapper<P, WindowedSynopsis<S>>> synopsesStream,
             DataStream<Tuple2<P, TimestampedQuery<Q>>> queryStream,
             QueryFunction<Tuple2<P, TimestampedQuery<Q>>, WindowedSynopsis<S>, StratifiedQueryResult<TimestampedQuery<Q>, O, P>> queryFunction,
             Class<P> partitionClass, int maxSynopsisCount) {

        // MapStateDescriptor for the BroadcastState which contains the stored synopsis keyed by <P>
        MapStateDescriptor<P, TreeSet<WindowedSynopsis<S>>> synopsisMapStateDescriptor = new MapStateDescriptor<P, TreeSet<WindowedSynopsis<S>>>(
                "latestSynopsis",
                TypeInformation.of(partitionClass),
                TypeInformation.of(new TypeHint<TreeSet<WindowedSynopsis<S>>>() {
                }));

        BroadcastStream<StratifiedSynopsisWrapper<P, WindowedSynopsis<S>>> broadcast = synopsesStream.broadcast(synopsisMapStateDescriptor);

        final KeyedStream<Tuple2<P, TimestampedQuery<Q>>, Tuple> keyedQueryStream = queryStream.keyBy(0);

        final SingleOutputStreamOperator<StratifiedQueryResult<TimestampedQuery<Q>, O, P>> queryResultStream = keyedQueryStream.connect(broadcast)
                .process(new QueryStratifiedTimestampedFunction<P, Q, S, O>(maxSynopsisCount, queryFunction, synopsisMapStateDescriptor));

        return queryResultStream;
    }
}
