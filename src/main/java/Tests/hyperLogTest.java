package Tests;

import Sketches.HyperLogLogSketch;
import org.apache.flink.api.java.tuple.Tuple2;

public class hyperLogTest {

    public static void main(String[] args) throws Exception {

        HyperLogLogSketch hll = null;

        Tuple2<Integer, Integer>[] data = new Tuple2[10];

        for (int i = 0; i < data.length; i++) {
            data[i] = new Tuple2<>(i, 1);
            hll.update(data[i].f1);
        }

        System.out.println("DistinctItemsEstimator: "+hll.distinctItemsEstimator());

    }
}
