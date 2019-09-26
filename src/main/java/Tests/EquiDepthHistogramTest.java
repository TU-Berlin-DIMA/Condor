package Tests;

import Histograms.EquiDepthHistBuilder;
import Histograms.EquiDepthHistogram;

import java.util.TreeMap;

public class EquiDepthHistogramTest {
    public static void main(String[] args) throws Exception {

        TreeMap<Integer, Integer> map = new TreeMap<>();
        map.put(0,2);
        map.put(1,2);
        map.put(2,2);
        map.put(3,2);
        map.put(4,2);
        map.put(5,2);
        map.put(6,2);
        map.put(7,2);
        map.put(8,2);
        map.put(9,10);

        int total = 9*2+10;

        EquiDepthHistBuilder builder = new EquiDepthHistBuilder();
        EquiDepthHistogram histogram = builder.buildEquiDepthHistogram(map, 5, total);
        System.out.println(histogram);

        double range0 = histogram.rangeQuery(0,10);

        System.out.println("0-10: " + range0);

    }
}
