package Tests;

import Histograms.EquiDepthHistogramTrivial;

import java.util.TreeMap;

public class EquiDepthHistogramTest {
    public static void main(String[] args) throws Exception {

        TreeMap<Integer, Integer> map = new TreeMap<>();
        map.put(0,1);
        map.put(1,2);
        map.put(2,3);
        map.put(3,4);
        map.put(4,5);
        map.put(5,6);
        map.put(6,5);
        map.put(7,4);
        map.put(8,3);
        map.put(9,2);

        int total = 1+2+3+4+5+6+5+4+3+2;

        EquiDepthHistogramTrivial histogram = new EquiDepthHistogramTrivial(map, 5, total);

        int range0 = histogram.rangeQuery(0,10);

        System.out.println("0-10: " + range0);

    }
}
