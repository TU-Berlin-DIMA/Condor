package Tests;

import Histograms.BashHistogram;
import Histograms.EquiDepthHistogram;

public class BashHistogramTest {
    public static void main(String[] args){
        BashHistogram bash = new BashHistogram(3, 4);
        bash.update(0);
        EquiDepthHistogram result;
        System.out.println(bash);
        result = bash.buildEquiDepthHistogram();
        System.out.println(result);
        for (int i = 0; i < 2; i++) {
            bash.update(1);
            System.out.println(bash);
            result = bash.buildEquiDepthHistogram();
            System.out.println(result);
        }
        for (int i = 0; i < 3; i++) {
            bash.update(2);
            System.out.println(bash);
            result = bash.buildEquiDepthHistogram();
            System.out.println(result);
        }
        for (int i = 0; i < 4; i++) {
            bash.update(3);
            System.out.println(bash);
            result = bash.buildEquiDepthHistogram();
            System.out.println(result);
        }
        for (int i = 0; i < 5; i++) {
            bash.update(4);
            System.out.println(bash);
            result = bash.buildEquiDepthHistogram();
            System.out.println(result);
        }
        for (int i = 0; i < 6; i++) {
            bash.update(5);
            System.out.println(bash);
            result = bash.buildEquiDepthHistogram();
            System.out.println(result);
        }
        for (int i = 0; i < 5; i++) {
            bash.update(6);
            System.out.println(bash);
            result = bash.buildEquiDepthHistogram();
            System.out.println(result);
        }
        for (int i = 0; i < 4; i++) {
            bash.update(7);
            System.out.println(bash);
            result = bash.buildEquiDepthHistogram();
            System.out.println(result);
        }
        for (int i = 0; i < 3; i++) {
            bash.update(8);
            System.out.println(bash);
            result = bash.buildEquiDepthHistogram();
            System.out.println(result);
        }
        for (int i = 0; i < 2; i++) {
            bash.update(9);
            System.out.println(bash);
            result = bash.buildEquiDepthHistogram();
            System.out.println(result);
        }

        int total = 1+2+3+4+5+6+5+4+3+2;

        result = bash.buildEquiDepthHistogram();
        double range0 = result.rangeQuery(0,2);

        System.out.println("0-10: " + range0);
        System.out.println(result);
    }
}
