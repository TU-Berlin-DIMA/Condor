package de.tub.dima.condor.core.Tests;


import de.tub.dima.condor.core.Synopsis.Wavelets.DistributedWaveletsManager;
import de.tub.dima.condor.core.Synopsis.Wavelets.WaveletSynopsis;

import java.util.ArrayList;

public class waveletTest {
    public static void main(String[] args) throws Exception {
        WaveletSynopsis waveletSynopsis = new WaveletSynopsis(10);
        waveletSynopsis.update(-1);
        waveletSynopsis.update(8);
        waveletSynopsis.update(6);
        waveletSynopsis.update(10);
        System.out.println(waveletSynopsis);

//        WaveletSynopsis waveletSynopsis = new WaveletSynopsis(6);
//        waveletSynopsis.update(9);
//        waveletSynopsis.update(3);
//        System.out.println(waveletSynopsis);
//        waveletSynopsis.update(9);
//        waveletSynopsis.update(-5);
//        System.out.println(waveletSynopsis);
//        waveletSynopsis.update(5);
//        waveletSynopsis.update(13);
//        System.out.println(waveletSynopsis);
//        waveletSynopsis.update(13);
//        waveletSynopsis.update(17);
//        System.out.println(waveletSynopsis);
//        waveletSynopsis.update(14);
//        waveletSynopsis.update(-2);
//        System.out.println(waveletSynopsis);
//        waveletSynopsis.update(9);
//        waveletSynopsis.update(7);
//        System.out.println(waveletSynopsis);
//        waveletSynopsis.update(7);
//        waveletSynopsis.update(3);
//        System.out.println(waveletSynopsis);
//
//        waveletSynopsis.padding();
//        System.out.println(waveletSynopsis);
//
//        for (int i = 0; i < 14; i++) {
//            System.out.println(waveletSynopsis.pointQuery(i));
//        }
//
//        System.out.println("Begin Range Sum Queries");
//        double sum = 0;
//
//        for (int i = 0; i < 14; i++) {
//            sum += waveletSynopsis.pointQuery(i);
//            System.out.println("rangeSum(0:" + i + "): " + waveletSynopsis.rangeSumQuery(0,i) + "  --  " + sum + " (true value)");
//        }
//
//        WaveletSynopsis partition0 = new WaveletSynopsis(10);
//        WaveletSynopsis partition1 = new WaveletSynopsis(10);
//        WaveletSynopsis partition2 = new WaveletSynopsis(10);
//
//        partition0.update(9);
//        partition1.update(3);
//        partition2.update(9);
//        partition0.update(-5);
//        partition1.update(5);
//        partition2.update(13);
//        partition0.update(13);
//        partition1.update(17);
//        partition2.update(14);
//        partition0.update(-2);
//        partition1.update(9);
//        partition2.update(7);
//        partition0.update(7);
//        partition1.update(3);
//        partition2.update(0);
//        partition0.update(0);
//        partition1.update(0);
//        partition2.update(0);
//
//        System.out.println(partition0);
//
//        partition0.padding();
//        partition1.padding();
//        partition2.padding();
//
//        System.out.println(partition0);
//        System.out.println(partition1);
//        System.out.println(partition2);
//        ArrayList<WaveletSynopsis> arrayList = new ArrayList<>();
//        arrayList.add(partition0);
//        arrayList.add(partition1);
//        arrayList.add(partition2);
//
//        DistributedWaveletsManager distributedWaveletsManager = new DistributedWaveletsManager(3, arrayList);
//
//        double rangeSum = 0;
//
//        for (int i = 0; i < 14; i++) {
//            double pointQuery = distributedWaveletsManager.pointQuery(i);
//            rangeSum += pointQuery;
//            System.out.println("PointQuery(" + i + "):  " + pointQuery);
//            System.out.println("Range Query (0 - " + i + "): " + distributedWaveletsManager.rangeSumQuery(0, i) + "   -- " + rangeSum + " (true value)");
//        }
//
//        rangeSum = 102;
//
//        for (int i = 0; i < 14; i++) {
//            double pointQuery = 0;
//            if (i>0){
//                pointQuery = distributedWaveletsManager.pointQuery(i-1);
//            }
//            rangeSum -= pointQuery;
//            System.out.println("Range Query (" + i + "- 13): " + distributedWaveletsManager.rangeSumQuery(i, 13) + "   -- " + rangeSum + " (true value)");
//        }

    }
}
