package de.tub.dima.condor.core.Tests;


import de.tub.dima.condor.core.Synopsis.Wavelets.DistributedSliceWaveletsManager;
import de.tub.dima.condor.core.Synopsis.Wavelets.DistributedWaveletsManager;
import de.tub.dima.condor.core.Synopsis.Wavelets.SliceWaveletsManager;
import de.tub.dima.condor.core.Synopsis.Wavelets.WaveletSynopsis;

import java.util.ArrayList;

public class SliceManagerWaveletTest {
    public static void main(String[] args) throws Exception {

        ArrayList<WaveletSynopsis<Integer>> slices0 = new ArrayList<>();
        ArrayList<WaveletSynopsis<Integer>> slices1 = new ArrayList<>();
        ArrayList<WaveletSynopsis<Integer>> slices2 = new ArrayList<>();

        int waveletSize = 20;
        for (int i = 0; i < 4; i++) {
            slices0.add(new WaveletSynopsis<Integer>(waveletSize));
            slices1.add(new WaveletSynopsis<Integer>(waveletSize));
            slices2.add(new WaveletSynopsis<Integer>(waveletSize));
        }

        for (int i = 0; i < 120; i++) {
//            try {

                if (i % 3 == 0) {
                    if (i < 50) {
                        slices0.get(0).update(i);
                    } else if (i < 60) {
                        slices0.get(1).update(i);
                    } else if (i < 70) {
                        slices0.get(2).update(i);
                    } else {
                        slices0.get(3).update(i);
                    }
                } else if (i % 3 == 1) {
                    if (i < 30) {
                        slices1.get(0).update(i);
                    } else if (i < 40) {
                        slices1.get(1).update(i);
                    } else if (i < 110) {
                        slices1.get(2).update(i);
                    } else {
                        slices1.get(3).update(i);
                    }
                } else if (i % 3 == 2) {
                    if (i < 30) {
                        slices2.get(0).update(i);
                    } else if (i < 60) {
                        slices2.get(1).update(i);
                    } else if (i < 90) {
                        slices2.get(2).update(i);
                    } else {
                        slices2.get(3).update(i);
                    }
                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
        }
//        for (int i = 0; i < 4; i++) {
//            slices0.get(i).padding();
//            slices1.get(i).padding();
//            slices2.get(i).padding();
//        }

        SliceWaveletsManager<Integer> sliceManager0 = new SliceWaveletsManager<>();
        SliceWaveletsManager<Integer> sliceManager1 = new SliceWaveletsManager<>(slices1);
        SliceWaveletsManager<Integer> sliceManager2 = new SliceWaveletsManager<>(slices2);
        sliceManager0.addSynopsis(slices0.get(0));
        sliceManager0.addSynopsis(slices0.get(1));
        sliceManager0.addSynopsis(slices0.get(2));
        sliceManager0.addSynopsis(slices0.get(3));

//        ArrayList<SliceWaveletsManager<Integer>> slices = new ArrayList<>();
//        slices.add(sliceManager0);
//        slices.add(sliceManager1);
//        slices.add(sliceManager2);

//        Environment.out.println("Predicted: " + slices0.get(0).pointQuery(0));
//
//        Environment.out.println("Predicted: " + sliceManager0.pointQuery(0));
        DistributedSliceWaveletsManager<Integer> globalManager = new DistributedSliceWaveletsManager<>();
        globalManager.addSynopsis(sliceManager0);
        globalManager.addSynopsis(sliceManager1);
        globalManager.addSynopsis(sliceManager2);
//        DistributedSliceWaveletsManager<Integer> globalManager = new DistributedSliceWaveletsManager<>(slices);
//        Environment.out.println(globalManager.pointQuery(76));
        double sum = 0.0;
        for (int i = 0; i < 120; i++) {
            double v = globalManager.pointQuery(i);
            if (Math.abs(v - i) > 0.1) {
                System.out.println("Predicted: " + v + "  |  Real:" + i);
                if (i == 0) {
                    sum += Math.abs(v - i);
                } else {
                    sum += (Math.abs(v - i) / i);
                }
            }
//            Environment.out.println(v);
        }
        System.out.println("Average error: " + sum / 120 * 100 + "%");
    }
}
