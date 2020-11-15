package de.tub.dima.condor.core.Tests;

import org.junit.jupiter.api.Test;
import de.tub.dima.condor.core.Synopsis.Histograms.SplitAndMergeWithDDSketch;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

/**
 * @author Zahra Salmani
 */
public class SplitAndMergeWithDDSketchTest {

@Test
    public void updateTest(){
    SplitAndMergeWithDDSketch SMWithDDSketch= new SplitAndMergeWithDDSketch(10,0.1);
    //read from file and update with read elements
    File file= new File("data/dataset.csv");
    Scanner inputStream;
    try{
        inputStream = new Scanner(file);
        while(inputStream.hasNext()){
            String line= inputStream.next();
            SMWithDDSketch.update(Integer.parseInt(line));
        }
        inputStream.close();
    }catch (FileNotFoundException e) {
        e.printStackTrace();
    }

}
}
