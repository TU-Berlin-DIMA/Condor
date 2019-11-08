package Tests;
import Sampling.FiFoSampler;
import org.junit.Assert;
import org.junit.Test;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Scanner;
import java.io.File;
import java.io.FileNotFoundException;
/**
 * @author Zahra Salmani
 */
public class FiFoSamplerTest {
    @Test
    public void updateTest() {
        FiFoSampler fifoSampler = new FiFoSampler(10);

        String fileName = "data/testdata.csv";
        File file = new File(fileName);

        // this gives you a 2-dimensional array of strings
        LinkedList<String> lines = new LinkedList<>();
        Scanner inputStream;

        try {
            inputStream = new Scanner(file);
            while (inputStream.hasNext()) {
                String line = inputStream.next();
                // this adds the currently parsed line to the 2-dimensional string array
                lines.add(line);
            }
            inputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < 10; i++) {
            fifoSampler.update(lines.get(i));
        }
        LinkedList<String> fixedSample1 = new LinkedList<>();
        fixedSample1.addAll(0, Arrays.asList(new String[]{"103", "52", "161", "25", "188", "19", "48", "93", "50", "143"}));

        Object notfullsample = fifoSampler.getSample();
        Assert.assertEquals(notfullsample, fixedSample1);

        for (int i=10;i<lines.size();i++) {
            fifoSampler.update(lines.get(i));
        }
        Object fullSample = fifoSampler.getSample();
        LinkedList<String> fixedSample2 = new LinkedList<>();
        fixedSample2.addAll(0, Arrays.asList(new String[]{"87", "198", "106", "34", "6", "153", "168", "90", "55", "59"}));

        Assert.assertEquals(fullSample, fixedSample2);
    }

    @Test(expected = Exception.class)
    public void illegalmergesamplesizeTest() throws Exception {
        FiFoSampler fifoSampler = new FiFoSampler(12);
        FiFoSampler other = new FiFoSampler(10);
        fifoSampler.merge(other);
    }


    @Test
    public void mergeTest() throws Exception {


    }
}




