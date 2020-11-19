package de.tub.dima.condor.core.tests;
import de.tub.dima.condor.core.synopsis.Sampling.FiFoSampler;
import de.tub.dima.condor.core.synopsis.Sampling.TimestampedElement;
import org.junit.Test;
import java.util.*;
import java.io.File;
import java.io.FileNotFoundException;
import org.junit.jupiter.api.Assertions;

/**
 * @author Zahra Salmani
 */
public class FiFoSamplerTest {
     @Test
   public void updateTest() {
        FiFoSampler fifoSampler = new FiFoSampler(10);

        String fileName = "data/testdata.csv";
        File file = new File(fileName);

        // read element from files before it exceeds sample size
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
            TimestampedElement sampleElement=new TimestampedElement(lines.get(i),i);
            fifoSampler.update(sampleElement);
        }
        //create desired sample when fifosampler is not full yet
        TimestampedElement[] Sample1= new TimestampedElement[]{new TimestampedElement("103",0), new TimestampedElement("52",1),
                new TimestampedElement("161",2),
                new TimestampedElement("25",3), new TimestampedElement("188", 4),new TimestampedElement("19", 5),
                new TimestampedElement("48",6),
                new TimestampedElement("93",7), new TimestampedElement("50", 8),new TimestampedElement("143",9)};


         TreeSet<TimestampedElement> notFullExpectedSample = new TreeSet<>();
         for (TimestampedElement el : Sample1) {
             notFullExpectedSample.add(el);
         }

         TreeSet<TimestampedElement>  notFullSample = fifoSampler.getSample();
         // compare desired and actual samples
         Assertions.assertTrue(sampleTreeSetComprator(notFullSample,notFullExpectedSample));


         //the same steps as above but this time read more elements than sample size, so sampler will remove older elements
        for (int i=10;i<lines.size();i++) {
            TimestampedElement sampleElement=new TimestampedElement(lines.get(i),i);
            fifoSampler.update(sampleElement);
        }
         TreeSet<TimestampedElement> fullSample = fifoSampler.getSample();
         // desired full sample
         TimestampedElement[] Sample2= new TimestampedElement[]{new TimestampedElement("87",90), new TimestampedElement("198",91),
                 new TimestampedElement("106",92),
                 new TimestampedElement("34",93), new TimestampedElement("6", 94),
                 new TimestampedElement("153", 95),new TimestampedElement("168",96),
                 new TimestampedElement("90",97), new TimestampedElement("55", 98),new TimestampedElement("59",99)};
         TreeSet<TimestampedElement> fullExpectedSample = new TreeSet<>();
         for (TimestampedElement el : Sample2) {
             fullExpectedSample.add(el);
         }

        Assertions.assertTrue(sampleTreeSetComprator(fullExpectedSample,fullSample));

     }
    @Test
    public void mergeWithEmptyTest() throws Exception {
        FiFoSampler fifoSampler = new FiFoSampler(10);//main fifo sampler other will be merged to.
        FiFoSampler otherWithSize = new FiFoSampler(12);// other sampler with different size should throws exception
        Assertions.assertThrows(IllegalArgumentException.class,()->fifoSampler.merge(otherWithSize));

        FiFoSampler otherMergedInEmpty = new FiFoSampler(10);// when main sampler is Empty and the other is full

        //update other fifosampler with sample elements and keep main sampler empty
        TimestampedElement[] Sample1= new TimestampedElement[]{new TimestampedElement("87",1), new TimestampedElement("198",5),
                new TimestampedElement("106",11),
                new TimestampedElement("34",12), new TimestampedElement("6", 17),
                new TimestampedElement("153", 20),new TimestampedElement("168",23),
                new TimestampedElement("90",24), new TimestampedElement("55", 28),new TimestampedElement("39",32)};

        for (TimestampedElement el : Sample1) {
            otherMergedInEmpty.update(el);
        }

        TreeSet<TimestampedElement> mergeMainEmptyExpectedTree = (TreeSet<TimestampedElement>)otherMergedInEmpty.getSample().clone();//
        TreeSet<TimestampedElement> mergemainEmptyTree = fifoSampler.merge(otherMergedInEmpty).getSample();
        Assertions.assertTrue(sampleTreeSetComprator(mergeMainEmptyExpectedTree,mergemainEmptyTree));
        //update main fifosampler with sample elements
        for (TimestampedElement el : Sample1) {
            fifoSampler.update(el);
        }

        FiFoSampler other = new FiFoSampler(10);//other Fifo sampler
        //merge non-empty fifosampler with a empty other sample, should not effect it
        TreeSet<TimestampedElement> mergeEmptyExpectedTree = (TreeSet<TimestampedElement>)fifoSampler.getSample().clone();//
        TreeSet<TimestampedElement> mergeEmptyTree = fifoSampler.merge(other).getSample();
        Assertions.assertTrue(sampleTreeSetComprator(mergeEmptyExpectedTree,mergeEmptyTree));

    }

    @Test
    public void mergeTest() throws Exception {
        FiFoSampler fifoSampler = new FiFoSampler(10);//main fifo sampler other will be merged to.

        //update fifosampler with sample elements
        TimestampedElement[] Sample1= new TimestampedElement[]{new TimestampedElement("87",1), new TimestampedElement("198",5),
                new TimestampedElement("106",11),
                new TimestampedElement("34",12), new TimestampedElement("6", 17),
                new TimestampedElement("153", 20),new TimestampedElement("168",23),
                new TimestampedElement("90",24), new TimestampedElement("55", 28),new TimestampedElement("39",32)};

        for (TimestampedElement el : Sample1) {
            fifoSampler.update(el);
        }

        FiFoSampler other = new FiFoSampler(10);//other Fifo sampler
        //update other sampler with list of new samplers
        TimestampedElement[] Sample2= new TimestampedElement[]{new TimestampedElement("80",3), new TimestampedElement("98",6),
                new TimestampedElement("16",8),
                new TimestampedElement("34",13), new TimestampedElement("15", 19),
                new TimestampedElement("183", 22),new TimestampedElement("1668",25),
                new TimestampedElement("91",29), new TimestampedElement("55", 31),new TimestampedElement("59",32)};

        for (TimestampedElement el : Sample2) {
            other.update(el);
        }


        //create expected sample of merge result
        TimestampedElement[] mergeExpectedSample= new TimestampedElement[]{new TimestampedElement("153", 20),new TimestampedElement("183", 22),
                new TimestampedElement("168",23),new TimestampedElement("90",24),new TimestampedElement("1668",25),
                new TimestampedElement("55", 28),new TimestampedElement("91",29),new TimestampedElement("55", 31),
                new TimestampedElement("39",32),new TimestampedElement("59",32)};
        TreeSet<TimestampedElement> mergeExpectedTree = new TreeSet<>();
        for (TimestampedElement el : mergeExpectedSample) {
            mergeExpectedTree.add(el);
        }
        TreeSet<TimestampedElement> mergeTree = fifoSampler.merge(other).getSample();
        //compare actual and expected merge results
        Assertions.assertTrue(sampleTreeSetComprator(mergeTree,mergeExpectedTree));

    }

   public boolean sampleTreeSetComprator(TreeSet<TimestampedElement> tree1, TreeSet<TimestampedElement> tree2)
   {
       boolean Result=true;

       TimestampedElement element1, element2;
       if(tree1.size()!=tree2.size())
       {
           Result=false;
           System.out.println("size");
       }
       while(!tree1.isEmpty()&&!tree2.isEmpty()){

           element1= tree1.pollFirst();
           element2= tree2.pollFirst();
           if(((String)element1.getValue()).compareTo((String) element2.getValue()) != 0)
           {
               Result=false;
           }
       }
       return Result;
   }

}




