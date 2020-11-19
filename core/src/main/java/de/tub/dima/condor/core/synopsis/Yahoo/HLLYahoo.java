package de.tub.dima.condor.core.synopsis.Yahoo;

import de.tub.dima.condor.core.synopsis.CommutativeSynopsis;
import de.tub.dima.condor.core.synopsis.MergeableSynopsis;
import de.tub.dima.condor.core.synopsis.StratifiedSynopsis;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.Union;

import java.io.*;

public class HLLYahoo extends StratifiedSynopsis implements CommutativeSynopsis<Integer>, Serializable {
    public HllSketch hll;
    public int processedElements = 0;

    public HLLYahoo() {
        hll = new HllSketch();
    }



    @Override
    public String toString() {
        return "Processed Elements: "+processedElements+"\n"+hll.toString();
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
//        System.out.println("Serialize: \n"+this.toString());
        out.writeInt(processedElements);
        byte[] bytes = hll.toUpdatableByteArray();
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        this.processedElements = in.readInt();
        int length = in.readInt();
        byte[] bytes = new byte[length];
        in.read(bytes);
        this.hll = HllSketch.heapify(bytes);
    }

    private void readObjectNoData() throws ObjectStreamException {
        throw new NotSerializableException("Serialization error in class " + this.getClass().getName());
    }

    @Override
    public HLLYahoo merge(MergeableSynopsis<Integer> other) {
        if (!(other instanceof HLLYahoo)){
            throw new IllegalArgumentException("MergeableSynopsis.Sketches can only be merged with other MergeableSynopsis.Sketches of the same Type \n" +
                    "otherHLL.getClass() = " + other.getClass() + "\n" +
                    "otherHLL.getClass().isInstance(HyperLogLogSketch.class) = false");
        }
        HLLYahoo otherHLL = (HLLYahoo) other;
        Union union = new Union();
        union.update(this.hll);
        union.update(otherHLL.hll);

        this.processedElements += otherHLL.processedElements;

        this.hll = union.getResult();

        return this;
    }

    @Override
    public void update(Integer element) {
        this.hll.update(element);
    }
}
