package Sketches;

import Synopsis.Synopsis;
import org.apache.flink.util.XORShiftRandom;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;

/**
 * Implementation of classical Cuckoo Filter sketch to estimate the the elements that were contained in a
 * datastream.
 * Tis implementation uses a family of pairwise independent hash functions to update the hash map of the
 * sketch and has the limitation that are not suitable for applications that insert the same
 * item more than 2b times (b is the bucket size) because the error guarantees get lost.
 *
 * @param <T> the type of elements maintained by this sketch
 * @author Rudi Poepsel Lemaitre
 */
public class CuckooFilter<T> implements Synopsis<T>, Serializable {
    private int bucketSize;
    private ArrayList<Byte> buckets[];
    private int maxNumKicks = 500;
    private int a; // hash function parameter
    private int b; // hash function parameter
    private final int p = 1610612741; // prime
    private XORShiftRandom random;
    private int elementsProcessed;
    private boolean full;

    /**
     * Creates a new Cuckoo Filter.
     *
     * @param bucketSize Maximal bucket size
     * @param numBuckets number of buckets maintained by this sketch
     * @param seed       for the randomness of the hash functions
     */
    public CuckooFilter(Integer bucketSize, Integer numBuckets, Long seed) {
        this.bucketSize = bucketSize;
        this.buckets = new ArrayList[numBuckets];
        for (int i = 0; i < numBuckets; i++) {
            buckets[i] = new ArrayList<>();
        }
        this.random = new XORShiftRandom(seed);
        this.a = random.nextInt(p);
        this.b = random.nextInt(p);
        this.full = false;
    }

    /**
     * Get the first 8 Bits of the java hash code from the element as its fingerprint
     *
     * @param element to get the fingerprint from
     * @return 8 Bit long fingerprint
     */
    public byte getFingerprint(T element) {
        return (byte) element.hashCode();
    }

    public int hash(int elementHashCode) {
        int temp = (a * elementHashCode + b) % p % buckets.length;
        if (temp < 0) {
            temp *= -1;
        }
        return temp;
    }

    /**
     * Add the fingerprint of the new incoming element into the corresponding bucket. In the case that the
     * bucket is considered full the element will be added to the alternative location using a partial hash function
     * if the second bucket is also full the function will kick out multiple elements to its alternate location until
     * the maximal number of kicks is reached, in this case the structure is considered full.
     *
     * @param element new incoming element
     */
    @Override
    public void update(T element) {
        byte fingerprint = getFingerprint(element);
        int pos1 = hash(element.hashCode());
        if (full || buckets[pos1].size() < bucketSize) {
            buckets[pos1].add(fingerprint);
            elementsProcessed++;
            return;
        }
        int pos2 = (pos1 ^ hash(fingerprint)) % buckets.length;
        if (buckets[pos2].size() < bucketSize) {
            buckets[pos2].add(fingerprint);
            elementsProcessed++;
            return;
        }
        // must relocate existing items
        int pos = random.nextDouble() < 0.5 ? pos1 : pos2;
        for (int i = 0; i < maxNumKicks; i++) {
            int toReplace = random.nextInt(bucketSize);
            byte replaced = buckets[pos].get(toReplace);
            buckets[pos].set(toReplace, fingerprint);
            fingerprint = replaced;
            pos = (pos ^ hash(fingerprint)) % buckets.length;
            if (buckets[pos].size() < bucketSize) {
                buckets[pos].add(fingerprint);
                elementsProcessed++;
                return;
            }
        }
        full = true;
        buckets[pos1].add(fingerprint);
        elementsProcessed++;
        return;
    }

    /**
     * Lookup if the elements fingerprint was possibly contained in the datastream
     *
     * @param element
     * @return true if the element was possibly in the datastream, false guarantees the element was not in the
     * datastream
     */
    public boolean lookup(T element) {
        Byte fingerprint = getFingerprint(element);
        int pos1 = hash(element.hashCode());
        if (buckets[pos1].contains(fingerprint)) {
            return true;
        }
        int pos2 = (pos1 ^ hash(fingerprint)) % buckets.length;
        if (buckets[pos2].contains(fingerprint)) {
            return true;
        }
        return false;
    }

    /**
     * Delete an the fingerprint of the data structure
     *
     * @param element to be deleted
     * @return true if the elements fingerprint was founded and deleted
     */
    public boolean delete(T element) {

        Byte fingerprint = getFingerprint(element);
        int pos1 = hash(element.hashCode());
        int toRemove = buckets[pos1].indexOf(fingerprint);
        if (toRemove != -1) {
            buckets[pos1].remove(toRemove);
            return true;
        }
        int pos2 = (pos1 ^ hash(fingerprint)) % buckets.length;
        toRemove = buckets[pos2].indexOf(fingerprint);
        if (toRemove != -1) {
            buckets[pos2].remove(toRemove);
            return true;
        }

        return false;
    }

    public int getBucketSize() {
        return bucketSize;
    }

    public ArrayList<Byte>[] getBuckets() {
        return buckets;
    }

    public int getMaxNumKicks() {
        return maxNumKicks;
    }

    public int getA() {
        return a;
    }

    public int getB() {
        return b;
    }

    public int getElementsProcessed() {
        return elementsProcessed;
    }

    /**
     * Function to Merge two Cuckoo Filters by adding the content of the buckets.
     *
     * @param other synopsis to be merged with
     * @return merged synopsis
     * @throws Exception
     */
    @Override
    public CuckooFilter merge(Synopsis other) {
        if (other instanceof CuckooFilter) {
            CuckooFilter<T> otherCF = (CuckooFilter) other;
            if (!this.full && !otherCF.full && otherCF.getA() == this.a && otherCF.getB() == this.b && otherCF.getBuckets().length == this.buckets.length
                    && otherCF.getMaxNumKicks() == this.maxNumKicks && otherCF.getBucketSize() == this.bucketSize) {
                boolean merged = true;
                outerloop:
                for (int i = 0; i < this.buckets.length; i++) {
                    for (int j = 0; j < otherCF.getBuckets()[i].size(); j++) {
                        byte fingerprint = otherCF.getBuckets()[i].get(j);
                        if (buckets[i].size() < bucketSize) {
                            buckets[i].add(fingerprint);
                        } else {
                            int pos2 = (i ^ hash(fingerprint)) % buckets.length;
                            if (buckets[pos2].size() < bucketSize) {
                                buckets[pos2].add(fingerprint);
                            } else {
                                merged = false;
                                break outerloop;
                            }
                        }
                    }
                }
                if (merged) {
                    this.elementsProcessed += otherCF.getElementsProcessed();
                    return this;
                } else {
                    throw new IllegalArgumentException("Cuckoo Filter is considered full");
                }
            }
        }
        throw new IllegalArgumentException("Sketches to merge have to be the same size and hash Functions");

    }

    @Override
    public String toString() {
        String sketch = new String();
        sketch += "Hash function:\n";
        sketch += "A: " + a + "  ";
        sketch += "B: " + b + "\n";
        sketch += "Bucket size: " + bucketSize + "\n";
        for (int i = 0; i < buckets.length; i++) {
            sketch += i + " " + buckets[i].size() + "\n";
        }
        sketch += "Elements processed: " + elementsProcessed + "\n";

        return sketch;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeInt(bucketSize);
        for (int i = 0; i < buckets.length; i++) {
            out.writeObject(buckets[i]);
        }
        out.writeInt(maxNumKicks);
        out.writeInt(a);
        out.writeInt(b);
        out.writeObject(random);
        out.writeInt(elementsProcessed);
        out.writeBoolean(full);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        bucketSize = in.readInt();
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = (ArrayList<Byte>) in.readObject();
        }
        maxNumKicks = in.readInt();
        a = in.readInt();
        b = in.readInt();
        random = (XORShiftRandom) in.readObject();
        elementsProcessed = in.readInt();
        full = in.readBoolean();
    }

    private void readObjectNoData() throws ObjectStreamException {
        System.out.println("readObjectNoData() called - should give an exception");
    }
}
