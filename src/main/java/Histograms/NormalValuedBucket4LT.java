package Histograms;

public class NormalValuedBucket4LT {
    int lowerBound, upperBound, root;
    byte level1;
    short level2;
    short level3;

    public NormalValuedBucket4LT(int lowerBound, int upperBound) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.root = 0;
        this.level1 = 0;
        this.level2 = 0;
        this.level3 = 0;
    }

    public void update(int key, int frequency){
        root += frequency;
        int difference = frequency - root;
        if (key < (upperBound+lowerBound)/2){

        }
    }

    public void update(int key){
        this.update(key, 1);
    }

    public int getRoot() {
        return root;
    }

    public int getFrequency(int lowerBound, int upperBound){
        // TODO
        return 0;
    }
}
