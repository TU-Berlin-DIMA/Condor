package de.tub.dima.condor.core.synopsis.Wavelets;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;

/**
 * FrontlineNode Class which is essentially stores averages and information to DataNodes / other Frontline Nodes
 */
public class FrontlineNode implements Serializable {

    DataNode hungChild;     // pointer to DataNode hanging from this
    double value;
    FrontlineNode next;     // pointer to next (upper) fnode
    FrontlineNode prev;     // ppinter to previous (lower) fnode
    double positiveerror;   // error quantities from deleted orphans
    double negativeerror;   // error quantities from deleted orphans
    boolean errorhanging;
    int level;

    public FrontlineNode(double value, int level) {
        this.value = value;
        this.level = level;
        errorhanging = false;
    }

    @Override
    public String toString() {
        return ("Level " + this.level + " ---> " + this.value);
    }

    public void mergeError(double minError, double maxError){
        positiveerror = Math.max(maxError, positiveerror);
        negativeerror = Math.min(minError, negativeerror);
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeObject(hungChild);
        out.writeDouble(value);
        out.writeObject(next);
        out.writeObject(prev);
        out.writeDouble(positiveerror);
        out.writeDouble(negativeerror);
        out.writeBoolean(errorhanging);
        out.writeInt(level);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException{
        hungChild = (DataNode) in.readObject();
        value = in.readDouble();
        next = (FrontlineNode) in.readObject();
        prev = (FrontlineNode) in.readObject();
        positiveerror = in.readDouble();
        negativeerror = in.readDouble();
        errorhanging = in.readBoolean();
        level = in.readInt();
    }


    private void readObjectNoData() throws ObjectStreamException {
        // no idea what to put here...
    }
}
