package de.tub.dima.condor.core.Synopsis.Wavelets;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;

/**
 * Class which represents an error-tree node in the sibling tree.
 */
public class DataNode implements Serializable, Comparable<DataNode> {
    double data;    // coefficient value
    double maxerrorleft = 0;     // maximum error left subtree
    double minerrorleft = 0;     // minimum error left subtree
    double maxerrorright = 0;     // maximum error right subtree
    double minerrorright = 0;     // minimum error right subtree
    double maxabserror;
    int level;  // level of node in sibling-tree
    int orderinlevel;   // order of node in error-tree level
    FrontlineNode front;    // Frontline node where this node is hanged - only set for the directly hanged DataNode (1 per frontline node)
    DataNode leftMostChild;  // this nodes leftmost child - usually on this nodes left subtree but can in cases of multiple deletions also be on the right subtree
    DataNode parent;    // pointer to the parent DataNode
    Utils.relationship reltoparent;     // relationship of this node to its parent node
    DataNode nextSibling;   // next (right) sibling
    DataNode previousSibling;   // previous (left) sibling

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeDouble(data);
        out.writeDouble(maxerrorleft);
        out.writeDouble(minerrorleft);
        out.writeDouble(maxerrorright);
        out.writeDouble(minerrorright);
        out.writeDouble(maxabserror);
        out.writeInt(level);
        out.writeInt(orderinlevel);
        out.writeObject(front);
        out.writeObject(leftMostChild);
        out.writeObject(parent);
        out.writeObject(reltoparent);
        out.writeObject(nextSibling);
        out.writeObject(previousSibling);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException{
        data = in.readDouble();
        maxerrorleft = in.readDouble();
        minerrorleft = in.readDouble();
        maxerrorright = in.readDouble();
        minerrorright = in.readDouble();
        maxabserror = in.readDouble();
        level = in.readInt();
        orderinlevel = in.readInt();
        front = (FrontlineNode) in.readObject();
        leftMostChild = (DataNode) in.readObject();
        parent = (DataNode) in.readObject();
        reltoparent = (Utils.relationship) in.readObject();
        nextSibling = (DataNode) in.readObject();
        previousSibling = (DataNode) in.readObject();
    }


    private void readObjectNoData() throws ObjectStreamException {
        // no idea what to put here...
    }

    public DataNode(double data, int level, int orderinlevel, DataNode leftChild, DataNode previousSibling) {
        this.data = data;
        this.level = level;
        this.orderinlevel = orderinlevel;
        if (leftChild != null){
            this.leftMostChild = leftChild;
            this.leftMostChild.setParent(this);
        }
        if (previousSibling != null){
            this.previousSibling = previousSibling;
            previousSibling.nextSibling = this;
        }
        reltoparent = Utils.relationship.none;
    }

    /**
     * Computes the ErrorValues of this node from all it's direct descendants.
     *
     * @param   prevFrontlineNode
     * @return  true if error values changed
     */
    public boolean computeErrorValues(FrontlineNode prevFrontlineNode){

        double oldmaxleft = maxerrorleft;
        double oldminleft = minerrorleft;
        double oldmaxright = maxerrorright;
        double oldminright = minerrorright;

        DataNode currentChild = leftMostChild;

        while (currentChild != null){
            double maxerror = Math.max(currentChild.maxerrorleft, currentChild.maxerrorright);
            double minerror = Math.min(currentChild.minerrorleft, currentChild.minerrorright);
            if (currentChild.reltoparent == Utils.relationship.leftChild){
                maxerrorleft = Math.max(maxerrorleft, maxerror);
                minerrorleft = Math.min(minerrorleft, minerror);
            }else if (currentChild.reltoparent == Utils.relationship.rightChild){
                maxerrorright = Math.max(maxerrorright, maxerror);
                minerrorright = Math.min(minerrorright, minerror);
            }

            currentChild = currentChild.nextSibling;
        }

        // also regard possible stored error metrics in the previous frontline node
        // since the tree is created from left to right, and orphan's relationship to its missing parent is always left!
        if (prevFrontlineNode != null && prevFrontlineNode.errorhanging){
            maxerrorleft = Math.max(maxerrorleft, prevFrontlineNode.positiveerror);
            minerrorleft = Math.min(minerrorleft, prevFrontlineNode.negativeerror);
        }

        if (oldmaxleft == maxerrorleft && oldminleft == minerrorleft && oldmaxright == maxerrorright && oldminright == minerrorright){
            return false;
        }else {
            return true;
        }
    }


    /**
     * compute the potential maximum absolute error if this node were to be discarded
     * @return      potential maximum absolute error
     */
    public double computeMA(){
        double left = Math.max(Math.abs(maxerrorleft - data), Math.abs(minerrorleft - data));
        double right = Math.max(Math.abs(maxerrorright + data), Math.abs(minerrorright + data));
        maxabserror =  Math.max(left,right);
        return maxabserror;
    }

    public int indexInSubtree(int queryIndex, int maxLevel){
        int coefficientsInLevel = (int) Math.pow(2, maxLevel - level);
        int treeSize = (int) Math.pow(2, maxLevel);
        int lowerBound = treeSize / coefficientsInLevel * (orderinlevel - 1); // inclusive
        int upperBound = treeSize / coefficientsInLevel * orderinlevel; // exclusive
        if (queryIndex >= lowerBound && queryIndex < (lowerBound + upperBound)/2){
            return 1;   // index in left subtree
        }else if(queryIndex >= (lowerBound + upperBound) / 2 && queryIndex < upperBound){
            return -1;  // index in right subtree
        }else {
            return 0;   // index not contained in subtree
        }
    }

    /**
     * methods used to calculate the amount of leftleaves this node has which is in the range from leftIndex to rightIndex
     * Used to calculate the range sum query
     *
     * @param leftIndex     inclusive
     * @param rightIndex    inclusive
     * @param maxLevel
     * @return
     */
    public int countLeftLeaves(int leftIndex,int rightIndex, int maxLevel){
        int coefficientsInLevel = (int) Math.pow(2, maxLevel - level);
        int treeSize = (int) Math.pow(2, maxLevel);
        int indexStartLeftSubtree = treeSize / coefficientsInLevel * (orderinlevel - 1); // inclusive
        int indexStartRightSubtree = ((treeSize / coefficientsInLevel * orderinlevel) + indexStartLeftSubtree) / 2; // exclusive
        int rightBorder = Math.min(rightIndex + 1, indexStartRightSubtree); // rightIndex + 1 to make the index exclusive
        int leftBorder = Math.max(indexStartLeftSubtree, leftIndex);

        return Math.max((rightBorder - leftBorder), 0);
    }

    /**
     * methods used to calculate the amount of rightleaves this node has which is in the range from leftIndex to rightIndex
     * Used to calculate the range sum query
     *
     * @param leftIndex     inclusive
     * @param rightIndex    inclusive
     * @param maxLevel
     * @return
     */
    public int countRightLeaves(int leftIndex, int rightIndex, int maxLevel){
        int coefficientsInLevel = (int) Math.pow(2, maxLevel - level);
        int treeSize = (int) Math.pow(2, maxLevel);
        int indexStartLeftSubtree = treeSize / coefficientsInLevel * (orderinlevel - 1); // inclusive
        int indexStartRightSubtree = ((treeSize / coefficientsInLevel * orderinlevel) + indexStartLeftSubtree) / 2; // exclusive
        int indexEndRightSubtree = treeSize / coefficientsInLevel * orderinlevel; // exclusive
        int rightBorder = Math.min(rightIndex +1, indexEndRightSubtree);
        int leftBorder = Math.max(indexStartRightSubtree, leftIndex);

        return Math.max(rightBorder - leftBorder, 0);
    }

    @Override
    public String toString() {
        String s = "Coeff. value: "+data + " ["+ maxerrorleft +"," + minerrorleft + "," + maxerrorright + "," + minerrorright +"]  RelToParent: " + reltoparent.toString();

        if (leftMostChild != null){
            s+=("\nLeft child: ["+ leftMostChild.toString()+"]");
        }
        if (nextSibling != null){
            s+=("\nSibling: ["+nextSibling.toString()+"]");
        }
        return s;
    }

    /**
     * sets the parent and also computes it's relation to it
     * @param parent
     */
    public void setParent(DataNode parent){
        this.parent = parent;
        if (parent != null){
            if (parent.level > level){
                reltoparent = ancestorRelationship(parent);
            }
        }else {
            reltoparent = Utils.relationship.none;
        }
        if (nextSibling != null){
            nextSibling.setParent(parent);
        }
    }

    /**
     * Method to compute whether this node is in the left or right subtree of given ancestor
     * @param       ancestor
     * @return      the relationship of this node to the given ancestor
     */
    public Utils.relationship ancestorRelationship(DataNode ancestor){
        if ((2 * orderinlevel - 1) < ((2 * ancestor.orderinlevel -1) * Math.pow(2, ancestor.level - this.level))){
            return Utils.relationship.leftChild;
        }else {
            return Utils.relationship.rightChild;
        }
    }

    /**
     * Compare to method based on the maximum absolute error.
     * If two DataNodes have the same maxabserror the level and subsequently order in level get compared instead.
     * This way no two DataNodes in the same SiblinTree should ever be equal!
     *
     * This method specifically compares the error so the DataNodes can be stored in a convenient Sorted Structure based on error
     * (so the nodes with the lowest potential absolute error can be deleted first).
     *
     * @param o     DataNode to compare To
     * @return      -1 if this is smaller, 0 if equal, 1 if bigger than other DataNode (in regards to maximum absolute error)
     */
    @Override
    public int compareTo(@NotNull DataNode o) {
        if (maxabserror == o.maxabserror){
            if (level == o.level){
                return Integer.compare(orderinlevel, o.orderinlevel);
            }
            return Integer.compare(level, o.level);
        }
        return Double.compare(maxabserror, o.maxabserror);
    }

    /**
     * equals method consistent with compareTo() method so no problems arise when a DataNode is put into an ordered map
     * with MA as key.
     *
     * @param obj   Object to compare to
     * @return      true if obejcts are equal
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DataNode){
            DataNode o = (DataNode) obj;
            if (o.level == level && o.orderinlevel == orderinlevel && o.maxabserror == maxabserror){
                return true;
            }
        }
        return false;
    }

}
