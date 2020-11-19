package de.tub.dima.condor.core.synopsis.Wavelets;
import de.tub.dima.condor.core.synopsis.Synopsis;
import de.tub.dima.condor.core.synopsis.StratifiedSynopsis;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.PriorityQueue;

public class WaveletSynopsis<T> extends StratifiedSynopsis<T> implements Synopsis<T>, Serializable {

    private int size;
    private FrontlineNode frontlineBottom;
    private FrontlineNode frontlineTop;
    private FrontlineNode rootnode;  // only set after the whole data stream is read (in padding)
    private int streamElementCounter;
    private PriorityQueue<DataNode> errorHeap;
    private double data1;

    private WaveletSynopsis<T> combinedWith;

    public int getStreamElementCounter() {
        return streamElementCounter;
    }
//TODO: make sure padding includes data1 when elementsProcessed is odd! (otherwise last input will be forgotten)


    /**
     * WaveletSynopsis constructor - creates the sibling tree with a given space budget (size).
     *
     * @param size denotes the size budget of the WaveletSynopsis structure which equals the maximum amount of Coefficients
     *             the WaveletSynopsis stores at all times.
     */
    public WaveletSynopsis(Integer size) {
        if (size < 2){
            throw new IllegalArgumentException("Wavelet size must be bigger than 1.");
        }
        this.size = size;
        streamElementCounter = 0;

        frontlineBottom = null;
        frontlineTop = null;
        errorHeap = new PriorityQueue<>();
        combinedWith = null;
    }

    @Override
    public void update(T element) {
        if (element instanceof Number) {
            double input = ((Number) element).doubleValue();
            update(input);
        } else {
            throw new IllegalArgumentException("input elements have to be instance of Number!");
        }
    }

    private void update(double element) {
        streamElementCounter++;
        if (streamElementCounter % 2 == 0) {
            double data2 = element;
            climbup(data1, data2);
            if (streamElementCounter > size) {
                discard();  // remove the two DataNodes with smallest MA from the Heap
            }
        } else {
            data1 = element;
        }
    }

    /**
     * perform a simple point query based on the given index
     *
     * @param index
     * @return value of the stream element at given index
     */
    public double pointQuery(int index) {

        if (index > streamElementCounter || index < 0) {
            throw new IllegalArgumentException("Local index must be higher than 0 and lower than " + streamElementCounter + " and given was " + index);
        }
        if (rootnode == null) {
            this.padding();
        }
        return pointQuery(index, rootnode.hungChild, rootnode.value);
    }

    /**
     * private method which recursively traverses the tree to get the approximated value at the given index.
     * This is done by going trough all siblings of current node and choose the one whose subtree contains the given index.
     * By appropriately adding or subtracting the coefficient values the final result is computed.
     *
     * @param index
     * @param parentAverage
     * @return
     */
    private double pointQuery(int index, DataNode current, double parentAverage) {

        double currentAverage = parentAverage;

        while (current.indexInSubtree(index, rootnode.level) == 0) { // loop through all siblings until index is within subtree of node
            current = current.nextSibling;
            if (current == null) {
                return currentAverage; // approximate value defined by parent coefficient
            }
        }

        // current DataNode influences approximation
        if (current.indexInSubtree(index, rootnode.level) == 1) {
            currentAverage += current.data;
        } else {
            currentAverage -= current.data;
        }

        if (current.leftMostChild == null) { // if no descendants exist return the current Average
            return currentAverage;
        }

        return pointQuery(index, current.leftMostChild, currentAverage); // recursively traverse the tree downwards along the descendants
    }

    /**
     * performs a range sum query on the final rooted error-tree.
     *
     * @param leftIndex
     * @param rightIndex
     * @return approximated sum of values between leftIndex and rightIndex
     */
    public double rangeSumQuery(int leftIndex, int rightIndex) {

        if (rightIndex < leftIndex) {
            throw new IllegalArgumentException("rightIndex has to be greater than leftIndex.");
        }
        if (leftIndex > streamElementCounter || leftIndex < 0 || rightIndex > streamElementCounter || rightIndex < 0) {
            throw new IllegalArgumentException("Local index must be higher than 0 and lower than " + streamElementCounter+
                    ", but was[ "+leftIndex+", "+rightIndex+" ]");
        }

        if (rootnode == null) {
            this.padding();
        }

        double rangeSum = (rightIndex - leftIndex + 1) * rootnode.value;

        return rangeQueryTraversal(leftIndex, rightIndex, rootnode.hungChild, rangeSum);
    }

    private double rangeQueryTraversal(int leftIndex, int rightIndex, DataNode current, double ancestorContribution) {

        DataNode onLeftPath = current;
        DataNode onRightPath = current;

        while (onLeftPath.indexInSubtree(leftIndex, rootnode.level) == 0) {
            onLeftPath = onLeftPath.nextSibling;
            if (onLeftPath == null) {
                break;   // finish recursive call ->
            }
        }

        while (onRightPath.indexInSubtree(rightIndex, rootnode.level) == 0) {
            onRightPath = onRightPath.nextSibling;
            if (onRightPath == null) {
                break;
            }
        }

        double leftPathContribution = 0;
        double rightPathContribution = 0;
        if (onLeftPath != null) {
            leftPathContribution = (onLeftPath.countLeftLeaves(leftIndex, rightIndex, rootnode.level) - onLeftPath.countRightLeaves(leftIndex, rightIndex, rootnode.level)) * onLeftPath.data;
        }
        if (onRightPath != null && onRightPath != onLeftPath) {
            rightPathContribution = (onRightPath.countLeftLeaves(leftIndex, rightIndex, rootnode.level) - onRightPath.countRightLeaves(leftIndex, rightIndex, rootnode.level)) * onRightPath.data;
        }

        double currentValue = ancestorContribution + leftPathContribution + rightPathContribution;

        if (onLeftPath != null && onRightPath != null && onLeftPath != onRightPath) {    // left and right path split for the first time -> traverse both left and right path
            if (onLeftPath.leftMostChild != null) {
                currentValue = rangeQueryTraversal(leftIndex, rightIndex, onLeftPath.leftMostChild, currentValue);
            }
            if (onRightPath.leftMostChild != null) {
                currentValue += rangeQueryTraversal(leftIndex, rightIndex, onRightPath.leftMostChild, 0);
            }
        } else {
            if (onLeftPath != null && onLeftPath.leftMostChild != null) { // traverse the left path
                currentValue = rangeQueryTraversal(leftIndex, rightIndex, onLeftPath.leftMostChild, currentValue);
            }
            if (onRightPath != null && onRightPath != onLeftPath && onRightPath.leftMostChild != null) { // traverse the right path if it deviates from the left path
                currentValue = rangeQueryTraversal(leftIndex, rightIndex, onRightPath.leftMostChild, currentValue);
            }
        }

        return currentValue;
    }

    /**
     * method which turns uses average value and level information in the frontline to create additional error-tree nodes
     * that turn the current structure into a rooted (sparse) sibling tree, which may be used to reconstruct any data value.
     */
    public void padding() {
        if (streamElementCounter % 2 == 1) { // make sure to include data1 if odd elements have been added
            update(data1);  // update with a proxy element so wavelet structure can include the single last element - value same as last element as to not discard important coefficients
            streamElementCounter -= 1; // make sure the counter does not include the proxy element
        }

        if (frontlineBottom == frontlineTop) {   // no need for padding -> sibling tree is already rooted
            rootnode = frontlineTop;
        } else {
            int maxLevel = frontlineTop.level + 1;
            double average = 0;
            DataNode previousCoefficient = null;
            boolean firstIteration = true;
            while (frontlineBottom.next != null) { // more frontline nodes exist
                DataNode lowerHanging = frontlineBottom.hungChild;
                DataNode upperHanging = frontlineBottom.next.hungChild;
                average = firstIteration ? (frontlineBottom.value + frontlineBottom.next.value) / 2 : (average + frontlineBottom.next.value) / 2;
                double coefficientValue = frontlineBottom.next.value - average;
                int level = frontlineBottom.next.level + 1;
                int orderInLevel = (int) Math.pow(2, maxLevel - level);
                DataNode newCoefficient = new DataNode(coefficientValue, level, orderInLevel, upperHanging, null);

                if (upperHanging != null) {  // connect left subtree of new coefficient with right subtree
                    upperHanging.front = null;
                    if (previousCoefficient != null) {
                        upperHanging.nextSibling = previousCoefficient;
                        previousCoefficient.previousSibling = upperHanging;
                        previousCoefficient.setParent(newCoefficient);
                    } else if (lowerHanging != null) {
                        upperHanging.nextSibling = lowerHanging;
                        lowerHanging.previousSibling = upperHanging;
                    }
                } else { // left subtree of newCoefficient completely empty (coefficients all deleted)
                    if (previousCoefficient != null) {// if previous coefficient exists set him as child
                        previousCoefficient.setParent(newCoefficient);
                        newCoefficient.leftMostChild = previousCoefficient;
                        previousCoefficient.setParent(newCoefficient);
                    } else if (lowerHanging != null) {// otherwise: set lowerHanging as child
                        lowerHanging.setParent(newCoefficient);
                        newCoefficient.leftMostChild = lowerHanging;
                    }
                }
                if (lowerHanging != null) lowerHanging.front = null;

                previousCoefficient = newCoefficient;
                frontlineBottom = frontlineBottom.next;
                frontlineBottom.prev = null;
                firstIteration = false;
            }
            rootnode = new FrontlineNode(average, maxLevel);
            rootnode.hungChild = previousCoefficient;
            previousCoefficient.front = rootnode;
            if (previousCoefficient.leftMostChild != null) {
                previousCoefficient.leftMostChild.front = null;
            }
        }
    }


    /**
     * Extends the sibling tree structure based on two incoming data elements.
     * Always creates two additional Nodes.
     *
     * @param data1
     * @param data2
     */
    private void climbup(double data1, double data2) {

        FrontlineNode frontlineNode = frontlineBottom;
        FrontlineNode prevFrontlineNode = null;

        int order = streamElementCounter;
        double curentAverage = 0;
        double average = 0;
        int level = 0;
        double value;
        boolean firstLoop = true;

        // loop through the levels from bottom to top and merge the smallest unconnected subtrees until there are a maximum of 1 frontline node per level
        while (order > 0 && order % 2 == 0) {
            DataNode child = null;
            DataNode sibling = null;
            order /= 2;
            level++;

            if (firstLoop) { //first loop / level 0
                average = (data1 + data2) / 2;
                value = data1 - average;
                firstLoop = false;
            } else {
                average = (average + curentAverage) / 2;
                value = curentAverage - average;
                child = prevFrontlineNode.hungChild;
                prevFrontlineNode.hungChild = null;
            }

            if (frontlineNode != null && frontlineNode.level == level) {
                sibling = frontlineNode.hungChild;
                if (sibling != null) {
//                    throw new IllegalArgumentException("Size of wavelet was to small ("+size+") for the number of the already processed elements (")

                    while (sibling.nextSibling != null) {
                        sibling = sibling.nextSibling;          // set s to be the last sibling of the hung child of f
                    }
                }
            }

            DataNode current = new DataNode(value, level, order, child, sibling);   // create new DataNode with computed values and bidirectional references to child and sibling


            current.computeErrorValues(prevFrontlineNode);      // compute the error values for the new DataNode from children and the previous frontline node
            current.computeMA();        // compute the maximum absolute error of the new node
            errorHeap.add(current);     // add the new node to the error Heap structure


            // delete the previous frontline node by removing all of its references
            if (prevFrontlineNode != null) {
                if (child != null) {
                    child.front = null;             // remove the reference of the hung child to the previous frontline node
                }
                if (frontlineNode != null) {
                    frontlineNode.prev = null;      // remove the reference of the frontline node to the previous frontline node
                }
            }

            FrontlineNode newFrontlineNode = frontlineNode;

            if (frontlineNode == null) {     // this is only the case if the new frontline node is the highest frontline-node
                newFrontlineNode = new FrontlineNode(average, level);
                frontlineTop = newFrontlineNode;
                frontlineBottom = newFrontlineNode;
            } else if (frontlineNode.level != level) {   // this is the case when a new frontline node is created but there are still other frontline nodes with higher levels in the structure
                newFrontlineNode = new FrontlineNode(average, level);
                frontlineBottom = newFrontlineNode;
                newFrontlineNode.next = frontlineNode;
                frontlineNode.prev = newFrontlineNode;
            } else {
                curentAverage = frontlineNode.value;
            }

            if (newFrontlineNode.hungChild == null) {       // hang the newly created DataNode to the new Frontline node (only if new frontline node was actually created)
                newFrontlineNode.hungChild = current;
                current.front = newFrontlineNode;
            }
            prevFrontlineNode = frontlineNode;
            frontlineNode = newFrontlineNode.next;
        }
    }

    /**
     * function which discards the two datanodes which incur the least absolute error
     */
    private void discard() {
        for (int i = 0; i < 2; i++) {
            DataNode discarded = errorHeap.poll();
            if (discarded != null) {
                propagateError(discarded);

                if (discarded.leftMostChild != null) {   // handle children / siblings
                    DataNode child = discarded.leftMostChild;

                    while (child != null) { // set all childrens parent to the discarded nodes parent
                        child.setParent(discarded.parent);
                        child = child.nextSibling;
                    }
                    child = discarded.leftMostChild;

                    if (discarded.front != null) {       // hang child on frontline in place of discarded node
                        child.front = discarded.front;
                        discarded.front.hungChild = child;
                    }

                    if (discarded.previousSibling != null) {     // connect child as right sibling of previous sibling of the discarded node
                        discarded.previousSibling.nextSibling = child;
                        child.previousSibling = discarded.previousSibling;
                    }

                    if (discarded.nextSibling != null) {     // connect last sibling of child as left sibling of discarded.next
                        while (child.nextSibling != null) {
                            child = child.nextSibling;
                        }
                        child.nextSibling = discarded.nextSibling;
                        discarded.nextSibling.previousSibling = child;
                    }
                } else {     // no child
                    if (discarded.front != null) {
                        if (discarded.nextSibling != null) {
                            discarded.nextSibling.front = discarded.front;
                        }
                        discarded.front.hungChild = discarded.nextSibling;
                    }
                    if (discarded.previousSibling != null) {
                        discarded.previousSibling.nextSibling = discarded.nextSibling;
                    }
                    if (discarded.nextSibling != null) {
                        discarded.nextSibling.previousSibling = discarded.previousSibling;
                    }
                }
                if (discarded.parent != null && discarded.parent.leftMostChild == discarded) {  // handle parent if discarded is leftmost child of parent
                    if (discarded.leftMostChild != null) {
                        discarded.leftMostChild.parent = discarded.parent;
                        discarded.parent.leftMostChild = discarded.leftMostChild;
                    } else {
                        if (discarded.nextSibling != null) {
                            discarded.nextSibling.parent = discarded.parent;
                        }
                        discarded.parent.leftMostChild = discarded.nextSibling;
                    }
                }
            }
        }
    }

    /**
     * method which takes care of the error propagation when discarding a node
     *
     * @param discarded data node to be discarded
     */
    private void propagateError(DataNode discarded) {

        discarded.minerrorleft -= discarded.data;
        discarded.maxerrorleft -= discarded.data;
        discarded.minerrorright += discarded.data;
        discarded.maxerrorright += discarded.data;


        if (discarded.leftMostChild != null) {
            propagateErrorDown(discarded.leftMostChild, discarded);
        }
        if (discarded.parent == null) {
            double minError = Math.min(discarded.minerrorleft, discarded.minerrorright);
            double maxError = Math.max(discarded.maxerrorleft, discarded.maxerrorright);
            if (discarded.front == null) {       // store/merge error in fnode of ck's leftmost sibling
                DataNode sibling = discarded.previousSibling;
                while (sibling.previousSibling != null) {
                    sibling = sibling.previousSibling;
                }
                sibling.front.mergeError(minError, maxError);
            } else {     // store/merge error in fnode
                discarded.front.mergeError(minError, maxError);
            }
        } else {     // parents exist
            propagateErrorUp(discarded.parent);
        }
    }

    /**
     * propagates error up as long as necessary
     *
     * @param parent
     */
    private void propagateErrorUp(DataNode parent) {
        boolean propagateUpNecessary = true;
        while (propagateUpNecessary && parent != null) {
            propagateUpNecessary = parent.computeErrorValues(null);
            if (propagateUpNecessary) {
                errorHeap.remove(parent);
                parent.computeMA();
                errorHeap.add(parent);
            }
            parent = parent.parent;
        }
    }

    /**
     * propagates error of deleted node down
     *
     * @param descendant descendant of deleted node
     * @param ancestor   node to be deleted
     */
    private void propagateErrorDown(DataNode descendant, DataNode ancestor) {
        errorHeap.remove(descendant);   // remove the descendant from the error heap

        if (descendant.ancestorRelationship(ancestor) == Utils.relationship.leftChild) {     // decrease all error measures in left subtree of ancestor
            descendant.minerrorleft -= ancestor.data;
            descendant.maxerrorleft -= ancestor.data;
            descendant.minerrorright -= ancestor.data;
            descendant.maxerrorright -= ancestor.data;
        } else {      // increase all error measures in right subtree of ancestor
            descendant.minerrorleft += ancestor.data;
            descendant.maxerrorleft += ancestor.data;
            descendant.minerrorright += ancestor.data;
            descendant.maxerrorright += ancestor.data;
        }
        descendant.computeMA();
        errorHeap.add(descendant);      // add descendant to error heap with recomputed value
        if (descendant.leftMostChild != null) {
            propagateErrorDown(descendant.leftMostChild, ancestor);     // propagate error to all children
        }
        if (descendant.nextSibling != null) {
            propagateErrorDown(descendant.nextSibling, ancestor);       // propagate error to all siblings
        }
    }

    public void setCombinedWith(WaveletSynopsis<T> toCombineWith) {
        this.combinedWith = toCombineWith;
    }

    public WaveletSynopsis<T> getCombinedWith() {
        return combinedWith;
    }

    @Override
    public String toString() {
        String s = "streamElementCounter: " + streamElementCounter + "\n";
        if (frontlineBottom == null && rootnode == null) {
            return "The Sibling Tree is empty.";
        } else {
            FrontlineNode current = rootnode == null ? frontlineTop : rootnode;
            while (current != null) {
                s += (current.toString() + ":\n");
                if (current.hungChild != null) {
                    s += (current.hungChild.toString() + "\n");
                }
                s += "----------------------------------------------------------\n";
                current = current.prev;
            }

            return s;
        }
    }


    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeInt(size);
        out.writeObject(frontlineBottom);
        out.writeObject(frontlineTop);
        out.writeObject(rootnode);
        out.writeInt(streamElementCounter);
        out.writeObject(errorHeap);
        out.writeDouble(data1);
        out.writeObject(combinedWith);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        size = in.readInt();
        frontlineBottom = (FrontlineNode) in.readObject();
        frontlineTop = (FrontlineNode) in.readObject();
        rootnode = (FrontlineNode) in.readObject();
        streamElementCounter = in.readInt();
        errorHeap = (PriorityQueue<DataNode>) in.readObject();
        data1 = in.readDouble();
        combinedWith = (WaveletSynopsis<T>) in.readObject();
    }


    private void readObjectNoData() throws ObjectStreamException {
        // no idea what to put here...
    }

}
