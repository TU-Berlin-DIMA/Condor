package de.tub.dima.condor.flinkScottyConnector.processor.utils;

import org.apache.flink.api.common.state.ValueState;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectStreamException;
import java.io.Serializable;

/**
 * Integer state for Stateful Functions
 */
public class IntegerState implements ValueState<Integer>, Serializable {
    int value;

    @Override
    public Integer value() throws IOException {
        return value;
    }

    @Override
    public void update(Integer value) throws IOException {
        this.value = value;
    }

    @Override
    public void clear() {
        value = 0;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeInt(value);
    }


    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        this.value = in.readInt();
    }

    private void readObjectNoData() throws ObjectStreamException {
        throw new NotSerializableException("Serialization error in class " + this.getClass().getName());
    }
}
