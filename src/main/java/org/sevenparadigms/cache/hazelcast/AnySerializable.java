package org.sevenparadigms.cache.hazelcast;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import org.springframework.data.r2dbc.support.FastMethodInvoker;
import org.springframework.data.r2dbc.support.JsonUtils;

import java.io.IOException;
import java.io.Serializable;

public interface AnySerializable extends DataSerializable, Serializable {
    @Override
    default void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeString(JsonUtils.objectToJson(this).toString());
    }

    @Override
    default void readData(ObjectDataInput objectDataInput) throws IOException {
        FastMethodInvoker.copy(JsonUtils.stringToObject(objectDataInput.readString(), this.getClass()), this);
    }
}
