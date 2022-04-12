package org.sevenparadigms.cache.hazelcast;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import org.apache.commons.codec.digest.MurmurHash2;
import org.springframework.data.r2dbc.support.FastMethodInvoker;
import org.springframework.data.r2dbc.support.JsonUtils;

import java.io.IOException;
import java.io.Serializable;

interface AnySerializable extends DataSerializable, Serializable {
    @Override
    default void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        var input = JsonUtils.objectToJson(this).toString();
        var hash = this.getClass().getSimpleName() + MurmurHash2.hash64(input);
        HazelcastCacheManager.getSerializeCache().put(hash, this);
        objectDataOutput.writeString(input);
    }

    @Override
    default void readData(ObjectDataInput objectDataInput) throws IOException {
        var input = objectDataInput.readString();
        var hash = this.getClass().getSimpleName() + MurmurHash2.hash64(input);
        var value = HazelcastCacheManager.getSerializeCache().get(hash, this.getClass());
        if (value == null) {
            value = JsonUtils.stringToObject(input, this.getClass());
            HazelcastCacheManager.getSerializeCache().put(hash, value);
        }
        FastMethodInvoker.copy(value, this);
    }
}
