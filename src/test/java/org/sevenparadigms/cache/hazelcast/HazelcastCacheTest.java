package org.sevenparadigms.cache.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hamcrest.MatcherAssert;
import org.junit.Test;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.data.r2dbc.expression.ExpressionParserCache;
import org.springframework.expression.Expression;

import java.io.IOException;
import java.util.Objects;

public class HazelcastCacheTest {
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class WithExpression implements DataSerializable {
        Expression exp;

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(exp.getExpressionString());
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            exp = ExpressionParserCache.INSTANCE.parseExpression(in.readString());
        }
    }

    @Test
    public void shouldInit() {
        var instance = new HazelcastCacheConfiguration().hazelcastInstance(new StandardEnvironment());
        var cache = new HazelcastCacheConfiguration().hazelcastCacheManager(instance);

        var model = new WithExpression(ExpressionParserCache.INSTANCE.parseExpression("a==5"));
        Objects.requireNonNull(cache.getCache("test")).put("key", model);

        var test = Objects.requireNonNull(cache.getCache("test")).get("key", WithExpression.class);
        MatcherAssert.assertThat("Must equals", Objects.requireNonNull(test).exp.getExpressionString().equals("a==5"));

        Hazelcast.shutdownAll();
    }
}