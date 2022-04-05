package org.sevenparadigms.cache.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.data.r2dbc.expression.ExpressionParserCache;
import org.springframework.expression.Expression;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.IOException;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = HazelcastCacheConfiguration.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestPropertySource(properties = {
        "spring.cache.test.expireAfterAccess=1",
        "spring.cache.test.maximumSize=1"})
public class HazelcastCacheTest {
    @Autowired
    CacheManager cacheManager;

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
            exp = ExpressionParserCache.INSTANCE.parseExpression(Objects.requireNonNull(in.readString()));
        }
    }

    @Test
    @Order(1)
    public void shouldInit() {
        var model = new WithExpression(ExpressionParserCache.INSTANCE.parseExpression("a==5"));
        Objects.requireNonNull(cacheManager.getCache("test")).put("key", model);

        var test = Objects.requireNonNull(cacheManager.getCache("test")).get("key", WithExpression.class);
        assertThat("Must equals", Objects.requireNonNull(test).exp.getExpressionString().equals("a==5"));
    }

    @Test
    @Order(2)
    public void shouldExpireAndSizing() throws InterruptedException {
        Thread.sleep(1000);

        var test = Objects.requireNonNull(cacheManager.getCache("test")).get("key", WithExpression.class);
        assertThat("Must null", test == null);
    }

    @Test
    @Order(3)
    public void shouldEvictAndShutdown() {
        Objects.requireNonNull(cacheManager.getCache("test")).clear();
        Hazelcast.shutdownAll();
    }
}