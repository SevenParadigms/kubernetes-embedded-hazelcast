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
        "spring.cache.test.expireAfterAccess=200",
        "spring.cache.test.maximumSize=1"})
public class HazelcastCacheTest {
    @Autowired
    CacheManager cacheManager;

    WithExpression model = new WithExpression(ExpressionParserCache.INSTANCE.parseExpression("a==5"));

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
        var cache = cacheManager.getCache("test");
        assert cache != null;

        cache.put("key1", model);

        var test = cache.get("key1", WithExpression.class);
        assertThat("Must equals", Objects.requireNonNull(test).exp.getExpressionString().equals("a==5"));
    }

    @Test
    @Order(2)
    public void shouldExpire() throws InterruptedException {
        var cache = cacheManager.getCache("test");
        assert cache != null;

        assertThat("Must null", cache.get("key1", WithExpression.class) != null);

        Thread.sleep(210);

        assertThat("Must null", cache.get("key1", WithExpression.class) == null);
    }

    @Test
    @Order(3)
    public void shouldMaxSizing() throws InterruptedException {
        var cache = cacheManager.getCache("test");
        assert cache != null;

        cache.put("key2", model);
        Thread.sleep(100);
        cache.put("key3", model);

        assertThat("Must null", cache.get("key2", WithExpression.class) == null);
    }

    @Test
    @Order(4)
    public void shouldEvictAndShutdown() {
        var cache = cacheManager.getCache("test");
        assert cache != null;

        assertThat("Not null", Objects.requireNonNull(cache.get("key3", WithExpression.class)).exp.getExpressionString().equals("a==5"));
        cache.clear();
        assertThat("Must null", Objects.requireNonNull(cache.get("key3")).get() == null);

        Hazelcast.shutdownAll();
    }
}