package org.sevenparadigms.cache.hazelcast;

import com.hazelcast.core.Hazelcast;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.data.r2dbc.expression.ExpressionParserCache;
import org.springframework.expression.Expression;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;

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
    static class WithExpression implements AnySerializable {
        Expression exp;
    }

    @Test
    @Order(1)
    public void shouldInitAndEvictByKey() {
        var cache = cacheManager.getCache("test");
        assert cache != null;

        cache.put("key0", model);
        assertThat("Must null", cache.get("key0", WithExpression.class) != null);
        cache.evictIfPresent("key0");
        assertThat("Must null", cache.get("key0", WithExpression.class) == null);

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
    public void shouldMaxSizing() {
        var cache = cacheManager.getCache("test");
        assert cache != null;

        cache.put("key2", model);

        cache.put("key3", model);

        assertThat("Must null", cache.get("key2", WithExpression.class) == null);
    }

    @Test
    @Order(4)
    public void shouldEvictAllAndShutdown() {
        var cache = cacheManager.getCache("test");
        assert cache != null;

        assertThat("Not null", Objects.requireNonNull(cache.get("key3", WithExpression.class)).exp.getExpressionString().equals("a==5"));
        cache.put("key4", model);
        cache.clear();
        assertThat("Must null", Objects.requireNonNull(cache.get("key3")).get() == null);
        assertThat("Must null", Objects.requireNonNull(cache.get("key5")).get() == null);

        Hazelcast.shutdownAll();
    }
}