import java.util.Properties;
import java.util.function.BiConsumer;

import io.lettuce.core.RedisURI;
import org.junit.Test;

/**
 * Description:
 * <p>
 * User: Mark.Yang
 * Email: ywengineer@gmail.com
 * Date: 2020-05-05
 * Time: 12:07
 */
public class RedisURITests {
    @Test
    public void test() {
        RedisURI uri = RedisURI.create("redis-sentinel://mypassword@127.0.0.1:6379,127.0.0.1:6380/0?timeout=10s#mymaster");
        System.out.println(uri.toString());
        Properties properties = new Properties(System.getProperties());
        System.setProperty("a", "b");
        System.out.println(properties);
    }

    @Test
    public void test2() {
        final Properties properties = new Properties(System.getProperties());
        System.out.println(properties.stringPropertyNames());
        properties.forEach(new BiConsumer<Object, Object>() {
            @Override
            public void accept(Object o, Object o2) {
                System.out.println(o + ": " + o2);
            }
        });
    }
}
