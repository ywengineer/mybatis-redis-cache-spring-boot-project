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
    public void test(){
        RedisURI uri = RedisURI.create("redis-sentinel://mypassword@127.0.0.1:6379,127.0.0.1:6380/0?timeout=10s#mymaster");
        System.out.println(uri.toString());
    }
}
