package com.linkfun.mybatis.boot;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;

/**
 * Description:
 * <p>
 * User: Mark.Yang
 * Email: ywengineer@gmail.com
 * Date: 2020-05-05
 * Time: 18:04
 */
@Slf4j
public class MybatisCacheConfigEnvironmentProcessor implements EnvironmentPostProcessor, Ordered {
    /**
     * Post-process the given {@code environment}.
     *
     * @param environment the environment to post-process
     * @param application the application to which the environment belongs
     */
    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        log.info("MybatisCacheConfigEnvironmentProcessor executed.");
        System.setProperty("redis.uri", environment.getRequiredProperty("mybatis.cache.redis.uri"));
        System.setProperty("redis.codec", environment.getRequiredProperty("mybatis.cache.redis.codec"));
        System.setProperty("redis.mode", environment.getRequiredProperty("mybatis.cache.redis.mode"));
        System.setProperty("redis.maxTotal", environment.getProperty("mybatis.cache.redis.pool.max-total", "8"));
        System.setProperty("redis.maxIdle", environment.getProperty("mybatis.cache.redis.pool.max-idle", "8"));
        System.setProperty("redis.minIdle", environment.getProperty("mybatis.cache.redis.pool.min-idle", "0"));
    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE - 6;
    }
}
