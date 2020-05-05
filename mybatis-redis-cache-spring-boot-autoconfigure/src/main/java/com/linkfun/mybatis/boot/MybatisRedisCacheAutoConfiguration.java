package com.linkfun.mybatis.boot;

import com.linkfun.mybatis.boot.properties.MybatisRedisCacheProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Description:
 * <p>
 * User: Mark.Yang
 * Email: ywengineer@gmail.com
 * Date: 2020-05-05
 * Time: 13:10
 */
@EnableConfigurationProperties({MybatisRedisCacheProperties.class})
@Configuration
public class MybatisRedisCacheAutoConfiguration {
}
