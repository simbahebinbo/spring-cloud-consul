package org.springframework.cloud.consul.cluster;

import javax.annotation.PostConstruct;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.commons.util.UtilAutoConfiguration;
import org.springframework.cloud.consul.ConditionalOnConsulEnabled;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * consul重试配置
 */
@Slf4j
@Configuration
@ConditionalOnConsulEnabled
@EnableConfigurationProperties
@ConfigurationProperties("spring.cloud.consul.retry")
@AutoConfigureBefore(ClusterConsulBootstrapConfiguration.class)
@Import(UtilAutoConfiguration.class)
public class ConsulRetryConfiguration {

  /**
   * 监测间隔（单位：ms）
   */
  @Setter
  @Getter
  private long initialInterval = 10000L;

  @PostConstruct
  public void init() {
    if (initialInterval <= 0) {
      log.error(CommonConstant.LOG_PREFIX + ">>> spring.cloud.consul.retry should greater than 0 <<<");
      throw new BadConfigException("spring.cloud.consul.retry should greater than 0");
    }
  }
}
