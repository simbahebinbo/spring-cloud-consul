package org.springframework.cloud.consul.cluster;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.commons.util.UtilAutoConfiguration;
import org.springframework.cloud.consul.ConditionalOnConsulEnabled;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Slf4j
@Configuration
@ConditionalOnConsulEnabled
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "spring.cloud.consul.cluster")
@AutoConfigureBefore(ClusterConsulBootstrapConfiguration.class)
@Import(UtilAutoConfiguration.class)
public class ClusterConsulConfiguration {

  @Setter
  @Getter
  private String nodes;

  @Getter
  private List<String> clusterNodes;

  @PostConstruct
  public void init() {
    if (StringUtils.isEmpty(this.nodes)) {
      log.error(CommonConstant.LOG_PREFIX + ">>> spring.cloud.consul.cluster.nodes cannot be null <<<");
      throw new BadConfigException("spring.cloud.consul.cluster.nodes cannot be null");
    }

    this.clusterNodes = Arrays.stream(this.nodes.split(CommonConstant.SEPARATOR_COMMA)).filter(StringUtils::isNotEmpty)
        .collect(Collectors.toList());

    if (CollectionUtils.isEmpty(this.clusterNodes)) {
      log.error(CommonConstant.LOG_PREFIX + ">>> spring.cloud.consul.cluster.nodes config error. For example: example.com:8500,192.168.1.1:8080 <<<");
      throw new BadConfigException("spring.cloud.consul.cluster.nodes config error.");
    }

    this.clusterNodes.forEach(clusterNode -> {
      List<String> parts = Arrays.stream(clusterNode.split(CommonConstant.SEPARATOR_COLON)).filter(StringUtils::isNotEmpty)
          .collect(Collectors.toList());
      if (CollectionUtils.isEmpty(parts)) {
        log.error(CommonConstant.LOG_PREFIX + ">>> spring.cloud.consul.cluster.nodes config error. For example: example.com:8500,192.168.1.1:8080 <<<");
        throw new BadConfigException("spring.cloud.consul.cluster.nodes config error.");
      }
    });
  }
}
