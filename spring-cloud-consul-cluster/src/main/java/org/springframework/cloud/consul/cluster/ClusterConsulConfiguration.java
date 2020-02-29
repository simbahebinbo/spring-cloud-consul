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

  /**
   * 初始化时要求所有节点的类型
   */
  @Setter
  @Getter
  private String mode;

  @Getter
  private List<String> clusterNodes;

  @PostConstruct
  public void init() {
    if (StringUtils.isEmpty(nodes)) {
      log.error("spring.cloud.consul.cluster.nodes cannot be null");
      throw new BadConfigException("spring.cloud.consul.cluster.nodes cannot be null");
    }

    clusterNodes = Arrays.stream(nodes.split(CommonConstant.SEPARATOR_COMMA)).filter(StringUtils::isNotEmpty)
        .collect(Collectors.toList());

    if (CollectionUtils.isEmpty(clusterNodes)) {
      log.error("spring.cloud.consul.cluster.nodes config error. For example: example.com:8500,192.168.1.1:8080");
      throw new BadConfigException("spring.cloud.consul.cluster.nodes config error.");
    }

    clusterNodes.forEach(clusterNode -> {
      List<String> parts = Arrays.stream(clusterNode.split(CommonConstant.SEPARATOR_COLON)).filter(StringUtils::isNotEmpty)
          .collect(Collectors.toList());
      if (CollectionUtils.isEmpty(parts)) {
        log.error("spring.cloud.consul.cluster.nodes config error. For example: example.com:8500,192.168.1.1:8080");
        throw new BadConfigException("spring.cloud.consul.cluster.nodes config error.");
      }
    });

    if (StringUtils.isEmpty(mode)) {
      mode = NodeModeEnum.ALL.getValue();
    } else {
      if (NodeModeEnum.findByValue(mode) == null) {
        log.error("spring.cloud.consul.cluster.modes config error. For example: client or server or all");
        throw new BadConfigException("spring.cloud.consul.cluster.modes config error.");
      }
    }
  }

  public NodeModeEnum getNodeMode() {
    return NodeModeEnum.findByValue(mode);
  }
}
