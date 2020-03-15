package org.springframework.cloud.consul.cluster;

import com.ecwid.consul.v1.ConsulClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.commons.util.InetUtils;
import org.springframework.cloud.commons.util.InetUtils.HostInfo;
import org.springframework.cloud.commons.util.UtilAutoConfiguration;
import org.springframework.cloud.consul.ConditionalOnConsulEnabled;
import org.springframework.cloud.consul.ConsulProperties;
import org.springframework.cloud.consul.config.ConsulConfigBootstrapConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * 默认的ConsulClient启动配置,
 *
 * 用于覆盖@Import(ConsulAutoConfiguration.class)注册的ConsulClient
 */

@Slf4j
@Configuration
@ConditionalOnConsulEnabled
@EnableConfigurationProperties
@Import(UtilAutoConfiguration.class)
@AutoConfigureBefore(ConsulConfigBootstrapConfiguration.class)
public class ClusterConsulBootstrapConfiguration {

  @Autowired
  private InetUtils inetUtils;

  @Autowired
  private ClusterConsulConfiguration clusterConsulConfiguration;

  @Bean
  @ConditionalOnMissingBean
  public ConsulProperties consulProperties() {
    ClusterConsulProperties clusterConsulProperties = new ClusterConsulProperties();
    HostInfo hostInfo = inetUtils.findFirstNonLoopbackHostInfo();
    clusterConsulProperties.setClusterClientKey(hostInfo.getIpAddress());
    clusterConsulProperties.setClusterNodes(clusterConsulConfiguration.getClusterNodes());

    return clusterConsulProperties;
  }

  @Bean
  @ConditionalOnMissingBean
  public ConsulClient consulClient(ConsulProperties consulProperties) {
    ClusterConsulClient clusterConsulClient = new ClusterConsulClient((ClusterConsulProperties) consulProperties);

    log.info("spring cloud consul cluster: >>> Default ConsulClient created : {}, with config properties : {} <<<",
        clusterConsulClient, consulProperties);
    return clusterConsulClient;
  }
}


