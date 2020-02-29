package org.springframework.cloud.consul.cluster;

import com.ecwid.consul.v1.ConsulClient;
import com.netflix.client.config.IClientConfig;
import com.netflix.loadbalancer.ServerList;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.consul.discovery.ConsulDiscoveryProperties;
import org.springframework.cloud.consul.discovery.ConsulServerList;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 自定义的ribbon负载均衡配置，用于覆盖默认ServerList
 */
@Slf4j
@Configuration
public class CustomConsulRibbonClientConfiguration {

  @Autowired
  private ConsulClient consulClient;

  /**
   * 自定义的ConsulServerList
   */
  @Bean
  @ConditionalOnMissingBean
  public ServerList<?> ribbonServerList(IClientConfig config,
      ConsulDiscoveryProperties properties) {
    ConsulServerList serverList = new CustomConsulServerList(consulClient,
        properties);
    serverList.initWithNiwsConfig(config);
    log.info("consul server list: " + serverList);
    return serverList;
  }

}



