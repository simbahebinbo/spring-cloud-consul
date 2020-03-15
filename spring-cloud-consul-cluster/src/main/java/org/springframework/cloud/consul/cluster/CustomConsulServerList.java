package org.springframework.cloud.consul.cluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.health.model.HealthService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.consul.discovery.ConsulDiscoveryProperties;
import org.springframework.cloud.consul.discovery.ConsulServer;
import org.springframework.cloud.consul.discovery.ConsulServerList;

/**
 * 自定义ConsulServerList
 *
 * 解决微服务在不同consul节点上重复注册导致ServerList结果集重复问题
 */
@Slf4j
public class CustomConsulServerList extends ConsulServerList {

  private final ConsulDiscoveryProperties properties;

  public CustomConsulServerList(ConsulClient client,
      ConsulDiscoveryProperties properties) {
    super(client, properties);
    this.properties = properties;
  }

  @Override
  public List<ConsulServer> getInitialListOfServers() {
    List<ConsulServer> servers = super.getInitialListOfServers();
    log.info(CommonConstant.LOG_PREFIX + ">>> Get initial servers : {} <<<", servers);

    return servers;
  }

  @Override
  public List<ConsulServer> getUpdatedListOfServers() {
    List<ConsulServer> servers = super.getUpdatedListOfServers();
    log.info(CommonConstant.LOG_PREFIX + ">>> Get update servers : {} <<<", servers);

    return servers;
  }

  @Override
  protected List<ConsulServer> transformResponse(List<HealthService> healthServices) {
    Map<String, ConsulServer> servers = new HashMap<>();
    for (HealthService service : healthServices) {
      String instanceId = service.getService().getId();
      ConsulServer server = new ConsulServer(service);
      if (server.getMetadata()
          .containsKey(this.properties.getDefaultZoneMetadataName())) {
        server.setZone(server.getMetadata()
            .get(this.properties.getDefaultZoneMetadataName()));
      }
      servers.putIfAbsent(instanceId, server); // 去重
    }

    return new ArrayList<>(servers.values());
  }

  @Override
  protected ConsulDiscoveryProperties getProperties() {
    return properties;
  }

}
