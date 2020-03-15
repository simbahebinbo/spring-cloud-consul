package org.springframework.cloud.consul.cluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ecwid.consul.v1.ConsulClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.consul.discovery.ConsulDiscoveryClient;
import org.springframework.cloud.consul.discovery.ConsulDiscoveryProperties;

/**
 * 自定义的ConsulDiscoveryClient
 *
 * 解决微服务在不同consul节点上重复注册导致getInstances方法返回的结果集重复问题
 */
@Slf4j
public class CustomConsulDiscoveryClient extends ConsulDiscoveryClient {

  public CustomConsulDiscoveryClient(ConsulClient client,
      ConsulDiscoveryProperties properties) {
    super(client, properties);
  }

  /**
   * 重写getInstances方法去重
   */
  @Override
  public List<ServiceInstance> getInstances(String serviceId) {
    List<ServiceInstance> instances = super.getInstances(serviceId);
    log.info(CommonConstant.LOG_PREFIX + ">>> Before distinct:  Get instances of service({}) from consul : {} <<<", serviceId,
        instances);
    Map<String, ServiceInstance> filteredInstances = new HashMap<>();
    for (ServiceInstance instance : instances) { // 去重
      filteredInstances.putIfAbsent(instance.getInstanceId(), instance);
    }
    instances = new ArrayList<>(filteredInstances.values());
    log.info(CommonConstant.LOG_PREFIX + ">>> After distinct:  Get instances of service({}) from consul : {} <<<", serviceId,
        instances);

    return instances;
  }
}
