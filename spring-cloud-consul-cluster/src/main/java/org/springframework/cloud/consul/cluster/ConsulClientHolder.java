package org.springframework.cloud.consul.cluster;

import java.util.List;
import java.util.Map;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/*
 * ConsulClient holder
 */
@Slf4j
public class ConsulClientHolder implements Comparable<ConsulClientHolder> {

  /**
   * 当前ConsulClient的配置
   */
  @Getter
  private final ClusterConsulProperties properties;

  /**
   * Consul客户端
   */
  @Getter
  private final ConsulClient client;

  /**
   * 当前ConsulClient是否是健康的
   */
  @Getter
  @Setter
  private boolean healthy = true;

  /**
   * 是否为主客户端
   */
  @Getter
  @Setter
  private boolean isPrimary = false;

  public ConsulClientHolder(ClusterConsulProperties properties) {
    super();
    this.properties = properties;
    this.client = ConsulClientUtil.createConsulClient(properties);
    log.info("Cluster ConsulClient[{}] created!", this.getClientId());
    this.checkHealth(); // 创建时做一次健康检测
  }

  public String getClientId() {
    return properties.getHost() + CommonConstant.SEPARATOR_COLON + properties.getPort();
  }

  /**
   * 检测当前ConsulClient的健康状况
   */
  public void checkHealth() {
    String aclToken = properties.getAclToken();

    boolean tmpHealthy = false;
    try {
      Response<Map<String, List<String>>> response;
      if (!StringUtils.isEmpty(aclToken)) {
        response = client.getCatalogServices(QueryParams.DEFAULT, aclToken);
      } else {
        response = client.getCatalogServices(QueryParams.DEFAULT);
      }
      tmpHealthy = !response.getValue().isEmpty();
    } catch (Exception e) {
      log.error("Check consul client health failed : {}",
          e.getMessage());
    }
    this.setHealthy(tmpHealthy);
    log.info("Cluster consul client healthcheck finished: {}", this);
  }

  /**
   * 排序，保证服务分布式部署时的全局hash一致性
   */
  @Override
  public int compareTo(ConsulClientHolder o) {
    return getClientId().compareTo(o.getClientId());
  }

  @Override
  public String toString() {
    return "{ clientId = " + getClientId() + ", healthy = " + healthy
        + ", isPrimary = " + isPrimary + " }";
  }

}
