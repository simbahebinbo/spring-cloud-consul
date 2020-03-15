package org.springframework.cloud.consul.cluster;

import java.util.List;
import java.util.Map;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.cloud.consul.ConsulProperties;

/*
 * ConsulClient holder
 */
@Slf4j
public class ConsulClientHolder implements Comparable<ConsulClientHolder> {

  /**
   * 当前ConsulClient的配置
   */
  @Getter
  private final ConsulProperties properties;

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

  public ConsulClientHolder(ConsulProperties properties) {
    super();
    this.properties = properties;
    this.client = ConsulClientUtil.createConsulClient(properties);
    if (ObjectUtils.isNotEmpty(this.client)) {
      log.debug(CommonConstant.LOG_PREFIX + ">>> Cluster ConsulClient[{}] created! <<<", this.getClientId());
      this.checkHealth(); // 创建时做一次健康检测
    } else {
      log.warn(CommonConstant.LOG_PREFIX + ">>> Cluster ConsulClient[{}] cannot create! <<<", this.getClientId());
    }
  }

  public String getClientId() {
    return properties.getHost() + CommonConstant.SEPARATOR_COLON + properties.getPort();
  }

  /**
   * 检测当前ConsulClient的健康状况
   */
  public void checkHealth() {

    boolean tmpHealthy = false;
    if (ObjectUtils.isNotEmpty(this.client)) {
      try {
        Response<Map<String, List<String>>> response = this.client.getCatalogServices(QueryParams.DEFAULT);
        tmpHealthy = !response.getValue().isEmpty();
      } catch (Exception e) {
        log.error(CommonConstant.LOG_PREFIX + ">>> Check consul client health failed : {} <<<",
            e.getMessage());
      }
    }
    this.setHealthy(tmpHealthy);
    log.info(CommonConstant.LOG_PREFIX + ">>> Cluster consul client health check finished: {} <<<", this);
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
    return "{ clientId = " + getClientId() + ", healthy = " + healthy + " }";
  }
}

