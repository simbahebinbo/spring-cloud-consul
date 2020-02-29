package org.springframework.cloud.consul.cluster;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import com.ecwid.consul.transport.TLSConfig;
import com.ecwid.consul.v1.ConsulClient;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.cloud.consul.ConsulProperties;
import org.springframework.util.Assert;

/**
 * ConsulClient工具类
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ConsulClientUtil {

  private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

  /**
   * 通过一致性算法选择一个由给定key决定的命中节点
   *
   * @param key - 客户端提供的散列key,例如取自客户机的IP
   * @param clients - 在每次调用之前请确保clients的顺序是一致的
   */
  public static <T> T chooseClient(String key, List<T> clients) {
    Assert.hasText(key, "Parameter 'key' must be required!");
    int prime = 31; // always used in hashcode method
    return chooseClient(Hashing.murmur3_128(prime).hashString(key, DEFAULT_CHARSET),
        clients);
  }

  /**
   * 根据一致性算法获取由给定key决定的命中节点
   *
   * @param keyHash - 散列key
   * @param clients - 在每次调用之前请确保clients的顺序是一致的
   */
  public static <T> T chooseClient(HashCode keyHash, List<T> clients) {
    Assert.notNull(keyHash, "Parameter 'keyHash' must be required!");
    if (!CollectionUtils.isEmpty(clients)) {
      final List<T> nodeList = new ArrayList<>(clients);
      int hitIndex = Hashing.consistentHash(keyHash, nodeList.size());
      return clients.get(hitIndex);
    }
    return null;
  }

  /**
   * 创建 ConsulClient, copy from ConsulAutoConfiguration
   */
  public static ConsulClient createConsulClient(ConsulProperties consulProperties) {
    final int agentPort = consulProperties.getPort();
    final String agentHost = StringUtils.isEmpty(consulProperties.getScheme())
        ? consulProperties.getHost()
        : consulProperties.getScheme() + CommonConstant.SEPARATOR_COLON + StringUtils.repeat(CommonConstant.SEPARATOR_VIRGULE, 2) + consulProperties
            .getHost();

    ConsulClient consulClient;

    if (consulProperties.getTls() != null) {
      ConsulProperties.TLSConfig tls = consulProperties.getTls();
      TLSConfig tlsConfig = new TLSConfig(tls.getKeyStoreInstanceType(),
          tls.getCertificatePath(), tls.getCertificatePassword(),
          tls.getKeyStorePath(), tls.getKeyStorePassword());
      log.info("agentHost: " + agentHost + "      agentPort: " + agentPort + "     tlsConfig: " + tlsConfig);
      consulClient = new ConsulClient(agentHost, agentPort, tlsConfig);
    } else {
      log.info("agentHost: " + agentHost + "      agentPort: " + agentPort);
      consulClient = new ConsulClient(agentHost, agentPort);
    }
    return consulClient;
  }
}
