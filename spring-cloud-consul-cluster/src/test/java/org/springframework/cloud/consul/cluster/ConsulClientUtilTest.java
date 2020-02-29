package org.springframework.cloud.consul.cluster;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * ConsulClient工具类 单元测试
 */
@Slf4j
public class ConsulClientUtilTest {

  @Test
  public void testChooseClient() {
    String key = "172.16.18.174";
    List<String> clients = Arrays.asList("172.16.18.174:8500", "172.16.94.32:8500",
        "172.16.94.39:8500");
    Set<String> chooses = new HashSet<>();
    for (int i = 0; i < 10000; i++) {
      chooses.add(ConsulClientUtil.chooseClient(key, clients));
    }
    log.info(chooses.toString());
    Assertions.assertEquals(1, chooses.size());
  }
}
