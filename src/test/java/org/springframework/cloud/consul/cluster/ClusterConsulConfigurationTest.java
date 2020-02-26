package org.springframework.cloud.consul.cluster;

import java.util.Objects;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@Slf4j
@ExtendWith(SpringExtension.class)
@ActiveProfiles({"test"})
@DirtiesContext(classMode = ClassMode.AFTER_EACH_TEST_METHOD)
@SpringBootTest(classes = ClusterConsulConfiguration.class)
@EnableConfigurationProperties
public class ClusterConsulConfigurationTest {

  @Autowired
  private ClusterConsulConfiguration config;

  @Test
  public void testAll() {
    Assertions.assertTrue(Objects.nonNull(config));
    Assertions.assertEquals(2, config.getClusterNodes().size());
    Assertions.assertEquals(NodeModeEnum.CLIENT, config.getNodeMode());
  }
}