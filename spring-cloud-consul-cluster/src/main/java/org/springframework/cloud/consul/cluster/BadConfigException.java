package org.springframework.cloud.consul.cluster;


import lombok.NoArgsConstructor;

@NoArgsConstructor
public class BadConfigException extends RuntimeException {

  public BadConfigException(String message) {
    super(message);
  }
}
