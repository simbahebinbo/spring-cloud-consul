package org.springframework.cloud.consul.cluster;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ListUtil {

  /**
   * 判断 两个链表 是否相同
   */
  public static boolean isSame(@NonNull List<ConsulClientHolder> listOne, @NonNull List<ConsulClientHolder> listTwo) {
    if (listOne.size() != listTwo.size()) {
      return false;
    }

    List<ConsulClientHolder> sortListOne = Optional
        .of(listOne.stream().filter(Objects::nonNull).sorted().collect(Collectors.toList()))
        .orElseGet(Collections::emptyList);
    List<ConsulClientHolder> sortListTwo = Optional
        .of(listTwo.stream().filter(Objects::nonNull).sorted().collect(Collectors.toList()))
        .orElseGet(Collections::emptyList);

    if (sortListOne.size() != sortListTwo.size()) {
      return false;
    }

    int len = (sortListOne.size() + sortListTwo.size()) / 2;
    boolean flag = true;

    for (int i = 0; i < len; i++) {
      ConsulClientHolder clientOne = sortListOne.get(i);
      ConsulClientHolder clientTwo = sortListTwo.get(i);
      if (clientOne.compareTo(clientTwo) != 0) {
        flag = false;
        break;
      }
    }

    return flag;
  }

}

