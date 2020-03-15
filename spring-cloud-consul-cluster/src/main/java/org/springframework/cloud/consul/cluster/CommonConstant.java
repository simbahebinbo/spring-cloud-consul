package org.springframework.cloud.consul.cluster;


import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CommonConstant {

  public static final String UTF8 = "UTF-8";

  /**
   * 分隔符
   */
  public static final String SEPARATOR_SEMICOLON = ";";

  public static final String SEPARATOR_QUESTION_MARK = "?";

  public static final String SEPARATOR_COLON = ":";

  public static final String SEPARATOR_COLON2 = "::";

  public static final String SEPARATOR_COMMA = ",";

  //句号
  public static final String SEPARATOR_PERIOD = ".";

  public static final String SEPARATOR_AMPERSAND = "&";

  public static final String SEPARATOR_EQUAL_SIGN = "=";

  public static final String SEPARATOR_BLANK = "";

  public static final String SEPARATOR_ASTERISK = "*";


  /**
   * 斜线
   */
  public static final String SEPARATOR_VIRGULE = "/";

  /**
   * 竖线
   */
  public static final String SEPARATOR_VERTICAL_LINE = "|";

  /**
   * 下划线
   */
  public static final String SEPARATOR_UNDERSCORE = "_";

  /**
   * 连字符 中横线
   */
  public static final String SEPARATOR_HYPHEN = "-";

  //日志前缀
  public static final String LOG_PREFIX = "spring cloud consul cluster:   ";
}
