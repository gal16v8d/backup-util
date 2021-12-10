package com.gsdd.backup.constants;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class BackupConstants {

  public static final String CONNECTION_PROPS = "/connection.properties";
  public static final char UNDER_SCORE_CHAR = '_';
  public static final String UNDER_SCORE = String.valueOf(UNDER_SCORE_CHAR);
  public static final String SEPARATOR = ",";
}
