package com.gsdd.backup.psql.enums;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor(access = AccessLevel.PACKAGE)
public enum BackupFormat {

  // @formatter:off
  CUSTOM("c", ".backup"),
  PLAIN("p", ".sql"),
  TAR("t", ".tar");
  // @formatter:on

  private final String command;
  private final String extension;
}
