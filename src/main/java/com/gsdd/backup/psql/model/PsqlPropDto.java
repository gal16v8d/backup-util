package com.gsdd.backup.psql.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PsqlPropDto {

  private String driverClass;
  private String host;
  private String port;
  private String user;
  private String pass;
  private String dbName;
  private String pgBin;
  private String format;
  private String type;
  private String schema;
  private String tables;
}
