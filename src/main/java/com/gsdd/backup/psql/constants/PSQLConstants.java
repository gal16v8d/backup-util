package com.gsdd.backup.psql.constants;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class PSQLConstants {

  public static final String PG_DUMP = "pg_dump";
  public static final String MAIN_DB = "postgres";

  public static final String HOST = "-h";
  public static final String PORT = "-p";
  public static final String USER = "-U";
  public static final String SCHEMA = "-n";
  public static final String FORMAT = "-F";
  public static final String BIG_OBJECT = "-b";
  public static final String VERBOSE = "-v";
  public static final String FILE = "-f";
  public static final String ONLY_STRUCT = "-s";
  public static final String INSERTS = "--inserts";
  public static final String ONLY_DATA = "-a";
  public static final String TABLE = "-t";

  public static final String PASS_PROMPT = "PGPASSWORD";

  public static final String PG_SCHEMA = "pg_";
  public static final String INFO_SCHEMA = "information_schema";

}
