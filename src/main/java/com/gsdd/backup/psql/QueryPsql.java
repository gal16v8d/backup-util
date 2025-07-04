package com.gsdd.backup.psql;

import com.gsdd.backup.psql.constants.PsqlConstants;
import com.gsdd.backup.psql.model.PsqlPropDto;
import com.gsdd.dbutil.DbConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryPsql {

  private static final String PSQL_BD_QUERY = """
      SELECT datname,datacl
      FROM pg_database
      WHERE datacl IS NULL AND datname <> 'postgres'""";
  private static final String PSQL_SCHEMA_QUERY = "SELECT nspname FROM pg_namespace";
  private static final String JDBC_FORMAT = "jdbc:postgresql://%s/%s";

  private final Predicate<String> validateSchema = name -> !Objects.isNull(name)
      && !name.startsWith(PsqlConstants.PG_SCHEMA) && !name.equals(PsqlConstants.INFO_SCHEMA);

  private final DbConnection db;

  public QueryPsql() {
    db = new DbConnection();
  }

  public void connectDB(PsqlPropDto dto, String currentDb) {
    db.connectDB(
        dto.getDriverClass(),
        String.format(JDBC_FORMAT, dto.getHost(), currentDb),
        dto.getUser(),
        dto.getPass());
  }

  public void disconnectDB() {
    db.disconnectDB();
  }

  public List<String> getDatabasesPSQL() {
    List<String> dbs = new ArrayList<>();
    try {
      db.setPst(db.getCon().prepareStatement(PSQL_BD_QUERY));
      db.setRs(db.getPst().executeQuery());
      while (db.getRs().next()) {
        dbs.add(db.getRs().getString(1));
      }
      log.info("dbs -> {}", dbs);
    } catch (Exception e) {
      log.error(e.toString(), e);
    } finally {
      db.closeQuery();
    }
    return dbs;
  }

  public List<String> getSchemasPSQL(String currentDb) {
    List<String> schemas = new ArrayList<>();
    try {
      db.setPst(db.getCon().prepareStatement(PSQL_SCHEMA_QUERY));
      db.setRs(db.getPst().executeQuery());
      while (db.getRs().next()) {
        String result = db.getRs().getString(1);
        if (validateSchema.test(result)) {
          schemas.add(result);
        }
      }
      log.info("current db {} schemas -> {}", currentDb, schemas);
    } catch (Exception e) {
      log.error(e.toString(), e);
    } finally {
      db.closeQuery();
    }
    return schemas;
  }

}
