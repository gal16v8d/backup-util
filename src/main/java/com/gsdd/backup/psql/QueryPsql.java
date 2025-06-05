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

  public void connectDB(PsqlPropDto dto, String currentDb) {
    DbConnection.getInstance()
        .connectDB(
            dto.getDriverClass(),
            String.format(JDBC_FORMAT, dto.getHost(), currentDb),
            dto.getUser(),
            dto.getPass());
  }

  public void disconnectDB() {
    DbConnection.getInstance().disconnectDB();
  }

  public List<String> getDatabasesPSQL() {
    List<String> dbs = new ArrayList<>();
    try {
      DbConnection.getInstance()
          .setPst(DbConnection.getInstance().getCon().prepareStatement(PSQL_BD_QUERY));
      DbConnection.getInstance().setRs(DbConnection.getInstance().getPst().executeQuery());
      while (DbConnection.getInstance().getRs().next()) {
        dbs.add(DbConnection.getInstance().getRs().getString(1));
      }
      log.info("dbs -> {}", dbs);
    } catch (Exception e) {
      log.error(e.toString(), e);
    } finally {
      DbConnection.getInstance().closeQuery();
    }
    return dbs;
  }

  public List<String> getSchemasPSQL(String currentDb) {
    List<String> schemas = new ArrayList<>();
    try {
      DbConnection.getInstance()
          .setPst(DbConnection.getInstance().getCon().prepareStatement(PSQL_SCHEMA_QUERY));
      DbConnection.getInstance().setRs(DbConnection.getInstance().getPst().executeQuery());
      while (DbConnection.getInstance().getRs().next()) {
        String result = DbConnection.getInstance().getRs().getString(1);
        if (validateSchema.test(result)) {
          schemas.add(result);
        }
      }
      log.info("currentdb {} schemas -> {}", currentDb, schemas);
    } catch (Exception e) {
      log.error(e.toString(), e);
    } finally {
      DbConnection.getInstance().closeQuery();
    }
    return schemas;
  }

}
