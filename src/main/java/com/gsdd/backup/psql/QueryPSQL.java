package com.gsdd.backup.psql;

import com.gsdd.backup.psql.constants.PSQLConstants;
import com.gsdd.backup.psql.model.PSQLPropDto;
import com.gsdd.dbutil.DBConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryPSQL {

  private static final String PSQL_BD_QUERY =
      "SELECT datname,datacl " + "FROM pg_database WHERE datacl IS NULL AND datname <> 'postgres'";
  private static final String PSQL_SCHEMA_QUERY = "SELECT nspname FROM pg_namespace";
  private static final String JDBC_FORMAT = "jdbc:postgresql://%s/%s";

  Predicate<String> validateSchema = name -> !Objects.isNull(name)
      && !name.startsWith(PSQLConstants.PG_SCHEMA) && !name.equals(PSQLConstants.INFO_SCHEMA);

  public void connectDB(PSQLPropDto dto, String currentDb) {
    DBConnection.getInstance()
        .connectDB(
            dto.getDriverclass(),
            String.format(JDBC_FORMAT, dto.getHost(), currentDb),
            dto.getUser(),
            dto.getPass());
  }

  public void disconnectDB() {
    DBConnection.getInstance().disconnectDB();
  }

  public List<String> getDatabasesPSQL() {
    List<String> dbs = new ArrayList<>();
    try {
      DBConnection.getInstance()
          .setPst(DBConnection.getInstance().getCon().prepareStatement(PSQL_BD_QUERY));
      DBConnection.getInstance().setRs(DBConnection.getInstance().getPst().executeQuery());
      while (DBConnection.getInstance().getRs().next()) {
        dbs.add(DBConnection.getInstance().getRs().getString(1));
      }
      log.info("dbs -> {}", dbs);
    } catch (Exception e) {
      log.error(e.toString(), e);
    } finally {
      DBConnection.getInstance().closeQuery();
    }
    return dbs;
  }

  public List<String> getSchemasPSQL(String currentDb) {
    List<String> schemas = new ArrayList<>();
    try {
      DBConnection.getInstance()
          .setPst(DBConnection.getInstance().getCon().prepareStatement(PSQL_SCHEMA_QUERY));
      DBConnection.getInstance().setRs(DBConnection.getInstance().getPst().executeQuery());
      while (DBConnection.getInstance().getRs().next()) {
        String result = DBConnection.getInstance().getRs().getString(1);
        if (validateSchema.test(result)) {
          schemas.add(result);
        }
      }
      log.info("currentdb {} schemas -> {}", currentDb, schemas);
    } catch (Exception e) {
      log.error(e.toString(), e);
    } finally {
      DBConnection.getInstance().closeQuery();
    }
    return schemas;
  }

}
