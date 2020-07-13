package co.com.gsdd.backup.psql;

import java.util.ArrayList;
import java.util.List;

import co.com.gsdd.backup.psql.constants.PSQLConstants;
import co.com.gsdd.backup.psql.model.PSQLPropDto;
import co.com.gsdd.dbutil.DBConnection;
import co.com.gsdd.property.util.PropertyUtils;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryPSQL {

    private static final String PSQL_BD_QUERY_PROP = "psql.bd";
    private static final String PSQL_SCHEMA_QUERY_PROP = "psql.schema";
    private static final String QUERY_PROPERTIES = "/query.properties";
    private static final String JDBC_FORMAT = "jdbc:postgresql://%s/%s";

    public void connectDB(PSQLPropDto dto, String currentDb) {
        DBConnection.getInstance().connectDB(dto.getDriverclass(), String.format(JDBC_FORMAT, dto.getHost(), currentDb),
                dto.getUser(), dto.getPass());
    }

    public void disconnectDB(PSQLPropDto dto, String currentDb) {
        DBConnection.getInstance().disconnectDB();
    }

    public List<String> getDatabasesPSQL(PSQLPropDto dto, String currentDb) {
        List<String> dbs = new ArrayList<>();
        try {
            DBConnection.getInstance().setPst(DBConnection.getInstance().getCon().prepareStatement(
                    PropertyUtils.loadPropsFromLocalFile(PSQL_BD_QUERY_PROP, QUERY_PROPERTIES, QueryPSQL.class)));
            DBConnection.getInstance().setRs(DBConnection.getInstance().getPst().executeQuery());
            while (DBConnection.getInstance().getRs().next()) {
                dbs.add(DBConnection.getInstance().getRs().getString(1));
            }
            log.info("dbs -> {}", dbs);
            return dbs;
        } catch (Exception e) {
            log.error(e.toString(), e);
            return null;
        } finally {
            DBConnection.getInstance().closeQuery();
        }
    }

    public List<String> getSchemasPSQL(PSQLPropDto dto, String currentDb) {
        List<String> schemas = new ArrayList<>();
        try {
            DBConnection.getInstance().setPst(DBConnection.getInstance().getCon().prepareStatement(
                    PropertyUtils.loadPropsFromLocalFile(PSQL_SCHEMA_QUERY_PROP, QUERY_PROPERTIES, QueryPSQL.class)));
            DBConnection.getInstance().setRs(DBConnection.getInstance().getPst().executeQuery());
            while (DBConnection.getInstance().getRs().next()) {
                String result = DBConnection.getInstance().getRs().getString(1);
                if (validateSchema(result)) {
                    schemas.add(result);
                }
            }
            log.info("currentdb {} schemas -> {}", currentDb, schemas);
            return schemas;
        } catch (Exception e) {
            log.error(e.toString(), e);
            return null;
        } finally {
            DBConnection.getInstance().closeQuery();
        }
    }

    public boolean validateSchema(String name) {
        return !name.startsWith(PSQLConstants.PG_SCHEMA) && !name.equals(PSQLConstants.INFO_SCHEMA);
    }

}
