package com.gsdd.backup.psql;

import com.gsdd.backup.constants.BackupConstants;
import com.gsdd.backup.psql.constants.PSQLConstants;
import com.gsdd.backup.psql.enums.BackupFormat;
import com.gsdd.backup.psql.enums.BackupType;
import com.gsdd.backup.psql.model.PSQLPropDto;
import com.gsdd.property.util.PropertyUtils;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PSQLBackup {

  public static void main(String[] args) {
    PSQLPropDto psqlDto = new PSQLPropDto();
    psqlDto.setDriverclass(
        PropertyUtils.loadPropsFromLocalFile(
            "backup.bd.driverclass", BackupConstants.CONNECTION_PROPS, PSQLBackup.class));
    psqlDto.setDbName(
        PropertyUtils.loadPropsFromLocalFile(
            "backup.bd.dbname", BackupConstants.CONNECTION_PROPS, PSQLBackup.class));
    psqlDto.setPgbin(
        PropertyUtils.loadPropsFromLocalFile(
            "backup.bd.bindir", BackupConstants.CONNECTION_PROPS, PSQLBackup.class));
    psqlDto.setHost(
        PropertyUtils.loadPropsFromLocalFile(
            "backup.bd.host", BackupConstants.CONNECTION_PROPS, PSQLBackup.class));
    psqlDto.setPort(
        PropertyUtils.loadPropsFromLocalFile(
            "backup.bd.port", BackupConstants.CONNECTION_PROPS, PSQLBackup.class));
    psqlDto.setUser(
        PropertyUtils.loadPropsFromLocalFile(
            "backup.bd.user", BackupConstants.CONNECTION_PROPS, PSQLBackup.class));
    psqlDto.setPass(
        PropertyUtils.loadPropsFromLocalFile(
            "backup.bd.pass", BackupConstants.CONNECTION_PROPS, PSQLBackup.class));
    psqlDto.setFormat(
        PropertyUtils.loadPropsFromLocalFile(
            "backup.bd.format", BackupConstants.CONNECTION_PROPS, PSQLBackup.class));
    psqlDto.setType(
        PropertyUtils.loadPropsFromLocalFile(
            "backup.bd.type", BackupConstants.CONNECTION_PROPS, PSQLBackup.class));
    psqlDto.setSchema(
        PropertyUtils.loadPropsFromLocalFile(
            "backup.bd.schema", BackupConstants.CONNECTION_PROPS, PSQLBackup.class));
    psqlDto.setTables(
        PropertyUtils.loadPropsFromLocalFile(
            "backup.bd.tables", BackupConstants.CONNECTION_PROPS, PSQLBackup.class));
    String outputDir =
        Optional.ofNullable(
                PropertyUtils.loadPropsFromLocalFile(
                    "backup.bd.output", BackupConstants.CONNECTION_PROPS, PSQLBackup.class))
            .orElseGet(() -> System.getProperty("user.home"));
    new PSQLBackup().setUpBackup(psqlDto, outputDir);
  }

  public boolean setUpBackup(PSQLPropDto psqlDto, String outputDir) {
    String[] databases = readPropArray(psqlDto.getDbName());
    String[] schemas = readPropArray(psqlDto.getSchema());
    QueryPSQL queryHelper = new QueryPSQL();
    if (databases.length > 0) {
      queryHelper.connectDB(psqlDto, PSQLConstants.MAIN_DB);
      List<String> validDbs = queryHelper.getDatabasesPSQL();
      iterateDbs(psqlDto, databases, schemas, queryHelper, validDbs, outputDir);
      queryHelper.disconnectDB();
      log.info("Finish execution!");
    } else {
      log.error("No databases found!");
    }
    return true;
  }

  private void iterateDbs(
      PSQLPropDto psqlDto,
      String[] databases,
      String[] schemas,
      QueryPSQL queryHelper,
      List<String> validDbs,
      String outputDir) {
    for (String currentDb : databases) {
      if (!validDbs.contains(currentDb)) {
        log.error("Database '{}' not found/valid", currentDb);
      } else {
        checkAndIterateSchemas(psqlDto, schemas, queryHelper, currentDb, outputDir);
      }
    }
  }

  private void checkAndIterateSchemas(
      PSQLPropDto psqlDto,
      String[] schemas,
      QueryPSQL queryHelper,
      String currentDb,
      String outputDir) {
    if (BackupType.SCHEMA.name().equalsIgnoreCase(psqlDto.getType())
        && Objects.nonNull(psqlDto.getSchema())) {
      List<String> validSchemas = queryHelper.getSchemasPSQL(currentDb);
      for (String schema : schemas) {
        if (!validSchemas.contains(schema)) {
          log.error("Schema '{}' not found/valid", schema);
        } else {
          executeCmd(currentDb, schema, psqlDto, outputDir);
        }
      }
    } else {
      executeCmd(currentDb, null, psqlDto, outputDir);
    }
  }

  private void executeCmd(String db, String schema, PSQLPropDto dto, String outputDir) {
    boolean cmdResult = executeCommand(buildCommand(dto, db, schema, outputDir), dto);
    log.info("Backup for '{}' and schema '{}' was {}", db, schema, cmdResult);
  }

  private String[] readPropArray(String input) {
    return Objects.isNull(input) ? new String[0] : input.split(BackupConstants.SEPARATOR);
  }

  private boolean executeCommand(List<String> commandList, PSQLPropDto psqlDto) {
    log.info("cmd list -> {}", commandList);
    ProcessBuilder pCreate = new ProcessBuilder();
    pCreate.command(commandList);
    pCreate.environment().put(PSQLConstants.PASS_PROMPT, psqlDto.getPass());
    return getResponseFromCmd(pCreate);
  }

  private boolean getResponseFromCmd(ProcessBuilder pCreate) {
    InputStreamReader isr = null;
    BufferedReader br = null;
    Process p = null;
    try {
      pCreate.redirectErrorStream(true);
      p = pCreate.start();
      isr = new InputStreamReader(p.getInputStream());
      br = new BufferedReader(isr);
      getProcessOutput(br);
      return p.exitValue() == 0;
    } catch (Exception e) {
      log.error(e.toString(), e);
      return false;
    } finally {
      closeQuietly(br);
      closeQuietly(isr);
      if (p != null) {
        p.destroy();
      }
    }
  }

  private void closeQuietly(Closeable resource) {
    if (resource != null) {
      try {
        resource.close();
      } catch (IOException e) {
        log.debug("Error while closing resources", e);
      }
    }
  }

  private void getProcessOutput(BufferedReader br) {
    String lineRead;
    try {
      while ((lineRead = br.readLine()) != null) {
        log.info("{}", lineRead.replaceAll("[\r\n]", ""));
      }
    } catch (Exception e) {
      log.error(e.toString(), e);
    }
  }

  public List<String> buildCommand(
      PSQLPropDto psqlDto, String currentDB, String currentSchema, String outputDir) {
    List<String> commandList = new ArrayList<>();
    String backupName = currentDB;
    commandList.add(psqlDto.getPgbin() + PSQLConstants.PG_DUMP);
    commandList.add(PSQLConstants.HOST);
    commandList.add(psqlDto.getHost());
    commandList.add(PSQLConstants.PORT);
    commandList.add(psqlDto.getPort());
    commandList.add(PSQLConstants.USER);
    commandList.add(psqlDto.getUser());
    if (BackupType.SCHEMA.name().equalsIgnoreCase(psqlDto.getType())
        && Objects.nonNull(currentSchema)) {
      commandList.add(PSQLConstants.SCHEMA);
      commandList.add(currentSchema);
      backupName = backupName + BackupConstants.UNDER_SCORE + currentSchema;
    }
    if (BackupType.TABLE.name().equalsIgnoreCase(psqlDto.getType())
        && Objects.nonNull(psqlDto.getTables())) {
      commandList.add(PSQLConstants.INSERTS);
      commandList.add(PSQLConstants.ONLY_DATA);
      String[] tables = psqlDto.getTables().split(BackupConstants.SEPARATOR);
      for (String t : tables) {
        commandList.add(PSQLConstants.TABLE);
        commandList.add(t);
      }
      backupName = backupName + BackupConstants.UNDER_SCORE + BackupType.TABLE.name();
      psqlDto.setFormat(BackupFormat.PLAIN.name());
    }
    if (BackupType.STRUCTURE.name().equalsIgnoreCase(psqlDto.getType())) {
      commandList.add(PSQLConstants.ONLY_STRUCT);
      backupName = backupName + BackupConstants.UNDER_SCORE + BackupType.STRUCTURE.name();
    }
    BackupFormat backupFormat = BackupFormat.valueOf(psqlDto.getFormat());
    commandList.add(PSQLConstants.FORMAT + backupFormat.getCommand());
    commandList.add(PSQLConstants.BIG_OBJECT);
    commandList.add(PSQLConstants.VERBOSE);
    commandList.add(PSQLConstants.FILE);
    commandList.add(
        outputDir
            + File.separator
            + backupName
            + BackupConstants.UNDER_SCORE
            + getDateTimeForBackup()
            + backupFormat.getExtension());
    commandList.add(psqlDto.getDbName());
    return commandList;
  }

  private String getDateTimeForBackup() {
    return Instant.now().toString().replace(':', BackupConstants.UNDER_SCORE_CHAR);
  }
}
