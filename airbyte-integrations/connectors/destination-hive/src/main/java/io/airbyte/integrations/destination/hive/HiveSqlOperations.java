/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.hive;

import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.integrations.base.JavaBaseConstants;
import io.airbyte.integrations.destination.jdbc.JdbcSqlOperations;
import io.airbyte.integrations.destination.jdbc.SqlOperationsUtils;
import io.airbyte.integrations.destination.jdbc.WriteConfig;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import java.sql.SQLException;
import java.util.List;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class HiveSqlOperations extends JdbcSqlOperations {

    @Setter
    private FileFormat fileFormat = FileFormat.PARQUET;
    @Setter
    private boolean autoMerge = false;
    @Setter
    private String autoMergeParams = null;


    @Override
    public void createSchemaIfNotExists(final JdbcDatabase database, final String schemaName) throws Exception {
        database.execute(String.format("CREATE DATABASE IF NOT EXISTS %s", schemaName));
    }

    @Override
    public boolean isSchemaRequired() {
        return false;
    }

    @Override
    public String createTableQuery(final JdbcDatabase database, final String schemaName, final String tableName) {
        return String.format("""
                CREATE TABLE IF NOT EXISTS `%s`.`%s` (
                  `%s` STRING,
                  `%s` STRING,
                  `%s` TIMESTAMP
                )
                %s
                """,
            schemaName,
            tableName,
            JavaBaseConstants.COLUMN_NAME_AB_ID,
            JavaBaseConstants.COLUMN_NAME_DATA,
            JavaBaseConstants.COLUMN_NAME_EMITTED_AT,
            this.fileFormat.getHiveFormatConfig());
    }

    @Override
    public void executeTransaction(final JdbcDatabase database, final List<String> queries) throws Exception {
        // Note: Hive does not support multi query
        database.execute(connection -> {
            for (final String query : queries) {
                connection.createStatement().execute(query);
            }
        });
    }

    @Override
    public String truncateTableQuery(final JdbcDatabase database, final String schemaName, final String tableName) {
        return String.format("TRUNCATE TABLE `%s`.`%s`", schemaName, tableName);
    }

    @Override
    public String copyTableQuery(final JdbcDatabase database,
        final String schemaName,
        final String srcTableName,
        final String dstTableName) {
        return String.format("INSERT INTO `%s`.`%s` SELECT * FROM `%s`.`%s`",
            schemaName,
            dstTableName,
            schemaName,
            srcTableName);
    }

    @Override
    public void dropTableIfExists(final JdbcDatabase database, final String schemaName, final String tableName)
        throws SQLException {
        database.execute(String.format("DROP TABLE IF EXISTS `%s`.`%s`", schemaName, tableName));
    }

    @Override
    public void insertRecordsInternal(final JdbcDatabase database,
        final List<AirbyteRecordMessage> records,
        final String schemaName,
        final String tmpTableName) throws SQLException {
        log.info("actual size of batch: {}", records.size());

        if (records.isEmpty()) {
            return;
        }

        final String insertQueryComponent = String.format(
            "INSERT INTO `%s`.`%s` (`%s`, `%s`, `%s`) VALUES\n",
            schemaName,
            tmpTableName,
            JavaBaseConstants.COLUMN_NAME_AB_ID,
            JavaBaseConstants.COLUMN_NAME_DATA,
            JavaBaseConstants.COLUMN_NAME_EMITTED_AT);
        final String recordQueryComponent = "(?, ?, ?),\n";
        SqlOperationsUtils.insertRawRecordsInSingleQueryNoSem(insertQueryComponent,
            recordQueryComponent,
            database,
            records);
    }

    @Override
    public void afterDestinationCloseOperations(JdbcDatabase database, List<WriteConfig> writeConfigs) {
        if (!autoMerge) {
            return;
        }

        if (StringUtils.isNotBlank(autoMergeParams)) {
            try {
                for (String param : autoMergeParams.split("&")) {
                    database.execute("SET " + param);
                }
            } catch (Exception e) {
                log.error("Fail in setting merging hive table's properties! " + e.getMessage());
                throw new RuntimeException(e);
            }
        }
        for (WriteConfig writeConfig : writeConfigs) {
            try {
                String outputTableName = "`%s`.`%s`".formatted(
                    writeConfig.getOutputSchemaName(),
                    writeConfig.getOutputTableName());
                database.execute("INSERT OVERWRITE TABLE %s SELECT * FROM %s".formatted(
                    outputTableName, outputTableName));
                log.info("merged table {} by params: {}", outputTableName, autoMergeParams);
            } catch (Exception e) {
                log.error("Fail merging hive table's data files: {}", e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }
    }
}
