/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.hive;

import static io.airbyte.integrations.destination.hive.HiveConsts.AUTO_MERGE_CONFIG_KEY;
import static io.airbyte.integrations.destination.hive.HiveConsts.AUTO_MERGE_PARAMS_CONFIG_KEY;
import static io.airbyte.integrations.destination.hive.HiveConsts.FILE_FORMAT_CONFIG_KEY;
import static io.airbyte.integrations.destination.hive.HiveConsts.HIVE_DRIVER;
import static io.airbyte.integrations.destination.hive.HiveConsts.getTextConfigOrEmpty;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.factory.DataSourceFactory;
import io.airbyte.db.factory.DatabaseDriver;
import io.airbyte.db.jdbc.JdbcDatabase;
import io.airbyte.db.jdbc.JdbcUtils;
import io.airbyte.integrations.base.AirbyteMessageConsumer;
import io.airbyte.integrations.base.Destination;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.destination.jdbc.AbstractJdbcDestination;
import io.airbyte.integrations.destination.jdbc.JdbcBufferedConsumerFactory;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteConnectionStatus.Status;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import java.util.Map;
import java.util.function.Consumer;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HiveDestination extends AbstractJdbcDestination implements Destination {

    public HiveDestination() {
        super(HIVE_DRIVER, new HiveSqlNameTransformer(), new HiveSqlOperations());
    }

    @Override
    protected Map<String, String> getDefaultConnectionProperties(JsonNode config) {
        return Map.of();
    }

    @Override
    public AirbyteConnectionStatus check(final JsonNode config) {
        final DataSource dataSource = getDataSource(config);

        try {
            final JdbcDatabase database = getDatabase(dataSource);
            final String outputSchema = getNamingResolver().getIdentifier(config.get(JdbcUtils.DATABASE_KEY).asText());
            attemptSQLCreateAndDropTableOperations(outputSchema, database, getNamingResolver(), getSqlOperations());
            return new AirbyteConnectionStatus().withStatus(Status.SUCCEEDED);
        } catch (final Exception e) {
            log.error("Exception while checking connection: ", e);
            return new AirbyteConnectionStatus()
                .withStatus(Status.FAILED)
                .withMessage("Could not connect with provided configuration. \n" + e.getMessage());
        } finally {
            try {
                DataSourceFactory.close(dataSource);
            } catch (final Exception e) {
                log.warn("Unable to close data source.", e);
            }
        }
    }

    @Override
    public JsonNode toJdbcConfig(JsonNode config) {
        Builder<Object, Object> configBuilder = ImmutableMap.builder()
            .put(JdbcUtils.JDBC_URL_KEY, DatabaseDriver.HIVE.getUrlFormatString().formatted(
                config.get(JdbcUtils.HOST_KEY).asText(),
                config.get(JdbcUtils.PORT_KEY).asInt(),
                ""
            ))
            .put(JdbcUtils.USERNAME_KEY, getTextConfigOrEmpty(config, JdbcUtils.USERNAME_KEY))
            .put(JdbcUtils.PASSWORD_KEY, getTextConfigOrEmpty(config, JdbcUtils.PASSWORD_KEY));
        if (config.has(JdbcUtils.JDBC_URL_PARAMS_KEY)) {
            configBuilder.put(JdbcUtils.JDBC_URL_PARAMS_KEY, config.get(JdbcUtils.JDBC_URL_PARAMS_KEY));
        }
        return Jsons.jsonNode(configBuilder.build());
    }

    @Override
    public AirbyteMessageConsumer getConsumer(final JsonNode config,
        final ConfiguredAirbyteCatalog catalog,
        final Consumer<AirbyteMessage> outputRecordCollector) {
        HiveSqlOperations sqlOperations = (HiveSqlOperations) getSqlOperations();
        if (config.has(FILE_FORMAT_CONFIG_KEY)) {
            String fileFormatStr = config.get(FILE_FORMAT_CONFIG_KEY).asText();
            sqlOperations.setFileFormat(FileFormat.parse(fileFormatStr));
        }
        if (config.has(AUTO_MERGE_CONFIG_KEY) && config.get(AUTO_MERGE_CONFIG_KEY).asBoolean(false)) {
            //enabled auto merge table's data files in close
            sqlOperations.setAutoMerge(true);
            sqlOperations.setAutoMergeParams(config.get(AUTO_MERGE_PARAMS_CONFIG_KEY).asText());
        }
        return JdbcBufferedConsumerFactory.create(outputRecordCollector,
            getDatabase(getDataSource(config)),
            sqlOperations,
            getNamingResolver(),
            config,
            catalog);
    }

    public static void main(String[] args) throws Exception {
        log.info("starting destination: {}", HiveDestination.class);
        new IntegrationRunner(new HiveDestination()).run(args);
        log.info("completed destination: {}", HiveDestination.class);
    }

}
