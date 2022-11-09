/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.hive;

import static io.airbyte.db.jdbc.JdbcUtils.HOST_KEY;
import static io.airbyte.db.jdbc.JdbcUtils.PASSWORD_KEY;
import static io.airbyte.db.jdbc.JdbcUtils.PORT_KEY;
import static io.airbyte.db.jdbc.JdbcUtils.USERNAME_KEY;
import static io.airbyte.integrations.destination.hive.HiveConsts.getTextConfigOrEmpty;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.commons.io.IOs;
import io.airbyte.commons.json.Jsons;
import io.airbyte.db.Database;
import io.airbyte.db.factory.DSLContextFactory;
import io.airbyte.db.factory.DatabaseDriver;
import io.airbyte.db.jdbc.JdbcUtils;
import io.airbyte.integrations.base.JavaBaseConstants;
import io.airbyte.integrations.standardtest.destination.JdbcDestinationAcceptanceTest;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.jooq.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveDestinationAcceptanceTest extends JdbcDestinationAcceptanceTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveDestinationAcceptanceTest.class);


    private static final String SECRET_FILE_PATH = "secrets/config.json";
    private final HiveSqlNameTransformer hiveSqlNameTransformer = new HiveSqlNameTransformer();
    private final JsonNode configJson = Jsons.deserialize(IOs.readFile(Path.of(SECRET_FILE_PATH)));

    @Override
    protected String getImageName() {
        return "airbyte/destination-hive:dev";
    }

    @Override
    protected JsonNode getConfig() {
        return configJson;
    }

    @Override
    protected JsonNode getFailCheckConfig() {
        final JsonNode failCheckJson = Jsons.jsonNode(Collections.emptyMap());
        // invalid credential
        ((ObjectNode) failCheckJson).put("host", "fake-host");
        ((ObjectNode) failCheckJson).put("port", 10000);
        return failCheckJson;
    }

    @Override
    protected List<JsonNode> retrieveRecords(TestDestinationEnv testEnv,
        String streamName,
        String namespace,
        JsonNode streamSchema)
        throws Exception {
        String realTableName = hiveSqlNameTransformer.getRawTableName(streamName);
        return getDatabase()
            .query(ctx -> ctx
                .fetch(String.format("SELECT * FROM `%s`.`%s` ORDER BY `%s` ASC",
                    getDatabaseName(namespace),
                    realTableName,
                    JavaBaseConstants.COLUMN_NAME_EMITTED_AT)))
            .stream()
            .map(row -> getJsonFromRawRecord(row, realTableName))
            .toList();
    }

    private JsonNode getJsonFromRawRecord(Record record, String realTableName) {
        String hiveFieldName = realTableName + "." + JavaBaseConstants.COLUMN_NAME_DATA;
        Object data;
        if (record.indexOf(JavaBaseConstants.COLUMN_NAME_DATA) >= 0) {
            data = record.get(JavaBaseConstants.COLUMN_NAME_DATA);
        } else if (record.indexOf(hiveFieldName) >= 0) {
            data = record.get(hiveFieldName);
        } else {
            return Jsons.jsonNode(Map.of());
        }
        return Jsons.deserialize(data.toString()
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\\u", "\\\\u")
        );
    }

    protected String getDatabaseName(String namespace) {
        if (StringUtils.isNotBlank(namespace)) {
            return hiveSqlNameTransformer.getIdentifier(namespace);
        } else {
            return configJson.get(JdbcUtils.DATABASE_KEY).asText("default");
        }
    }

    protected Database getDatabase() {
        return new Database(
            DSLContextFactory.create(
                getTextConfigOrEmpty(configJson, USERNAME_KEY),
                getTextConfigOrEmpty(configJson, PASSWORD_KEY),
                DatabaseDriver.HIVE.getDriverClassName(),
                String.format(DatabaseDriver.HIVE.getUrlFormatString(),
                    configJson.get(HOST_KEY).asText(),
                    configJson.get(PORT_KEY).asInt(),
                    ""),
                null,
                Map.of()));
    }

    @Override
    protected void setup(TestDestinationEnv testEnv) {
    }

    @Override
    protected void tearDown(TestDestinationEnv testEnv) {
    }
}
