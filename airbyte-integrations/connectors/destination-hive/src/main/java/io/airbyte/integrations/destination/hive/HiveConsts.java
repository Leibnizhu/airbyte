package io.airbyte.integrations.destination.hive;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.db.factory.DatabaseDriver;

/**
 * @author Leibniz on 2022/11/9.
 */
public class HiveConsts {

    public static final String HIVE_DRIVER = DatabaseDriver.HIVE.getDriverClassName();
    public static final String FILE_FORMAT_CONFIG_KEY = "file_format";
    public static final String AUTO_MERGE_CONFIG_KEY = "auto_merge";
    public static final String AUTO_MERGE_PARAMS_CONFIG_KEY = "auto_merge_params";

    public static String getTextConfigOrEmpty(JsonNode config, String key) {
        return config.has(key) ? config.get(key).asText("") : "";
    }
}
