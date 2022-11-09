package io.airbyte.integrations.destination.hive;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Leibniz on 2022/10/31.
 */
@Slf4j
public enum FileFormat {
    TEXTFILE("ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\001' STORED AS TEXTFILE"),
    AVRO("STORED AS AVRO"),
    PARQUET("STORED AS PARQUET"),
    ORC("STORED AS ORC"),
    ;

    @Getter
    private final String hiveFormatConfig;

    FileFormat(final String hiveFormatConfig) {
        this.hiveFormatConfig = hiveFormatConfig;
    }

    public static FileFormat parse(String configStr) {
        try {
            return FileFormat.valueOf(configStr.toUpperCase());
        } catch (Exception e) {
            log.warn("Unsupported file format: " + configStr);
            return PARQUET;
        }
    }
}
