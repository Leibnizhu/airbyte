/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.iceberg;

import static io.airbyte.integrations.destination.iceberg.IcebergConstants.FORMAT_TYPE_CONFIG_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.destination.iceberg.IcebergOperations.IcebergOperationsFactory;
import io.airbyte.integrations.destination.iceberg.config.catalog.HiveCatalogConfig;
import io.airbyte.integrations.destination.iceberg.config.catalog.IcebergCatalogConfig;
import io.airbyte.integrations.destination.iceberg.config.format.FormatConfig;
import io.airbyte.integrations.destination.iceberg.config.storage.S3Config;
import io.airbyte.integrations.destination.iceberg.config.storage.credential.S3AccessKeyCredentialConfig;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteConnectionStatus.Status;
import java.io.IOException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.spark.SparkCatalog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
class IcebergHiveCatalogConfigTest {

  private static final String FAKE_WAREHOUSE_URI = "s3a://fake-bucket";
  private static final String FAKE_ENDPOINT = "fake-endpoint";
  private static final String FAKE_ENDPOINT_WITH_SCHEMA = "https://fake-endpoint";
  private static final String FAKE_ACCESS_KEY_ID = "fake-accessKeyId";
  private static final String FAKE_SECRET_ACCESS_KEY = "fake-secretAccessKey";
  private static final String FAKE_THRIFT_URI = "thrift://fake-thrift-uri";
  private static final String MIN_USABLE_CONFIG_JSON = """
    {
      "catalog_config": {
        "catalog_type": "Hive",
        "hive_thrift_uri": "thrift://server:9083",
        "database": "test"
      },
      "storage_config": {
        "storage_type": "S3",
        "s3_warehouse_uri": "s3a://warehouse/hive",
        "s3_bucket_region": "us-east-1"
      },
      "format_config": {
        "format": "Parquet"
      }
    }""";

  private HiveCatalogConfig config;
  private IcebergOperationsFactory factory;
  private IcebergOperations operations;

  @BeforeAll
  static void staticSetup() {
  }

  @AfterAll
  static void staticStop() {
  }

  @BeforeEach
  void setup() throws IOException {
    operations = mock(IcebergOperations.class);

    config = new HiveCatalogConfig(FAKE_THRIFT_URI);
    config.setStorageConfig(S3Config.builder()
      .warehouseUri(FAKE_WAREHOUSE_URI)
      .bucketRegion("fake-region")
      .endpoint(FAKE_ENDPOINT)
      .endpointWithSchema(FAKE_ENDPOINT_WITH_SCHEMA)
      .accessKeyId(FAKE_ACCESS_KEY_ID)
      .secretKey(FAKE_SECRET_ACCESS_KEY)
      .credentialConfig(new S3AccessKeyCredentialConfig(FAKE_ACCESS_KEY_ID, FAKE_SECRET_ACCESS_KEY))
      .build());
    config.setFormatConfig(new FormatConfig(Jsons.jsonNode(ImmutableMap.of(FORMAT_TYPE_CONFIG_KEY, "Parquet"))));
    config.setDefaultOutputDatabase("default");

    factory = new IcebergOperationsFactory() {
      @Override
      public IcebergOperations getInstance(IcebergCatalogConfig icebergCatalogConfig, String sparkAppName) {
        return operations;
      }
    };
  }

  @Test
  public void checksTempTableAlreadyExistsTest() throws Exception {
    final IcebergDestination destinationFail = new IcebergDestination(factory);
    doThrow(new AlreadyExistsException("Table already exists: temp_1123412341234"))
      .when(operations)
      .createAirbyteRawTable(anyString());
    final AirbyteConnectionStatus status = destinationFail.check(Jsons.deserialize(MIN_USABLE_CONFIG_JSON));
    log.info("status={}", status);
    assertEquals(Status.FAILED, status.getStatus(), "Connection check should have failed");
    assertTrue(status.getMessage().contains("Table already exists"),
      "Connection check returned wrong failure message");
  }

  @Test
  public void checksSelectTableFailedTest() throws Exception {
    final IcebergDestination destinationFail = new IcebergDestination(factory);
    doThrow(new ForbiddenException("S3 Forbidden"))
      .when(operations)
      .collectTableContent(anyString(), anyInt());
    final AirbyteConnectionStatus status = destinationFail.check(Jsons.deserialize(MIN_USABLE_CONFIG_JSON));
    log.info("status={}", status);
    assertEquals(Status.FAILED, status.getStatus(), "Connection check should have failed");
    assertTrue(status.getMessage().contains("S3 Forbidden"),
      "Connection check returned wrong failure message");
  }

  @Test
  public void hiveThriftUriInvalidTest() throws IllegalAccessException {
    final IcebergDestination destinationFail = new IcebergDestination();
    final AirbyteConnectionStatus status = destinationFail.check(Jsons.deserialize("""
      {
        "catalog_config": {
          "catalog_type": "Hive",
          "hive_thrift_uri": "server:9083",
          "database": "test"
        },
        "storage_config": {
          "storage_type": "S3",
                                                                                       "access_key_id": "xxxxxxxxxxx",
                                                                                       "secret_access_key": "yyyyyyyyyyyy",
                                                                                       "s3_warehouse_uri": "s3a://warehouse/hive",
                                                                                       "s3_bucket_region": "us-east-1",
                                                                                       "s3_endpoint": "your-own-minio-host:9000"
                                                                                     },
                                                                                     "format_config": {
                                                                                       "format": "Parquet"
                                                                                     }
                                                                                   }"""));
    log.info("status={}", status);
    assertEquals(Status.FAILED, status.getStatus(), "Connection check should have failed");
    assertTrue(status.getMessage().contains("hive_thrift_uri must start with 'thrift://'"),
        "Connection check returned wrong failure message");
  }

  @Test
  public void hiveCatalogSparkConfigTest() {
    Map<String, String> sparkConfig = config.sparkConfigMap();
    log.info("Spark Config for Hive-S3 catalog: {}", sparkConfig);

    // Catalog config
    assertEquals("hive", sparkConfig.get("spark.sql.catalog.iceberg.type"));
    assertEquals(FAKE_THRIFT_URI, sparkConfig.get("spark.sql.catalog.iceberg.uri"));
    assertEquals(SparkCatalog.class.getName(), sparkConfig.get("spark.sql.catalog.iceberg"));
    assertEquals(S3FileIO.class.getName(), sparkConfig.get("spark.sql.catalog.iceberg.io-impl"));
    assertEquals(FAKE_WAREHOUSE_URI, sparkConfig.get("spark.sql.catalog.iceberg.warehouse"));
    assertEquals(FAKE_ACCESS_KEY_ID, sparkConfig.get("spark.sql.catalog.iceberg.s3.access-key-id"));
    assertEquals(FAKE_SECRET_ACCESS_KEY, sparkConfig.get("spark.sql.catalog.iceberg.s3.secret-access-key"));
    assertEquals(FAKE_ENDPOINT_WITH_SCHEMA, sparkConfig.get("spark.sql.catalog.iceberg.s3.endpoint"));
    assertEquals("false", sparkConfig.get("spark.sql.catalog.iceberg.s3.path-style-access"));

    // hadoop config
    assertEquals(FAKE_ENDPOINT, sparkConfig.get("spark.hadoop.fs.s3a.endpoint"));
    assertEquals(FAKE_ACCESS_KEY_ID, sparkConfig.get("spark.hadoop.fs.s3a.access.key"));
    assertEquals(FAKE_SECRET_ACCESS_KEY, sparkConfig.get("spark.hadoop.fs.s3a.secret.key"));
    assertEquals(S3AFileSystem.class.getName(), sparkConfig.get("spark.hadoop.fs.s3a.impl"));
    assertEquals("false", sparkConfig.get("spark.hadoop.fs.s3a.connection.ssl.enabled"));
  }

  @Test
  public void s3ConfigForCatalogInitializeTest() {
    Map<String, String> properties = config.getStorageConfig().catalogInitializeProperties();
    log.info("S3 Config for HiveCatalog Initialize: {}", properties);

    assertEquals(S3FileIO.class.getName(), properties.get("io-impl"));
    assertEquals(FAKE_ENDPOINT_WITH_SCHEMA, properties.get("s3.endpoint"));
    assertEquals(FAKE_ACCESS_KEY_ID, properties.get("s3.access-key-id"));
    assertEquals(FAKE_SECRET_ACCESS_KEY, properties.get("s3.secret-access-key"));
    assertEquals("false", properties.get("s3.path-style-access"));
  }

}
