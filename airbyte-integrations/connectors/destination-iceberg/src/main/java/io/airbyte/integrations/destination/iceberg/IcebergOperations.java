/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.iceberg;

import static io.airbyte.integrations.base.JavaBaseConstants.COLUMN_NAME_AB_ID;
import static io.airbyte.integrations.base.JavaBaseConstants.COLUMN_NAME_DATA;
import static io.airbyte.integrations.base.JavaBaseConstants.COLUMN_NAME_EMITTED_AT;

import io.airbyte.integrations.destination.iceberg.config.catalog.IcebergCatalogConfig;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType$;

/**
 * @author Leibniz on 2022/11/18.
 */
@Slf4j
public class IcebergOperations implements AutoCloseable {

  private final SparkSession spark;
  private final Catalog icebergCatalog;
  private final StructType normalizationSchema;

  private IcebergOperations(SparkSession spark, Catalog icebergCatalog) {
    this.spark = spark;
    this.icebergCatalog = icebergCatalog;
    this.normalizationSchema = new StructType().add(COLUMN_NAME_AB_ID, StringType$.MODULE$)
      .add(COLUMN_NAME_EMITTED_AT, TimestampType$.MODULE$)
      .add(COLUMN_NAME_DATA, StringType$.MODULE$);
  }

  public void createDatabase(String databaseName) throws Exception {
    this.spark.sql("CREATE DATABASE IF NOT EXISTS " + databaseName);
  }

  public boolean dropTable(String database, String tableName, boolean purge) throws Exception {
    log.info("Trying to drop temp table: {}.{}", database, tableName);
    TableIdentifier tempTableIdentifier = TableIdentifier.of(database, tableName);
    boolean dropSuccess = icebergCatalog.dropTable(tempTableIdentifier, true);
    log.info("Drop temp table: {}.{} {}", database, tableName, dropSuccess ? "success" : "fail");
    return dropSuccess;
  }

  public void createAirbyteRawTable(String tableFullName) throws Exception {
    this.spark.sql("CREATE TABLE %s (%s STRING, %s TIMESTAMP, %s STRING)".formatted(tableFullName,
      COLUMN_NAME_AB_ID,
      COLUMN_NAME_EMITTED_AT,
      COLUMN_NAME_DATA));
  }

  public void appendRowsToTable(String tableFullName, List<Row> rows, String format) {
    this.spark.createDataFrame(rows, this.normalizationSchema).write()
      // append data to temp table
      .mode(SaveMode.Append)
      //TODO compression config
      .option("write-format", format).saveAsTable(tableFullName);
  }

  public void copyFullTable(String sourceTableFullName, String targetTableFullName, SaveMode mode) {
    this.spark.table(sourceTableFullName).write().mode(mode).saveAsTable(targetTableFullName);
  }

  @Override
  public void close() throws Exception {
    this.spark.close();
  }

  public void compactTable(String database, String tableName, long compactTargetFileSizeBytes) throws Exception {
    TableIdentifier tableIdentifier = TableIdentifier.of(database, tableName);
    SparkActions.get()
      .rewriteDataFiles(icebergCatalog.loadTable(tableIdentifier))
      .option("target-file-size-bytes", String.valueOf(compactTargetFileSizeBytes))
      .execute();
  }

  public void showTableContent(String tableFullName, int limit) {
    spark.table(tableFullName).limit(limit).show();
  }

  public Row[] collectTableContent(String tableFullName, int limit) {
    return (Row[]) spark.table(tableFullName).limit(limit).collect();
  }

  public static class IcebergOperationsFactory {

    public IcebergOperations getInstance(IcebergCatalogConfig icebergCatalogConfig, String sparkAppName) {
      Catalog catalog = icebergCatalogConfig.genCatalog();
      return new IcebergOperations(buildSpark(icebergCatalogConfig, sparkAppName), catalog);
    }

    private SparkSession buildSpark(IcebergCatalogConfig icebergCatalogConfig, String sparkAppName) {
      Map<String, String> sparkConfMap = icebergCatalogConfig.sparkConfigMap();

      // in spark's local mode, driver and executor runs on the same container as the connector itself.
      // it can also run on an already existing cluster, it's not necessary, because of :
      //1. all incoming data are from the connector's container, it's the bottleneck of the whole syncing job; data are not distributed, so running spark job on existing cluster does not benefit from spark cluster.
      //2. submitting spark job to an already existing cluster may cost extra time, for waiting spark worker resource, and extra network I/O, etc.,
      Builder sparkBuilder = SparkSession.builder().master("local").appName(sparkAppName);
      sparkConfMap.forEach(sparkBuilder::config);
      return sparkBuilder.getOrCreate();
    }
  }
}
