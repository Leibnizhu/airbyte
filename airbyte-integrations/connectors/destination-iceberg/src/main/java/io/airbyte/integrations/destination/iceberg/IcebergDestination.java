/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.iceberg;

import static io.airbyte.integrations.destination.iceberg.config.format.DataFileFormat.PARQUET;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.BaseConnector;
import io.airbyte.integrations.base.AirbyteMessageConsumer;
import io.airbyte.integrations.base.Destination;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.destination.iceberg.IcebergOperations.IcebergOperationsFactory;
import io.airbyte.integrations.destination.iceberg.config.catalog.IcebergCatalogConfig;
import io.airbyte.integrations.destination.iceberg.config.catalog.IcebergCatalogConfigFactory;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteConnectionStatus.Status;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;

@Slf4j
public class IcebergDestination extends BaseConnector implements Destination {

  private final IcebergCatalogConfigFactory configFactory = new IcebergCatalogConfigFactory();
  private final IcebergOperationsFactory operationsFactory;

  public IcebergDestination() {
    this.operationsFactory = new IcebergOperationsFactory();
  }

  @VisibleForTesting
  public IcebergDestination(IcebergOperationsFactory operationsFactory) {
    this.operationsFactory = Objects.requireNonNullElseGet(operationsFactory,
      IcebergOperationsFactory::new);
  }

  public static void main(String[] args) throws Exception {
    new IntegrationRunner(new IcebergDestination()).run(args);
  }

  @Override
  public AirbyteConnectionStatus check(JsonNode config) {
    // parse configuration
    IcebergCatalogConfig icebergCatalogConfig;
    try {
      icebergCatalogConfig = configFactory.fromJsonNodeConfig(config);
    } catch (Exception e) {
      return handleCheckException(e, "parse the Iceberg configuration");
    }

    // check the configuration
    String database = icebergCatalogConfig.defaultOutputDatabase();
    String tempTableName = "temp_" + System.currentTimeMillis();
    String tempTableFullName = "%s.`%s`.`%s`".formatted(IcebergConstants.CATALOG_NAME, database, tempTableName);
    try (IcebergOperations operations = operationsFactory.getInstance(icebergCatalogConfig,
      "Iceberg-Config-Check")) {
      operations.createAirbyteRawTable(tempTableFullName);
      Row tempRow = generateCheckRow(Map.of("testKey", "testValue"));
      operations.appendRowsToTable(tempTableFullName, List.of(tempRow), PARQUET.getFormatName());
      operations.showTableContent(tempTableFullName, 1);
      var selectRows = (Row[]) operations.collectTableContent(tempTableFullName, 1);
      operations.dropTable(database, tempTableName, true);

      Preconditions.checkState(selectRows.length == 1,
        "Size of temp table for checking is not 1: " + selectRows.length);
      assertRowEquals(tempRow, selectRows[0]);

      //getting here means Iceberg catalog check success
      return new AirbyteConnectionStatus().withStatus(Status.SUCCEEDED);
    } catch (Exception e) {
      return handleCheckException(e, "access the Iceberg catalog");
    }
  }

  private void assertRowEquals(Row expect, Row actual) {
    Preconditions.checkState(expect.size() == actual.size(),
      "Inserted row and selected Row has different size:" + expect.size() + " vs " + actual.size());
    for (int i = 0; i < expect.size(); i++) {
      Preconditions.checkState(expect.get(i).equals(actual.get(i)),
        "Inserted row and selected Row has different value in " + i + "st column: " + actual.get(i) + " vs "
        + actual.get(i));
    }
  }

  private Row generateCheckRow(Map<String, Object> data) {
    String tempDataId = UUID.randomUUID().toString();
    long tempDataTs = System.currentTimeMillis();
    return new GenericRow(new Object[]{tempDataId, new Timestamp(tempDataTs), Jsons.serialize(data)});
  }

  private AirbyteConnectionStatus handleCheckException(Exception exception, String action) {
    log.error("Exception attempting to {}: {}", action, exception.getMessage(), exception);
    Throwable rootCause = getRootCause(exception);
    String errMessage =
      "Could not " + action + " with the provided configuration. \n" + exception.getMessage() + ", root cause: "
      + rootCause.getClass().getSimpleName() + "(" + rootCause.getMessage() + ")";
    return new AirbyteConnectionStatus().withStatus(Status.FAILED).withMessage(errMessage);
  }

  private Throwable getRootCause(Throwable exp) {
    Throwable curCause = exp.getCause();
    if (curCause == null) {
      return exp;
    } else {
      return getRootCause(curCause);
    }
  }

  @Override
  public AirbyteMessageConsumer getConsumer(JsonNode config,
                                            ConfiguredAirbyteCatalog catalog,
                                            Consumer<AirbyteMessage> outputRecordCollector) {
    final IcebergCatalogConfig icebergCatalogConfig = this.configFactory.fromJsonNodeConfig(config);
    String sparkAppName = "Airbyte->Iceberg-" + System.currentTimeMillis();
    IcebergOperations operations = this.operationsFactory.getInstance(icebergCatalogConfig, sparkAppName);
    return new IcebergConsumer(operations, outputRecordCollector, catalog, icebergCatalogConfig);
  }

}
