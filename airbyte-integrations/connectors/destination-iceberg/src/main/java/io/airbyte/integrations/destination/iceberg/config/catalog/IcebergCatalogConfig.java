/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.iceberg.config.catalog;

import static org.apache.commons.lang3.StringUtils.isBlank;

import io.airbyte.integrations.destination.iceberg.IcebergConstants;
import io.airbyte.integrations.destination.iceberg.config.format.FormatConfig;
import io.airbyte.integrations.destination.iceberg.config.storage.StorageConfig;
import java.util.Map;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.catalog.Catalog;

/**
 * @author Leibniz on 2022/10/26.
 */
@Data
@ToString
@Slf4j
public abstract class IcebergCatalogConfig {

  protected StorageConfig storageConfig;
  protected FormatConfig formatConfig;

  private String defaultOutputDatabase;

  public abstract Map<String, String> sparkConfigMap();

  public abstract Catalog genCatalog();

  public String defaultOutputDatabase() {
    return isBlank(defaultOutputDatabase) ? IcebergConstants.DEFAULT_DATABASE : defaultOutputDatabase;
  }

}
