/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.config.helper;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.config.IndexingConfig;
import com.linkedin.pinot.common.config.QuotaConfig;
import com.linkedin.pinot.common.config.SegmentsValidationAndRetentionConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableCustomConfig;
import com.linkedin.pinot.common.config.TableTaskConfig;
import com.linkedin.pinot.common.config.TenantConfig;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.common.utils.DateFormat;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONException;
import org.json.JSONObject;


public class TableConfigJSONBuilder {
  private static final String DEFAULT_PUSH_TYPE = "APPEND";
  private static final String DEFAULT_SEGMENT_ASSIGNMENT_STRATEGY = "BalanceNumSegmentAssignmentStrategy";
  private static final String DEFAULT_NUM_REPLICAS = "1";
  private static final String DEFAULT_LOAD_MODE = "HEAP";
  private static final String MMAP_LOAD_MODE = "MMAP";

  private final TableType _tableType;
  private String _tableName;
  private boolean _isLLC;

  // Validation config related
  private String _timeColumnName;
  private String _timeType;
  private DateFormat _dateFormat;
  private String _retentionTimeUnit;
  private String _retentionTimeValue;
  private String _segmentPushType = DEFAULT_PUSH_TYPE;
  private String _segmentAssignmentStrategy = DEFAULT_SEGMENT_ASSIGNMENT_STRATEGY;
  private String _schemaName;
  private String _numReplicas = DEFAULT_NUM_REPLICAS;

  // Tenant config related
  private String _brokerTenant;
  private String _serverTenant;

  // Indexing config related
  private String _loadMode = DEFAULT_LOAD_MODE;
  private String _segmentVersion;
  private String _sortedColumn;
  private List<String> _invertedIndexColumns;
  private List<String> _noDictionaryColumns;
  private Map<String, String> _streamConfigs;

  private TableCustomConfig _customConfig;
  private QuotaConfig _quotaConfig;
  private TableTaskConfig _taskConfig;

  public TableConfigJSONBuilder(TableType tableType) {
    _tableType = tableType;
  }

  public TableConfigJSONBuilder setTableName(String tableName) {
    _tableName = tableName;
    return this;
  }

  public TableConfigJSONBuilder setLLC(boolean isLLC) {
    Preconditions.checkState(_tableType == TableType.REALTIME);
    _isLLC = isLLC;
    return this;
  }

  public TableConfigJSONBuilder setTimeColumnName(String timeColumnName) {
    _timeColumnName = timeColumnName;
    return this;
  }

  public TableConfigJSONBuilder setTimeType(String timeType) {
    _timeType = timeType;
    return this;
  }

  public TableConfigJSONBuilder setDateFormat(DateFormat dateFormat) {
    _dateFormat = dateFormat;
    return this;
  }

  public TableConfigJSONBuilder setRetentionTimeUnit(String retentionTimeUnit) {
    _retentionTimeUnit = retentionTimeUnit;
    return this;
  }

  public TableConfigJSONBuilder setRetentionTimeValue(String retentionTimeValue) {
    _retentionTimeValue = retentionTimeValue;
    return this;
  }

  public TableConfigJSONBuilder setSegmentPushType(String segmentPushType) {
    _segmentPushType = segmentPushType;
    return this;
  }

  public TableConfigJSONBuilder setSegmentAssignmentStrategy(String segmentAssignmentStrategy) {
    _segmentAssignmentStrategy = segmentAssignmentStrategy;
    return this;
  }

  public TableConfigJSONBuilder setSchemaName(String schemaName) {
    _schemaName = schemaName;
    return this;
  }

  public TableConfigJSONBuilder setNumReplicas(int numReplicas) {
    _numReplicas = String.valueOf(numReplicas);
    return this;
  }

  public TableConfigJSONBuilder setBrokerTenant(String brokerTenant) {
    _brokerTenant = brokerTenant;
    return this;
  }

  public TableConfigJSONBuilder setServerTenant(String serverTenant) {
    _serverTenant = serverTenant;
    return this;
  }

  public TableConfigJSONBuilder setLoadMode(String loadMode) {
    if (MMAP_LOAD_MODE.equalsIgnoreCase(loadMode)) {
      _loadMode = MMAP_LOAD_MODE;
    }
    return this;
  }

  public TableConfigJSONBuilder setSegmentVersion(String segmentVersion) {
    _segmentVersion = segmentVersion;
    return this;
  }

  public TableConfigJSONBuilder setSortedColumn(String sortedColumn) {
    _sortedColumn = sortedColumn;
    return this;
  }

  public TableConfigJSONBuilder setInvertedIndexColumns(List<String> invertedIndexColumns) {
    _invertedIndexColumns = invertedIndexColumns;
    return this;
  }

  public TableConfigJSONBuilder setNoDictionaryColumns(List<String> noDictionaryColumns) {
    _noDictionaryColumns = noDictionaryColumns;
    return this;
  }

  public TableConfigJSONBuilder setStreamConfigs(Map<String, String> streamConfigs) {
    Preconditions.checkState(_tableType == TableType.REALTIME);
    _streamConfigs = streamConfigs;
    return this;
  }

  public TableConfigJSONBuilder setCustomConfig(TableCustomConfig customConfig) {
    _customConfig = customConfig;
    return this;
  }

  public TableConfigJSONBuilder setQuotaConfig(QuotaConfig quotaConfig) {
    _quotaConfig = quotaConfig;
    return this;
  }

  public TableConfigJSONBuilder setTaskConfig(TableTaskConfig taskConfig) {
    _taskConfig = taskConfig;
    return this;
  }

  public JSONObject build()
      throws IOException, JSONException {
    // Validation config
    SegmentsValidationAndRetentionConfig validationConfig = new SegmentsValidationAndRetentionConfig();
    validationConfig.setTimeColumnName(_timeColumnName);
    validationConfig.setTimeType(_timeType);
    validationConfig.setDateFormat(_dateFormat);
    validationConfig.setRetentionTimeUnit(_retentionTimeUnit);
    validationConfig.setRetentionTimeValue(_retentionTimeValue);
    validationConfig.setSegmentPushType(_segmentPushType);
    validationConfig.setSegmentAssignmentStrategy(_segmentAssignmentStrategy);
    validationConfig.setSchemaName(_schemaName);
    validationConfig.setReplication(_numReplicas);
    if (_isLLC) {
      validationConfig.setReplicasPerPartition(_numReplicas);
    }

    // Tenant config
    TenantConfig tenantConfig = new TenantConfig();
    tenantConfig.setBroker(_brokerTenant);
    tenantConfig.setServer(_serverTenant);

    // Indexing config
    IndexingConfig indexingConfig = new IndexingConfig();
    indexingConfig.setLoadMode(_loadMode);
    indexingConfig.setSegmentFormatVersion(_segmentVersion);
    if (_sortedColumn != null) {
      indexingConfig.setSortedColumn(Collections.singletonList(_sortedColumn));
    }
    indexingConfig.setInvertedIndexColumns(_invertedIndexColumns);
    indexingConfig.setNoDictionaryColumns(_noDictionaryColumns);
    indexingConfig.setStreamConfigs(_streamConfigs);
    // TODO: set SegmentPartitionConfig here

    if (_customConfig == null) {
      _customConfig = new TableCustomConfig();
      _customConfig.setCustomConfigs(new HashMap<String, String>());
    }

    return TableConfig.getJSONConfig(_tableName, _tableType, validationConfig, tenantConfig, indexingConfig,
        _customConfig, _quotaConfig, _taskConfig);
  }
}
