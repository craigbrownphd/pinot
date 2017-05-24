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
package com.linkedin.pinot.controller.api.restlet.resources;

import com.linkedin.pinot.common.config.QuotaConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.helper.TableConfigJSONBuilder;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.DataSource;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.DataSource.Realtime.Kafka;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.ControllerRequestURLBuilder;
import com.linkedin.pinot.controller.helix.ControllerTest;
import com.linkedin.pinot.controller.helix.ControllerTestUtils;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Test for table creation
 */
public class PinotTableRestletResourceTest extends ControllerTest {
  private static final int TABLE_MIN_REPLICATION = 3;

  private final TableConfigJSONBuilder _offlineBuilder = new TableConfigJSONBuilder(TableType.OFFLINE);
  private final TableConfigJSONBuilder _realtimeBuilder = new TableConfigJSONBuilder(TableType.REALTIME);
  private final ControllerRequestURLBuilder _controllerRequestURLBuilder =
      ControllerRequestURLBuilder.baseUrl(CONTROLLER_BASE_API_URL);

  @BeforeClass
  public void setUp() {
    startZk();
    ControllerConf config = ControllerTestUtils.getDefaultControllerConfiguration();
    config.setTableMinReplicas(TABLE_MIN_REPLICATION);
    startController(config);

    _offlineBuilder.setTimeColumnName("potato")
        .setTimeType("DAYS")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("5")
        .setBrokerTenant("default")
        .setServerTenant("default")
        .setLoadMode("MMAP");

    Map<String, String> streamConfigs = new HashMap<>();
    streamConfigs.put("streamType", "kafka");
    streamConfigs.put(DataSource.STREAM_PREFIX + "." + Kafka.CONSUMER_TYPE, Kafka.ConsumerType.highLevel.toString());
    streamConfigs.put(DataSource.STREAM_PREFIX + "." + Kafka.TOPIC_NAME, "fakeTopic");
    streamConfigs.put(DataSource.STREAM_PREFIX + "." + Kafka.DECODER_CLASS, "fakeClass");
    streamConfigs.put(DataSource.STREAM_PREFIX + "." + Kafka.ZK_BROKER_URL, "fakeUrl");
    streamConfigs.put(DataSource.STREAM_PREFIX + "." + Kafka.HighLevelConsumer.ZK_CONNECTION_STRING, "potato");
    streamConfigs.put(DataSource.Realtime.REALTIME_SEGMENT_FLUSH_SIZE, Integer.toString(1234));
    streamConfigs.put(
        DataSource.STREAM_PREFIX + "." + Kafka.KAFKA_CONSUMER_PROPS_PREFIX + "." + Kafka.AUTO_OFFSET_RESET, "smallest");
    _realtimeBuilder.setTimeColumnName("potato")
        .setTimeType("DAYS")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("5")
        .setSchemaName("fakeSchema")
        .setNumReplicas(3)
        .setBrokerTenant("default")
        .setServerTenant("default")
        .setLoadMode("MMAP")
        .setSortedColumn("fakeColumn")
        .setStreamConfigs(streamConfigs);
  }

  @Test
  public void testCreateTable()
      throws Exception {
    // Create a table with an invalid name
    String tableConfigJSONString =
        _offlineBuilder.setTableName("bad__table__name").setNumReplicas(3).build().toString();
    try {
      sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfigJSONString);
      Assert.fail("Creation of a table with two underscores in the table name did not fail");
    } catch (IOException e) {
      // Expected
    }

    // Create a table with a valid name
    tableConfigJSONString = _offlineBuilder.setTableName("valid_table_name").setNumReplicas(3).build().toString();
    sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfigJSONString);
    boolean tableExistsError = false;
    try {
      sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfigJSONString);
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().startsWith("Server returned HTTP response code: 409"), e.getMessage());
      tableExistsError = true;
    }
    Assert.assertTrue(tableExistsError);

    // Create a table with an invalid name
    tableConfigJSONString = _realtimeBuilder.setTableName("bad__table__name").setNumReplicas(3).build().toString();
    try {
      sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfigJSONString);
      Assert.fail("Creation of a table with two underscores in the table name did not fail");
    } catch (IOException e) {
      // Expected
    }

    // Create a table with a valid name
    tableConfigJSONString = _realtimeBuilder.setTableName("valid_table_name").setNumReplicas(3).build().toString();
    sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfigJSONString);

    // try again...should work because POST on existing RT table is allowed
    sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfigJSONString);
  }

  @Test
  public void testTableMinReplication()
      throws Exception {
    testTableMinReplicationInternal("minReplicationOne", 1);
    testTableMinReplicationInternal("minReplicationTwo", TABLE_MIN_REPLICATION + 2);
  }

  private void testTableMinReplicationInternal(String tableName, int tableReplication)
      throws Exception {
    String tableConfigJSONString =
        _offlineBuilder.setTableName(tableName).setNumReplicas(tableReplication).build().toString();
    sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfigJSONString);
    // table creation should succeed
    TableConfig tableConfig = getTableConfig(tableName, "OFFLINE");
    Assert.assertEquals(tableConfig.getValidationConfig().getReplicationNumber(),
        Math.max(tableReplication, TABLE_MIN_REPLICATION));

    tableConfigJSONString =
        _realtimeBuilder.setTableName(tableName).setNumReplicas(tableReplication).build().toString();
    sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfigJSONString);
    tableConfig = getTableConfig(tableName, "REALTIME");
    Assert.assertEquals(tableConfig.getValidationConfig().getReplicationNumber(),
        Math.max(tableReplication, TABLE_MIN_REPLICATION));
    // This test can only be done via integration test
//    int replicasPerPartition = Integer.valueOf(tableConfig.getValidationConfig().getReplicasPerPartition());
//    Assert.assertEquals(replicasPerPartition, Math.max(tableReplication, TABLE_MIN_REPLICATION));
  }

  private TableConfig getTableConfig(String tableName, String tableType)
      throws Exception {
    String tableConfigString = sendGetRequest(_controllerRequestURLBuilder.forTableGet(tableName));
    JSONObject tableConfigJSON = new JSONObject(tableConfigString);
    return TableConfig.init(tableConfigJSON.getJSONObject(tableType).toString());
  }

  @Test
  public void testUpdateTableConfig()
      throws Exception {
    String tableName = "updateTC";
    String tableConfigJSONString = _offlineBuilder.setTableName(tableName).setNumReplicas(2).build().toString();
    sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfigJSONString);
    // table creation should succeed
    TableConfig tableConfig = getTableConfig(tableName, "OFFLINE");
    Assert.assertEquals(tableConfig.getValidationConfig().getRetentionTimeValue(), "5");
    Assert.assertEquals(tableConfig.getValidationConfig().getRetentionTimeUnit(), "DAYS");

    tableConfig.getValidationConfig().setRetentionTimeUnit("HOURS");
    tableConfig.getValidationConfig().setRetentionTimeValue("10");

    JSONObject jsonResponse = new JSONObject(
        sendPutRequest(_controllerRequestURLBuilder.forUpdateTableConfig(tableName), tableConfig.toJSON().toString()));
    Assert.assertTrue(jsonResponse.has("status"));
    Assert.assertEquals(jsonResponse.getString("status"), "Success");

    TableConfig modifiedConfig = getTableConfig(tableName, "OFFLINE");
    Assert.assertEquals(modifiedConfig.getValidationConfig().getRetentionTimeUnit(), "HOURS");
    Assert.assertEquals(modifiedConfig.getValidationConfig().getRetentionTimeValue(), "10");

    // Realtime
    tableConfigJSONString = _realtimeBuilder.setTableName(tableName).setNumReplicas(2).build().toString();
    sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfigJSONString);
    tableConfig = getTableConfig(tableName, "REALTIME");
    Assert.assertEquals(tableConfig.getValidationConfig().getRetentionTimeValue(), "5");
    Assert.assertEquals(tableConfig.getValidationConfig().getRetentionTimeUnit(), "DAYS");
    Assert.assertNull(tableConfig.getQuotaConfig());

    QuotaConfig quota = new QuotaConfig();
    quota.setStorage("10G");
    tableConfig.setQuotaConfig(quota);
    sendPutRequest(_controllerRequestURLBuilder.forUpdateTableConfig(tableName), tableConfig.toJSON().toString());
    modifiedConfig = getTableConfig(tableName, "REALTIME");
    Assert.assertNotNull(modifiedConfig.getQuotaConfig());
    Assert.assertEquals(modifiedConfig.getQuotaConfig().getStorage(), "10G");
    boolean notFoundException = false;
    try {
      // table does not exist
      JSONObject jsonConfig = tableConfig.toJSON();
      jsonConfig.put("tableName", "noSuchTable_REALTIME");
      sendPutRequest(_controllerRequestURLBuilder.forUpdateTableConfig("noSuchTable"), jsonConfig.toString());
    } catch (Exception e) {
      Assert.assertTrue(e instanceof FileNotFoundException);
      notFoundException = true;
    }
    Assert.assertTrue(notFoundException);
  }
}
