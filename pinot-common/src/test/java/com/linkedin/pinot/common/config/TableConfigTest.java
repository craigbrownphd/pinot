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

package com.linkedin.pinot.common.config;

import com.linkedin.pinot.common.config.helper.TableConfigJSONBuilder;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.common.utils.DateFormat;
import java.io.IOException;
import org.json.JSONException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TableConfigTest {

  @Test
  public void testInit()
      throws IOException, JSONException {
    TableConfigJSONBuilder tableConfigJSONBuilder =
        new TableConfigJSONBuilder(TableType.OFFLINE).setTableName("myTable");
    {
      // No date format or quota config
      TableConfig tableConfig = TableConfig.init(tableConfigJSONBuilder.build().toString());

      Assert.assertEquals(tableConfig.getTableName(), "myTable_OFFLINE");
      Assert.assertEquals(tableConfig.getTableType(), TableType.OFFLINE);
      Assert.assertEquals(tableConfig.getIndexingConfig().getLoadMode(), "HEAP");
      Assert.assertNull(tableConfig.getValidationConfig().getDateFormat());
      Assert.assertNull(tableConfig.getQuotaConfig());

      // Serialize then de-serialize
      TableConfig validator = TableConfig.init(tableConfig.toJSON().toString());
      Assert.assertEquals(validator.getTableName(), tableConfig.getTableName());
      Assert.assertNull(validator.getValidationConfig().getDateFormat());
      Assert.assertNull(validator.getQuotaConfig());
    }
    {
      // With date format and quota config
      DateFormat dateFormat = new DateFormat();
      dateFormat.setSimpleDate("yymmdd");
      QuotaConfig quotaConfig = new QuotaConfig();
      quotaConfig.setStorage("30G");
      TableConfig tableConfig = TableConfig.init(
          tableConfigJSONBuilder.setDateFormat(dateFormat).setQuotaConfig(quotaConfig).build().toString());

      Assert.assertEquals(tableConfig.getTableName(), "myTable_OFFLINE");
      Assert.assertEquals(tableConfig.getTableType(), TableType.OFFLINE);
      Assert.assertEquals(tableConfig.getIndexingConfig().getLoadMode(), "HEAP");
      Assert.assertNotNull(tableConfig.getValidationConfig().getDateFormat());
      Assert.assertEquals(tableConfig.getValidationConfig().getDateFormat().getSimpleDate(), "yymmdd");
      Assert.assertNotNull(tableConfig.getQuotaConfig());
      Assert.assertEquals(tableConfig.getQuotaConfig().getStorage(), "30G");

      // Serialize then de-serialize
      TableConfig validator = TableConfig.init(tableConfig.toJSON().toString());
      Assert.assertEquals(validator.getTableName(), tableConfig.getTableName());
      Assert.assertNotNull(tableConfig.getValidationConfig().getDateFormat());
      Assert.assertEquals(tableConfig.getValidationConfig().getDateFormat().getSimpleDate(), "yymmdd");
      Assert.assertNotNull(validator.getQuotaConfig());
      Assert.assertEquals(validator.getQuotaConfig().getStorage(), tableConfig.getQuotaConfig().getStorage());
    }
  }
}
