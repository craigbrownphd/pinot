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
package com.linkedin.pinot.common.data;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.TimeGranularitySpec.TimeFormat;
import com.linkedin.pinot.common.utils.SchemaUtils;


public class SchemaTest {
  public static final Logger LOGGER = LoggerFactory.getLogger(SchemaTest.class);

  private Schema schema;

  @BeforeClass
  public void setUp()
      throws IOException {
    URL resourceUrl = getClass().getClassLoader().getResource("schemaTest.schema");
    Preconditions.checkNotNull(resourceUrl);
    schema = Schema.fromFile(new File(resourceUrl.getFile()));
  }

  @Test
  public void testValidation()
      throws Exception {
    Schema schemaToValidate;

    schemaToValidate = Schema.fromString(makeSchema(FieldSpec.DataType.LONG, FieldSpec.DataType.STRING, true));
    Assert.assertTrue(schemaToValidate.validate(LOGGER));

    schemaToValidate = Schema.fromString(makeSchema(FieldSpec.DataType.BOOLEAN, FieldSpec.DataType.STRING, true));
    Assert.assertFalse(schemaToValidate.validate(LOGGER));

    schemaToValidate = Schema.fromString(makeSchema(FieldSpec.DataType.STRING, FieldSpec.DataType.STRING, false));
    Assert.assertFalse(schemaToValidate.validate(LOGGER));

    schemaToValidate = Schema.fromString(makeSchema(FieldSpec.DataType.LONG, FieldSpec.DataType.BOOLEAN, false));
    Assert.assertTrue(schemaToValidate.validate(LOGGER));
  }

  private String makeSchema(FieldSpec.DataType metricType, FieldSpec.DataType dimensionType, boolean isSingleValue) {
    return "{"
        + "  \"schemaName\":\"SchemaTest\","
        + "  \"metricFieldSpecs\":["
        + "    {\"name\":\"m\",\"dataType\":\"" + metricType + "\"}"
        + "  ],"
        + "  \"dimensionFieldSpecs\":["
        + "    {\"name\":\"d\",\"dataType\":\"" + dimensionType + "\",\"singleValueField\":" + isSingleValue + "}"
        + "  ],"
        + "  \"timeFieldSpec\":{"
        + "    \"incomingGranularitySpec\":{\"dataType\":\"LONG\",\"timeType\":\"MILLISECONDS\",\"name\":\"time\"},"
        + "    \"defaultNullValue\":12345"
        + "  }"
        + "}";
  }

  @Test
  public void testSchemaBuilder() {
    final Float defaultFloat = 0.5f;
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("svDimension", FieldSpec.DataType.INT)
        .addSingleValueDimension("svDimensionWithDefault", FieldSpec.DataType.INT, 10)
        .addMultiValueDimension("mvDimension", FieldSpec.DataType.STRING)
        .addMultiValueDimension("mvDimensionWithDefault", FieldSpec.DataType.STRING, "default")
        .addMetric("metric", FieldSpec.DataType.INT)
        .addMetric("metricWithDefault", FieldSpec.DataType.INT, 5)
        .addMetric("derivedMetric", FieldSpec.DataType.LONG, 10, MetricFieldSpec.DerivedMetricType.HLL)
        .addMetric("derivedMetricWithDefault", DataType.FLOAT, 10, MetricFieldSpec.DerivedMetricType.HLL,
            defaultFloat.toString())
        .addTime("time", TimeUnit.DAYS, FieldSpec.DataType.LONG)
        .build();

    FieldSpec fieldSpec;
    fieldSpec = schema.getDimensionSpec("svDimension");
    Assert.assertNotNull(fieldSpec);
    Assert.assertEquals(fieldSpec.isSingleValueField(), true);
    Assert.assertEquals(fieldSpec.getDataType(), FieldSpec.DataType.INT);
    Assert.assertEquals(fieldSpec.getDefaultNullValue(), Integer.MIN_VALUE);

    fieldSpec = schema.getDimensionSpec("svDimensionWithDefault");
    Assert.assertNotNull(fieldSpec);
    Assert.assertEquals(fieldSpec.isSingleValueField(), true);
    Assert.assertEquals(fieldSpec.getDataType(), FieldSpec.DataType.INT);
    Assert.assertEquals(fieldSpec.getDefaultNullValue(), 10);

    fieldSpec = schema.getDimensionSpec("mvDimension");
    Assert.assertNotNull(fieldSpec);
    Assert.assertEquals(fieldSpec.isSingleValueField(), false);
    Assert.assertEquals(fieldSpec.getDataType(), FieldSpec.DataType.STRING);
    Assert.assertEquals(fieldSpec.getDefaultNullValue(), "null");

    fieldSpec = schema.getDimensionSpec("mvDimensionWithDefault");
    Assert.assertNotNull(fieldSpec);
    Assert.assertEquals(fieldSpec.isSingleValueField(), false);
    Assert.assertEquals(fieldSpec.getDataType(), FieldSpec.DataType.STRING);
    Assert.assertEquals(fieldSpec.getDefaultNullValue(), "default");

    fieldSpec = schema.getMetricSpec("metric");
    Assert.assertNotNull(fieldSpec);
    Assert.assertEquals(fieldSpec.isSingleValueField(), true);
    Assert.assertEquals(fieldSpec.getDataType(), FieldSpec.DataType.INT);
    Assert.assertEquals(fieldSpec.getDefaultNullValue(), 0);

    fieldSpec = schema.getMetricSpec("metricWithDefault");
    Assert.assertNotNull(fieldSpec);
    Assert.assertEquals(fieldSpec.isSingleValueField(), true);
    Assert.assertEquals(fieldSpec.getDataType(), FieldSpec.DataType.INT);
    Assert.assertEquals(fieldSpec.getDefaultNullValue(), 5);

    fieldSpec = schema.getMetricSpec("derivedMetric");
    Assert.assertNotNull(fieldSpec);
    Assert.assertEquals(fieldSpec.isSingleValueField(), true);
    Assert.assertEquals(fieldSpec.getDataType(), FieldSpec.DataType.LONG);
    Assert.assertEquals(fieldSpec.getDefaultNullValue(), 0L);

    fieldSpec = schema.getMetricSpec("derivedMetricWithDefault");
    Assert.assertNotNull(fieldSpec);
    Assert.assertEquals(fieldSpec.isSingleValueField(), true);
    Assert.assertEquals(fieldSpec.getDataType(), FieldSpec.DataType.FLOAT);
    Assert.assertEquals(fieldSpec.getDefaultNullValue(), defaultFloat);

    fieldSpec = schema.getTimeFieldSpec();
    Assert.assertNotNull(fieldSpec);
    Assert.assertEquals(fieldSpec.isSingleValueField(), true);
    Assert.assertEquals(fieldSpec.getDataType(), FieldSpec.DataType.LONG);
    Assert.assertEquals(fieldSpec.getDefaultNullValue(), Long.MIN_VALUE);
  }

  @Test
  public void testSchemaBuilderAddTime() {
    String incomingName = "incoming";
    FieldSpec.DataType incomingDataType = FieldSpec.DataType.LONG;
    TimeUnit incomingTimeUnit = TimeUnit.HOURS;
    int incomingTimeUnitSize = 1;
    TimeGranularitySpec incomingTimeGranularitySpec =
        new TimeGranularitySpec(incomingDataType, incomingTimeUnitSize, incomingTimeUnit, incomingName);
    String outgoingName = "outgoing";
    FieldSpec.DataType outgoingDataType = FieldSpec.DataType.INT;
    TimeUnit outgoingTimeUnit = TimeUnit.DAYS;
    int outgoingTimeUnitSize = 1;
    TimeGranularitySpec outgoingTimeGranularitySpec =
        new TimeGranularitySpec(outgoingDataType, outgoingTimeUnitSize, outgoingTimeUnit, outgoingName);
    int defaultNullValue = 17050;

    Schema schema1 = new Schema.SchemaBuilder().setSchemaName("testSchema")
        .addTime(incomingName, incomingTimeUnit, incomingDataType)
        .build();
    Schema schema2 = new Schema.SchemaBuilder().setSchemaName("testSchema")
        .addTime(incomingName, incomingTimeUnit, incomingDataType, defaultNullValue)
        .build();
    Schema schema3 = new Schema.SchemaBuilder().setSchemaName("testSchema")
        .addTime(incomingName, incomingTimeUnit, incomingDataType, outgoingName, outgoingTimeUnit, outgoingDataType)
        .build();
    Schema schema4 = new Schema.SchemaBuilder().setSchemaName("testSchema")
        .addTime(incomingName, incomingTimeUnit, incomingDataType, outgoingName, outgoingTimeUnit, outgoingDataType,
            defaultNullValue)
        .build();
    Schema schema5 = new Schema.SchemaBuilder().setSchemaName("testSchema")
        .addTime(incomingName, incomingTimeUnitSize, incomingTimeUnit, incomingDataType)
        .build();
    Schema schema6 = new Schema.SchemaBuilder().setSchemaName("testSchema")
        .addTime(incomingName, incomingTimeUnitSize, incomingTimeUnit, incomingDataType, defaultNullValue)
        .build();
    Schema schema7 = new Schema.SchemaBuilder().setSchemaName("testSchema")
        .addTime(incomingName, incomingTimeUnitSize, incomingTimeUnit, incomingDataType, outgoingName,
            outgoingTimeUnitSize, outgoingTimeUnit, outgoingDataType)
        .build();
    Schema schema8 = new Schema.SchemaBuilder().setSchemaName("testSchema")
        .addTime(incomingName, incomingTimeUnitSize, incomingTimeUnit, incomingDataType, outgoingName,
            outgoingTimeUnitSize, outgoingTimeUnit, outgoingDataType, defaultNullValue)
        .build();
    Schema schema9 =
        new Schema.SchemaBuilder().setSchemaName("testSchema").addTime(incomingTimeGranularitySpec).build();
    Schema schema10 = new Schema.SchemaBuilder().setSchemaName("testSchema")
        .addTime(incomingTimeGranularitySpec, defaultNullValue)
        .build();
    Schema schema11 = new Schema.SchemaBuilder().setSchemaName("testSchema")
        .addTime(incomingTimeGranularitySpec, outgoingTimeGranularitySpec)
        .build();
    Schema schema12 = new Schema.SchemaBuilder().setSchemaName("testSchema")
        .addTime(incomingTimeGranularitySpec, outgoingTimeGranularitySpec, defaultNullValue)
        .build();

    Assert.assertNotNull(schema1.getTimeFieldSpec());
    Assert.assertNotNull(schema2.getTimeFieldSpec());
    Assert.assertNotNull(schema3.getTimeFieldSpec());
    Assert.assertNotNull(schema4.getTimeFieldSpec());
    Assert.assertNotNull(schema5.getTimeFieldSpec());
    Assert.assertNotNull(schema6.getTimeFieldSpec());
    Assert.assertNotNull(schema7.getTimeFieldSpec());
    Assert.assertNotNull(schema8.getTimeFieldSpec());
    Assert.assertNotNull(schema9.getTimeFieldSpec());
    Assert.assertNotNull(schema10.getTimeFieldSpec());
    Assert.assertNotNull(schema11.getTimeFieldSpec());
    Assert.assertNotNull(schema12.getTimeFieldSpec());

    Assert.assertEquals(schema1, schema5);
    Assert.assertEquals(schema1, schema9);
    Assert.assertEquals(schema2, schema6);
    Assert.assertEquals(schema2, schema10);
    Assert.assertEquals(schema3, schema7);
    Assert.assertEquals(schema3, schema11);
    Assert.assertEquals(schema4, schema8);
    Assert.assertEquals(schema4, schema12);

    // Before adding default null value.
    Assert.assertFalse(schema1.equals(schema2));
    Assert.assertFalse(schema3.equals(schema4));
    Assert.assertFalse(schema5.equals(schema6));
    Assert.assertFalse(schema7.equals(schema8));
    Assert.assertFalse(schema9.equals(schema10));
    Assert.assertFalse(schema11.equals(schema12));

    // After adding default null value.
    schema1.getTimeFieldSpec().setDefaultNullValue(defaultNullValue);
    schema3.getTimeFieldSpec().setDefaultNullValue(defaultNullValue);
    schema5.getTimeFieldSpec().setDefaultNullValue(defaultNullValue);
    schema7.getTimeFieldSpec().setDefaultNullValue(defaultNullValue);
    schema9.getTimeFieldSpec().setDefaultNullValue(defaultNullValue);
    schema11.getTimeFieldSpec().setDefaultNullValue(defaultNullValue);
    Assert.assertEquals(schema1, schema2);
    Assert.assertEquals(schema3, schema4);
    Assert.assertEquals(schema5, schema6);
    Assert.assertEquals(schema7, schema8);
    Assert.assertEquals(schema9, schema10);
    Assert.assertEquals(schema11, schema12);
  }

  @Test
  public void testSerializeDeserialize()
      throws IOException, IllegalAccessException {
    Schema newSchema;

    newSchema = Schema.fromString(schema.getJSONSchema());
    Assert.assertEquals(newSchema, schema);
    Assert.assertEquals(newSchema.hashCode(), schema.hashCode());

    newSchema = SchemaUtils.fromZNRecord(SchemaUtils.toZNRecord(schema));
    Assert.assertEquals(newSchema, schema);
    Assert.assertEquals(newSchema.hashCode(), schema.hashCode());
  }

  @Test
  public void testSimpleDateFormat() throws IOException {
    TimeGranularitySpec incomingTimeGranularitySpec = new TimeGranularitySpec(DataType.STRING, 1,
        TimeUnit.DAYS, TimeFormat.SIMPLE_DATE_FORMAT + ":yyyyMMdd" , "Date");
    TimeGranularitySpec outgoingTimeGranularitySpec = new TimeGranularitySpec(DataType.STRING, 1,
        TimeUnit.DAYS, TimeFormat.SIMPLE_DATE_FORMAT + ":yyyyMMdd", "Date");
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testSchema")
        .addTime(incomingTimeGranularitySpec, outgoingTimeGranularitySpec).build();
    String jsonSchema = schema.getJSONSchema();
    Schema schemaFromJson = Schema.fromString(jsonSchema);
    Assert.assertEquals(schemaFromJson, schema);
    Assert.assertEquals(schemaFromJson.hashCode(), schema.hashCode());
  }
}
