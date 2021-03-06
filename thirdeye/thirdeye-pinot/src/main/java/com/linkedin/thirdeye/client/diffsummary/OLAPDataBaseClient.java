package com.linkedin.thirdeye.client.diffsummary;

import java.util.List;

import org.joda.time.DateTime;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.datasource.MetricExpression;

public interface OLAPDataBaseClient {

  public void setCollection(String collection);

  public void setMetricExpression(MetricExpression metricExpressions);

  public void setBaselineStartInclusive(DateTime dateTime);

  public void setBaselineEndExclusive(DateTime dateTime);

  public void setCurrentStartInclusive(DateTime dateTime);

  public void setCurrentEndExclusive(DateTime dateTime);

  public Row getTopAggregatedValues() throws Exception;

  public List<List<Row>> getAggregatedValuesOfDimension(Dimensions dimensions) throws Exception;

  public List<List<Row>> getAggregatedValuesOfLevels(Dimensions dimensions) throws Exception;
}
