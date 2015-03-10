package com.linkedin.thirdeye.bootstrap.rollup.phase4;

import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTreeConfig;

import java.util.ArrayList;
import java.util.List;

public class RollupPhaseFourConfig {
  private List<String> dimensionNames;
  private List<String> metricNames;
  private List<MetricType> metricTypes;
  private List<String> rollupOrder;

  public RollupPhaseFourConfig() {

  }

  /**
   *
   * @param dimensionNames
   * @param metricNames
   * @param metricTypes
   */
  public RollupPhaseFourConfig(List<String> dimensionNames,
      List<String> metricNames, List<MetricType> metricTypes,
      List<String> rollupOrder) {
    super();
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.metricTypes = metricTypes;
  }

  public List<String> getDimensionNames() {
    return dimensionNames;
  }

  public List<String> getMetricNames() {
    return metricNames;
  }

  public List<MetricType> getMetricTypes() {
    return metricTypes;
  }
  public List<String> getRollupOrder() {
    return rollupOrder;
  }

  public static RollupPhaseFourConfig fromStarTreeConfig(StarTreeConfig config)
  {
    List<String> metricNames = new ArrayList<String>(config.getMetrics().size());
    List<MetricType> metricTypes = new ArrayList<MetricType>(config.getMetrics().size());
    for (MetricSpec spec : config.getMetrics())
    {
      metricNames.add(spec.getName());
      metricTypes.add(spec.getType());
    }

    List<String> dimensionNames = new ArrayList<String>(config.getDimensions().size());
    for (DimensionSpec dimensionSpec : config.getDimensions())
    {
      dimensionNames.add(dimensionSpec.getName());
    }

    return new RollupPhaseFourConfig(dimensionNames,
                                     metricNames,
                                     metricTypes,
                                     config.getRollup().getOrder());
  }
}
