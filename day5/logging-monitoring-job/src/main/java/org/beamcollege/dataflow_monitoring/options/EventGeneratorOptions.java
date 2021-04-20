package org.beamcollege.dataflow_monitoring.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface EventGeneratorOptions extends PipelineOptions {

    @Description("Pub/Sub topic name")
    @Validation.Required
    String getTopicName();
    void setTopicName(String value);

    @Description("Operations per second")
    @Validation.Required
    Integer getOps();
    void setOps(Integer ops);
}
