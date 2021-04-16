package org.beamcollege.dataflow_monitoring.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface EventProcessingOptions extends PipelineOptions {

    @Description("Pub/Sub subscription name")
    @Validation.Required
    String getSubscriptionName();
    void setSubscriptionName(String value);

    @Description("Project name")
    @Validation.Required
    String getProjectName();
    void setProjectName(String value);

    @Description("Dataset name")
    @Validation.Required
    String getDatasetName();
    void setDatasetName(String value);

    @Description("Table name")
    @Validation.Required
    String getTableName();
    void setTableName(String value);

    @Description("Pub/Sub error output topic")
    @Validation.Required
    String getErrorTopicName();
    void setErrorTopicName(String value);

}