package org.beamcollege.dataflow_monitoring;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.beamcollege.dataflow_monitoring.model.Event;
import org.beamcollege.dataflow_monitoring.options.EventProcessingOptions;
import org.beamcollege.dataflow_monitoring.transforms.CSVTransforms;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.beam.sdk.values.TypeDescriptors.*;

public class EventProcessingPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(EventProcessingPipeline.class);

    public static void main(String[] args) {

        EventProcessingOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(EventProcessingOptions.class);
        Pipeline p = Pipeline.create(options);

        // Retrieve options from the command line

        String subscriptionName = options.getSubscriptionName();
        String errorTopicName = options.getErrorTopicName();
        String projectName = options.getProjectName();
        String datasetName = options.getDatasetName();
        String tableName = options.getTableName();

        // Read from Pub/Sub and create 10-second windows

        PCollection<Event> events = p
                .apply("Reading from Pub/Sub", PubsubIO.readStrings().fromSubscription(subscriptionName))
                .apply("Windowing", Window.into(FixedWindows.of(Duration.standardSeconds(10))))
                .apply("Converting line from CSV", new CSVTransforms.ReadEventsFromCsvLine());

        // Filter bad events (pressure < 10) from good events

        TupleTag<Event> goodEventsTupleTag = new TupleTag<Event>(){};
        TupleTag<Event> badEventsTupleTag = new TupleTag<Event>(){};

        PCollectionTuple eventsTuple = events
                .apply("Filter bad events", ParDo.of(new DoFn<Event, Event>() {

                    private final Counter counter = Metrics.counter(EventProcessingPipeline.class, "bad-counter");

                    @ProcessElement
                    public void processElement(ProcessContext ctx) {
                        Event event = ctx.element();
                        Double dataValue = event.getDataValue();
                        if (dataValue < 10) {
                            counter.inc();
                            if (dataValue < 0) {
                                LOG.warn("Received negative pressure value ({}) from factory {}", dataValue, event.getFactoryCode());
                            }
                            ctx.output(badEventsTupleTag, event);
                        }
                        else {
                            ctx.output(goodEventsTupleTag, event);
                        }
                    }
                }).withOutputTags(goodEventsTupleTag, TupleTagList.of(badEventsTupleTag)));


        // The bad events will be written to a Pub/Sub topic, and a custom metric is created to track their number

        PCollection<Event> badEvents = eventsTuple.get(badEventsTupleTag);

        badEvents
                .apply("Convert to CSV", new CSVTransforms.WriteCsvFromEvents())
                .apply("Write to PubSub", PubsubIO.writeStrings().to(errorTopicName));


        // With the good events, we compute a mean by key and write the result to BigQuery

        PCollection<Event> goodEvents = eventsTuple.get(goodEventsTupleTag);

        PCollection<KV<String, Double>> kvEvents = goodEvents
                .apply("To KV", MapElements
                        .into(TypeDescriptors.kvs(strings(), doubles()))
                        .via((event) -> KV.of(event.getFactoryCode(), event.getDataValue()))
                );

        PCollection<KV<String, Double>> means = kvEvents.apply("Compute means", Mean.perKey());

        PCollection<TableRow> rows = means.apply("Convert to table rows", ParDo.of(new DoFn<KV<String, Double>, TableRow>() {
                    DateTimeFormatter dtf;
                    @Setup
                    public void setup() {
                        dtf = ISODateTimeFormat.dateTime();
                    }
                    @ProcessElement
                    public void processElement(@Element KV<String, Double> kv, OutputReceiver<TableRow> out) {
                        String timestamp = dtf.print(System.currentTimeMillis());
                        TableRow tableRow = new TableRow();
                        tableRow.set("factoryCode", kv.getKey());
                        tableRow.set("mean", kv.getValue());
                        tableRow.set("timestamp", timestamp);
                        out.output(tableRow);
                    }
                }));

        rows.apply(
                "Write means to BQ",
                BigQueryIO.writeTableRows()
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .to(String.format("%s:%s.%s", projectName, datasetName, tableName))
                        .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS));

        p.run();

    }
}
