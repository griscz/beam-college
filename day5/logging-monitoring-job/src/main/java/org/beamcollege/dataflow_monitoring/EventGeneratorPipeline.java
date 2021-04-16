package org.beamcollege.dataflow_monitoring;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.beamcollege.dataflow_monitoring.model.Event;
import org.beamcollege.dataflow_monitoring.options.EventGeneratorOptions;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.nio.charset.StandardCharsets;
import java.util.Random;

public class EventGeneratorPipeline {

    private static final String[] MACHINES= {"machine1", "machine2", "machine3", "machine4", "machine5", "machine6",
    "machine7", "machine8", "machine9", "machine10", "machine11", "machine12", "machine13"};
    private static final String[] FACTORIES = {"factory1", "factory2", "factory4", "factory5", "factory6", "factory7",
            "factory8", "factory9", "factory10", "factory11"};
    private static final Random rand = new Random();

    public static void main(String[] args) {

        EventGeneratorOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(EventGeneratorOptions.class);
        Pipeline p = Pipeline.create(options);

        String topic = options.getTopicName();
        Integer ops = options.getOps();

        p
            .apply("Generate sequence", GenerateSequence.from(0L).withRate(ops, Duration.standardSeconds(1)))
            .apply("Generate messages",
                    ParDo.of(new DoFn<Long, PubsubMessage>() {

                        final String variableName = "pressureInBar";
                        DateTimeFormatter dtf;

                        @Setup
                        public void setup() {
                            dtf = ISODateTimeFormat.dateTime();
                        }

                        @ProcessElement
                        public void processElement(@Element Long nb, OutputReceiver<PubsubMessage> out) {
                            String machine = MACHINES[rand.nextInt(12)];
                            String factory = FACTORIES[rand.nextInt(10)];

                            Event event = new Event(dtf.print(System.currentTimeMillis()),
                                    factory, machine, variableName, rand.nextGaussian() * 12 + 60);
                            out.output(new PubsubMessage(event.toCSV().getBytes(StandardCharsets.UTF_8), null));
                        }
                    }))
            .apply(PubsubIO.writeMessages().to(topic));

        p.run();
    }
}
