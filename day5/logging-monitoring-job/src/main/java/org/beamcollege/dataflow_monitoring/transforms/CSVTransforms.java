package org.beamcollege.dataflow_monitoring.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.beamcollege.dataflow_monitoring.model.Event;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class CSVTransforms {

    public static class ReadEventsFromCsvLine extends PTransform<PCollection<String>, PCollection<Event>> {

        @Override
        public PCollection<Event> expand(PCollection<String> input) {
            return input.apply(ParDo.of(new DoFn<String, Event>() {
                DateTimeFormatter dtf;

                @Setup
                public void setup() {
                    dtf = ISODateTimeFormat.dateTime();
                }

                @ProcessElement
                public void processElement(@Element String line, OutputReceiver<Event> out) {
                    String[] els = line.split(",");
                    Event event = new Event(dtf.print(System.currentTimeMillis()),
                            els[1], els[2], els[3], Double.parseDouble(els[4]));
                    out.output(event);
                }
            }));
        }
    }

    public static class WriteCsvFromEvents extends PTransform<PCollection<Event>, PCollection<String>> {

        @Override
        public PCollection<String> expand(PCollection<Event> input) {
            return input.apply(ParDo.of(new DoFn<Event, String>() {

                @ProcessElement
                public void processElement(@Element Event event, OutputReceiver<String> out) {
                    out.output(event.toCSV());
                }
            }));
        }
    }
}
