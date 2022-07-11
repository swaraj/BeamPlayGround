package org.example;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Iterables;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class BeamPipeline {
    static Logger logger = LoggerFactory.getLogger(App.class);

    static String topic = "topic";
    static String outputTopic = "output";
    static String groupId = "GROUP_ID";
    static String groupId2 = "GROUP_ID_2";
    static String servers = "localhost:29092,localhost:39092";

    public static void main(String[] args) {
        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        pipelineOptions.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        Map<String, Object> consumerConfigMap = new HashMap<>();

        consumerConfigMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfigMap.put(ConsumerConfig.GROUP_ID_CONFIG, "BEAM-1");
        consumerConfigMap.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");

        pipeline
                .apply(
                        KafkaIO.<String, String>read()
                                .withConsumerConfigUpdates(consumerConfigMap)
                                .withBootstrapServers(servers)
                                .withKeyDeserializer(StringDeserializer.class)
                                .withValueDeserializer(StringDeserializer.class)
                                .withTopic(topic))
                .apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                        .via(KafkaRecord::getKV))
                .apply(
                        ParDo.of(
                                new BatchAggregateDoFn(
                                        100,
                                        org.joda.time.Duration.standardMinutes(10))))
                .apply(
                        MapElements.into(
                                        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                                .via(e -> KV.of(e.getKey(), e.getValue() + "")))
                .apply(
                        KafkaIO.<String, String>write()
                                .withBootstrapServers(servers)
                                .withTopic(outputTopic)
                                .withKeySerializer(StringSerializer.class)
                                .withValueSerializer(StringSerializer.class));

        PipelineResult pipelineResult = pipeline.run();

        pipelineResult.waitUntilFinish(org.joda.time.Duration.standardMinutes(10));
    }

    private static class ValueWithTimestamp {
        String value;
        Instant timestamp;

        public ValueWithTimestamp(String value, Instant timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }

        public String getValue() {
            return value;
        }

        public Instant getTimestamp() {
            return timestamp;
        }
    }

    private static class ValueWithTimestampCoder extends CustomCoder<ValueWithTimestamp> {

        private static final InstantCoder INSTANT_CODER = InstantCoder.of();

        public static ValueWithTimestampCoder of(Coder<String> valueCoder) {
            return new ValueWithTimestampCoder(valueCoder);
        }

        private final Coder<String> valueCoder;

        private ValueWithTimestampCoder(Coder<String> valueCoder) {
            this.valueCoder = valueCoder;
        }

        @Override
        public void encode(ValueWithTimestamp value, OutputStream outStream)
                throws IOException {

            valueCoder.encode(value.getValue(), outStream);
            INSTANT_CODER.encode(value.getTimestamp(), outStream);
        }

        @Override
        public ValueWithTimestamp decode(InputStream inStream) throws IOException {
            return new ValueWithTimestamp(valueCoder.decode(inStream), INSTANT_CODER.decode(inStream));
        }

        @Override
        public List<? extends Coder<?>> getCoderArguments() {
            return Collections.singletonList(valueCoder);
        }
    }

    private static class BatchAggregateDoFn extends DoFn<KV<String, String>, KV<String, Integer>> {

        private final int maxBatchSize;
        private final org.joda.time.Duration maxBatchWait;


        @StateId("batch")
        private final StateSpec<BagState<ValueWithTimestamp>> batchSpec =
                StateSpecs.bag(ValueWithTimestampCoder.of(StringUtf8Coder.of()));

        @StateId("batchSize")
        private final StateSpec<ValueState<Integer>> batchSizeSpec = StateSpecs.value();

        @TimerId("flushTimer")
        private final TimerSpec flushTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

        @TimerId("endOfTime")
        private final TimerSpec endOfTimeTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

        BatchAggregateDoFn(int maxBatchSize, org.joda.time.Duration maxBatchWait) {
            this.maxBatchSize = maxBatchSize;
            this.maxBatchWait = maxBatchWait;
        }

        @Setup
        public void setup() {
            // nothing to do
        }

        @Teardown
        public void tearDown() {
            // ideally, we want tearDown to be idempotent
        }

        @ProcessElement
        public void process(
                @Element KV<String, String> input,
                @Timestamp Instant timestamp,
                @StateId("batch") BagState<ValueWithTimestamp> elements,
                @StateId("batchSize") ValueState<Integer> batchSize,
                @TimerId("flushTimer") org.apache.beam.sdk.state.Timer flushTimer,
                @TimerId("endOfTime") org.apache.beam.sdk.state.Timer endOfTimeTimer,
                OutputReceiver<KV<String, Integer>> outputReceiver) {

            endOfTimeTimer.set(GlobalWindow.INSTANCE.maxTimestamp());
            int currentSize = MoreObjects.firstNonNull(batchSize.read(), 0);
            ValueWithTimestamp value = new ValueWithTimestamp(input.getKey() + " " + input.getValue(), timestamp);
            if (currentSize == maxBatchSize - 1) {
                flushOutput(
                        Iterables.concat(elements.read(), Collections.singletonList(value)), outputReceiver);
                clearState(elements, batchSize, flushTimer);
            } else {
                if (currentSize == 0) {
                    flushTimer.offset(maxBatchWait).setRelative();
                }
                elements.add(value);
                batchSize.write(currentSize + 1);
            }
        }

        @OnTimer("flushTimer")
        public void onFlushTimer(
                @StateId("batch") BagState<ValueWithTimestamp> elements,
                @StateId("batchSize") ValueState<Integer> batchSize,
                OutputReceiver<KV<String, Integer>> outputReceiver) {

            flushOutput(elements.read(), outputReceiver);
            clearState(elements, batchSize, null);
        }

        @OnTimer("endOfTime")
        public void onEndOfTimeTimer(
                @StateId("batch") BagState<ValueWithTimestamp> elements,
                @StateId("batchSize") ValueState<Integer> batchSize,
                OutputReceiver<KV<String, Integer>> outputReceiver) {

            flushOutput(elements.read(), outputReceiver);
            // no need to clear state, will be cleaned by the runner
        }

        private void clearState(
                BagState<ValueWithTimestamp> elements,
                ValueState<Integer> batchSize,
                @Nullable Timer flushTimer) {

            elements.clear();
            batchSize.clear();
            if (flushTimer != null) {
                // Beam is currently missing a clear() method for Timer
                // see https://issues.apache.org/jira/browse/BEAM-10887 for updates
                flushTimer.offset(maxBatchWait).setRelative();
            }
        }

        private void flushOutput(
                Iterable<ValueWithTimestamp> elements,
                OutputReceiver<KV<String, Integer>> outputReceiver) {

            Map<String, List<ValueWithTimestamp>> distinctElements =
                    StreamSupport.stream(elements.spliterator(), false).collect(Collectors.groupingBy(ValueWithTimestamp::getValue, Collectors.toList()));

            for (String key : distinctElements.keySet()) {
                List<ValueWithTimestamp> timestampsAndWindows = distinctElements.get(key);
                KV<String, Integer> kv = KV.of(key, timestampsAndWindows.size());
                outputReceiver.output(kv);
            }
        }
    }
}
