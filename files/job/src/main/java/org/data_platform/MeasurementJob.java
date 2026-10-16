package org.data_platform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.connector.pulsar.common.config.PulsarOptions;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.sink.PulsarSink;
import org.apache.flink.connector.pulsar.sink.writer.message.PulsarMessage;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/** Small stateful reference workload; all credentials are read from local files. */
public class MeasurementJob {
    static final ObjectMapper JSON = new ObjectMapper();

    public static String digest(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) { throw new IllegalStateException("SHA-256 unavailable"); }
    }

    public static JsonNode parse(String raw) {
        try {
            JsonNode value = JSON.readTree(raw);
            if (value == null || !value.isObject()
                    || !value.path("event_id").isTextual()
                    || !value.path("event_id").asText().matches("[A-Za-z0-9_-]{1,128}")
                    || !value.path("sensor").isTextual()
                    || !value.path("sensor").asText().matches("[A-Za-z0-9_-]{1,64}")
                    || !value.path("value").isNumber()
                    || !Double.isFinite(value.path("value").asDouble())
                    || Math.abs(value.path("value").asDouble()) > 1e9)
                throw new IllegalArgumentException("invalid_measurement");
            return value;
        } catch (Exception e) { throw new IllegalArgumentException("invalid_measurement"); }
    }

    public static String key(String raw) {
        try { return parse(raw).path("sensor").asText(); }
        catch (IllegalArgumentException e) { return "__invalid__"; }
    }

    public static String rejected(String raw, String reason) {
        ObjectNode result = JSON.createObjectNode();
        result.put("event_id", "rejected-"+digest(raw));
        result.put("record_type", "rejected");
        result.put("reason", reason);
        result.put("payload_sha256", digest(raw));
        return result.toString();
    }

    public static class Aggregate extends KeyedProcessFunction<String, String, String> {
        private transient MapState<String, Double> seen;
        private transient ValueState<Long> count;
        private transient ValueState<Double> sum;

        @Override public void open(Configuration parameters) {
            seen = getRuntimeContext().getMapState(new MapStateDescriptor<>("event-values", Types.STRING, Types.DOUBLE));
            count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Types.LONG));
            sum = getRuntimeContext().getState(new ValueStateDescriptor<>("sum", Types.DOUBLE));
        }

        @Override public void processElement(String raw, Context context, Collector<String> out) throws Exception {
            JsonNode input;
            try { input = parse(raw); }
            catch (IllegalArgumentException e) { out.collect(rejected(raw, "invalid_measurement")); return; }
            String id = input.path("event_id").asText();
            double value = input.path("value").asDouble();
            Double previous = seen.get(id);
            if (previous != null) {
                if (Double.compare(previous, value) != 0) out.collect(rejected(raw, "conflicting_event_id"));
                return;
            }
            long nextCount = count.value() == null ? 1 : Math.addExact(count.value(), 1);
            double nextSum = (sum.value() == null ? 0 : sum.value())+value;
            if (!Double.isFinite(nextSum)) throw new IllegalStateException("aggregate_overflow");
            seen.put(id, value); count.update(nextCount); sum.update(nextSum);
            ObjectNode result = JSON.createObjectNode();
            result.put("event_id", "measurement-"+digest(input.path("sensor").asText()+"\0"+id));
            result.put("record_type", "measurement_summary");
            result.put("input_event_id", id);
            result.put("sensor", input.path("sensor").asText());
            result.put("count", nextCount); result.put("sum", nextSum);
            result.put("mean", nextSum/nextCount);
            out.collect(result.toString());
        }
    }

    public static void main(String[] args) throws Exception {
        JsonNode settings = JSON.readTree(Files.readString(java.nio.file.Path.of(args[0])));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 5000));
        DataStream<String> input;
        boolean local = settings.path("mode").asText().equals("local");
        Configuration security = new Configuration();
        if (local) {
            FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(),
                    new Path(settings.path("input_directory").asText()))
                    .monitorContinuously(Duration.ofSeconds(1)).build();
            input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "measurement-files").uid("measurement-files-v1");
        } else {
            String tokenFile = settings.path("token_file").asText();
            security.set(PulsarOptions.PULSAR_AUTH_PLUGIN_CLASS_NAME, "org.apache.pulsar.client.impl.auth.AuthenticationToken");
            security.set(PulsarOptions.PULSAR_AUTH_PARAMS, java.nio.file.Path.of(tokenFile).toUri().toString());
            security.set(PulsarOptions.PULSAR_TLS_TRUST_CERTS_FILE_PATH, settings.path("ca_file").asText());
            security.set(PulsarOptions.PULSAR_TLS_ALLOW_INSECURE_CONNECTION, false);
            security.set(PulsarOptions.PULSAR_TLS_HOSTNAME_VERIFICATION_ENABLE, true);
            PulsarSource<String> source = PulsarSource.builder()
                    .setServiceUrl(settings.path("broker_url").asText())
                    .setAdminUrl(settings.path("admin_url").asText())
                    .setTopics(settings.path("input_topic").asText())
                    .setSubscriptionName(settings.path("subscription").asText())
                    .setStartCursor(StartCursor.earliest())
                    .setDeserializationSchema(new SimpleStringSchema()).setConfig(security).build();
            input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "pulsar-measurements").uid("pulsar-measurements-v1");
        }
        DataStream<String> output = input.keyBy(MeasurementJob::key).process(new Aggregate())
                .name("sensor-count-sum-mean").uid("sensor-aggregates-v1");
        if (local) {
            FileSink<String> sink = FileSink.forRowFormat(new Path(settings.path("output_directory").asText()),
                    new SimpleStringEncoder<String>("UTF-8"))
                    .withRollingPolicy(OnCheckpointRollingPolicy.build()).build();
            output.sinkTo(sink).uid("local-results-v1");
        } else {
            PulsarSink<String> sink = PulsarSink.builder()
                    .setServiceUrl(settings.path("broker_url").asText())
                    .setTopics(settings.path("output_topic").asText())
                    .setConfig(security).setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .setSerializationSchema((String value, org.apache.flink.connector.pulsar.sink.writer.context.PulsarSinkContext context) -> {
                        try {
                            return PulsarMessage.builder(value.getBytes(StandardCharsets.UTF_8))
                                    .property("event_id", JSON.readTree(value).path("event_id").asText()).build();
                        } catch (Exception e) { throw new IllegalArgumentException("invalid_result"); }
                    }).build();
            output.sinkTo(sink).uid("pulsar-results-v1");
        }
        env.execute("data-platform-measurements-v1");
    }
}
