import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class FlinkExample {

  private static final String SEPARATOR = ",";

  static public class VehicleDetection {
    public String plate;
    public int gate;
    public int lane;
    public String ts;
    public String nation;

    public VehicleDetection(String message){
      final String [] tokens = message.split(SEPARATOR);

      this.plate = tokens[0];
      this.gate = Integer.parseInt(tokens[1]);
      this.lane = Integer.parseInt(tokens[2]);
      this.ts = tokens[3];
      this.nation = tokens[4];
    }

    @Override public String toString() {
      return plate + " " + gate + " " + lane + " " + ts + " " + nation;
    }
  }

  public static class PlateCountAccumulator {
    Map<String, Long> plate2count = new HashMap<>();
  }

  public static class PlateCount implements AggregateFunction<VehicleDetection, PlateCountAccumulator, Map<String, Long>> {
    @Override
    public PlateCountAccumulator createAccumulator() {
      return new PlateCountAccumulator();
    }

    @Override
    public PlateCountAccumulator add(VehicleDetection value, PlateCountAccumulator accumulator) {
      final String plate = value.plate;
      final Map<String, Long> map = accumulator.plate2count;

      map.put(plate, map.getOrDefault(plate, 0L) + 1L);

      return accumulator;
    }

    @Override
    public Map<String, Long> getResult(PlateCountAccumulator accumulator) {
      return accumulator.plate2count;
    }

    @Override
    public PlateCountAccumulator merge(PlateCountAccumulator a, PlateCountAccumulator b) {
      final Map<String, Long> aMap = a.plate2count;
      final Map<String, Long> bMap = b.plate2count;

      // we merge b elements into a, if existing, otherwise they are inserted
      bMap.forEach((key, value) -> aMap.merge(key, value, Long::sum));

      return a;
    }
  }

  public static class ResultSerializer implements KeyedSerializationSchema<Map<String, Long>> {
    @Override
    public byte[] serializeKey(Map<String, Long> data) {
      return "key".getBytes();
    }

    @Override
    public byte[] serializeValue(Map<String, Long> data) {
      if (data == null) return null;

      final ByteArrayOutputStream bos = new ByteArrayOutputStream();

      try {
        final ObjectOutput out = new ObjectOutputStream(bos);
        out.writeObject(data);
        out.flush();
        return bos.toByteArray();
      } catch (IOException ex) {
        return null;
      } finally {
        try {
          bos.close();
        } catch (IOException e) {
          return null;
        }
      }
    }

    @Override
    public String getTargetTopic(Map<String, Long> data) {
      // use always the default topic
      return null;
    }
  }

  public static class VehicleDetectionSchema implements DeserializationSchema<VehicleDetection>, SerializationSchema<VehicleDetection> {

    @Override
    public byte[] serialize(VehicleDetection data) {
      if (data == null) return null;

      return (
          data.plate + SEPARATOR +
              data.gate + SEPARATOR +
              data.lane + SEPARATOR +
              data.ts + SEPARATOR +
              data.nation
      ).getBytes();
    }

    @Override
    public VehicleDetection deserialize(byte[] message) {
      return new VehicleDetection(new String(message));
    }

    // not used here
    @Override
    public boolean isEndOfStream(VehicleDetection nextElement) {
      return false;
    }

    @Override
    public TypeInformation<VehicleDetection> getProducedType() {
      return TypeExtractor.getForClass(VehicleDetection.class);
    }
  }

  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // use filesystem based state management
    env.setStateBackend(new FsStateBackend("file:///tmp/flink/checkpoints"));
    // checkpoint works fine if Flink is crashing but does not seem to work if job is restarted?
    env.enableCheckpointing(1000);

    Properties props = new Properties();
    props.setProperty("zookeeper.connect", "localhost:2181");
    props.setProperty("bootstrap.servers", "localhost:9092");
    // not to be shared with another job consuming the same topic
    props.setProperty("group.id", "flink-group");

    FlinkKafkaConsumer011<VehicleDetection> kafkaConsumer = new FlinkKafkaConsumer011<>(
            "origin",
            new VehicleDetectionSchema(),
            props);

    DataStream<VehicleDetection> stream = env.addSource(kafkaConsumer);

    final SingleOutputStreamOperator<Map<String, Long>> process =
            stream.keyBy(value -> value.plate)
            .window(GlobalWindows.create())
            // our trigger should probably be smarter
            .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
            .aggregate(new PlateCount());

    FlinkKafkaProducer011 kafkaProducer = new FlinkKafkaProducer011<>(
            "localhost:9092",
            "flink-destination",
            new ResultSerializer());
    process.addSink(kafkaProducer);

    env.execute("FlinkJob");
  }
}
