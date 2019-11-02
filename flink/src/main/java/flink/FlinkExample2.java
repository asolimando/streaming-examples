package flink;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import flink.FlinkUtils.PlateCountingOutputFormat;
import flink.FlinkUtils.ResultSerializer;
import flink.FlinkUtils.VehicleDetection;
import flink.FlinkUtils.VehicleDetectionSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Counting plate occurrences for the whole dataset
 */
public class FlinkExample2 {

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
            stream.keyBy(value -> value.gate)// split stream "per-gate"
            .window(GlobalWindows.create())
            .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
            .aggregate(new PlateCount());
/*
    FlinkKafkaProducer011<Map<String, Long>> kafkaProducer = new FlinkKafkaProducer011<>(
            "localhost:9092",
            "flink-destination",
            new ResultSerializer());
    process.addSink(kafkaProducer);
*/
    // just write to stdout
    process.writeUsingOutputFormat(new PlateCountingOutputFormat());

    env.execute("FlinkJob2");
  }
}