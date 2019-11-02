package flink;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.calcite.shaded.com.google.common.base.Functions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

import flink.FlinkUtils.Gate;
import flink.FlinkUtils.VehicleDetection;
import flink.FlinkUtils.VehicleDetectionSchema;
import org.codehaus.janino.Java;

import java.io.IOException;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.Queue;

public class FlinkExample {

  public static class SpeedingTicketAcc {
    Queue<Tuple2<VehicleDetection, Gate>> acc = new PriorityQueue<>((o1, o2) ->
        o1.f0.ts.compareTo(o2.f0.ts));
  }

  public static class SpeedingTicketsFunc implements AggregateFunction<Tuple2<VehicleDetection, Gate>, SpeedingTicketAcc, Map<String, Integer>> {
    @Override
    public SpeedingTicketAcc createAccumulator() {
      return new SpeedingTicketAcc();
    }

    @Override public SpeedingTicketAcc add(final Tuple2<VehicleDetection, Gate> value,
        final SpeedingTicketAcc accumulator) {
      accumulator.acc.add(value);
      return accumulator;
    }

    @Override public Map<String, Integer> getResult(final SpeedingTicketAcc accumulator) {
      return null;
    }

    @Override public SpeedingTicketAcc merge(final SpeedingTicketAcc a, final SpeedingTicketAcc b) {
      a.acc.addAll(b.acc);
      return a;
    }
  }

  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

    Properties props = new Properties();
    props.setProperty("zookeeper.connect", "localhost:2181");
    props.setProperty("bootstrap.servers", "localhost:9092");
    // not to be shared with another job consuming the same topic
    props.setProperty("group.id", "flink-group");

    FlinkKafkaConsumer011<VehicleDetection> kafkaConsumer = new FlinkKafkaConsumer011<>(
            "origin", new VehicleDetectionSchema(), props);

    DataStream<VehicleDetection> vehicleStream = env.addSource(kafkaConsumer);

    TableSource csvSource = CsvTableSource.builder()
        .path("gate_pos.csv")
        .lineDelimiter("\n")
        .fieldDelimiter(",")
        .field("gate", TypeInformation.of(Integer.class))
        .field("position", TypeInformation.of(Double.class))
        .build();

    // register the TableSource as table "CsvTable"
    tableEnv.registerTableSource("gate_positions", csvSource);

    Table t = tableEnv.scan("gate_positions")
        .select("gate, position");

    DataStream<Gate> positionStream = tableEnv.toAppendStream(t, Row.class)
        .map(value -> new Gate((int) value.getField(0), (double) value.getField(1)));

    final SingleOutputStreamOperator<Map<String, Integer>> process =
        vehicleStream.join(positionStream)
            .where(value -> value.gate)
            .equalTo(value -> value.gate)
            .window(GlobalWindows.create())
            .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
            .apply(new JoinFunction<VehicleDetection, Gate, Tuple2<VehicleDetection, Gate>>() {
              @Override public Tuple2<VehicleDetection, Gate> join(final VehicleDetection first, final Gate second) throws Exception {
                return new Tuple2<>(first, second);
              }
            })
            .keyBy(tuple -> tuple.f0.plate)
            .window(GlobalWindows.create())
            .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
            .aggregate(new SpeedingTicketsFunc());


    // just write to stdout
    process.writeUsingOutputFormat(new OutputFormat<Map<String, Integer>>() {
      @Override public void configure(final Configuration parameters) {
      }

      @Override public void open(final int taskNumber, final int numTasks) throws IOException {
      }

      @Override public void writeRecord(final Map<String, Integer> record) throws IOException {
        System.out.println(record);
      }

      @Override public void close() throws IOException {
      }
    });

    env.execute("FlinkJob1");
  }
}