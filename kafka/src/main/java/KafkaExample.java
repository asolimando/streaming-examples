import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.kstream.KTable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaExample {

  private static final String SEPARATOR = ",";

  static public class VehicleDetectionMessage {
    public String plate;
    public int gate;
    public int lane;
    public String ts;
    public String nation;

    @Override public String toString() {
      return plate + " " + gate + " " + lane + " " + ts + " " + nation;
    }
  }

  private static Properties getProperties() {
    final Properties streamingConfig = new Properties();

    streamingConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "KafkaJob");
    streamingConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    streamingConfig.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamingConfig.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    return streamingConfig;
  }

  public static class CSVSerializer implements Serializer<VehicleDetectionMessage>{

    @Override public void configure(final Map<String, ?> configs, final boolean isKey) { }

    @Override public byte[] serialize(final String topic, final VehicleDetectionMessage data) {
      if (data == null) return null;

      return (data.plate + SEPARATOR + data.gate + SEPARATOR + data.lane + SEPARATOR + data.ts +
          SEPARATOR + data.nation).getBytes();
    }

    @Override public void close() { }
  }

  public static class CSVDeserializer implements Deserializer<VehicleDetectionMessage>{

    @Override public void configure(final Map<String, ?> configs, final boolean isKey) { }

    @Override public VehicleDetectionMessage deserialize(final String topic, final byte[] data) {
      final VehicleDetectionMessage msg = new VehicleDetectionMessage();
      final String[] elems = new String(data).split(SEPARATOR);

      System.out.println(Arrays.toString(elems));

      msg.plate = elems[0];
      msg.gate = Integer.parseInt(elems[1]);
      msg.lane = (int) Double.parseDouble(elems[2]);
      msg.ts = elems[3];
      msg.nation = elems[4];

      System.out.println(msg);

      return msg;
    }

    @Override public void close() { }
  }

  public static class MapSerializer<S, T> implements Serializer<Map<S, T>> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) { }

    @Override
    public byte[] serialize(final String topic, final Map<S, T> data) {
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
    public void close() { }
  }

  public static class MapDeserializer<S, T> implements Deserializer<Map<S, T>> {

      @Override
      public void configure(Map<String, ?> configs, boolean isKey) { }

      @Override
      public Map<S, T> deserialize(final String topic, final byte[] data) {
        if (data == null) return null;

        final ByteArrayInputStream bis = new ByteArrayInputStream(data);

        try {
          final ObjectInput in = new ObjectInputStream(bis);
          return (Map<S, T>) in.readObject();
        } catch (ClassNotFoundException | IOException e) {
          return null;
        } finally {
          try {
            bis.close();
          } catch (IOException e) {
            return null;
          }
        }
      }

      @Override
      public void close() { }
  }

  public static void main(String[] args) {
    // Create an instance of StreamsConfig from the Properties instance
    StreamsConfig config = new StreamsConfig(getProperties());
    final Serde <String> stringSerde = Serdes.String();
    final Serde <Long> longSerde = Serdes.Long();

    // define countryMessageSerde
    Map<String, Object> serdeProps = new HashMap<>();
    final Serializer<VehicleDetectionMessage> vehicleMsgSerializer = new CSVSerializer();
    serdeProps.put("CSVLineClass", VehicleDetectionMessage.class);
    vehicleMsgSerializer.configure(serdeProps, false);

    final Deserializer<VehicleDetectionMessage> vehicleMsgDeserializer = new CSVDeserializer();
    serdeProps.put("CSVLineClass", VehicleDetectionMessage.class);
    vehicleMsgDeserializer.configure(serdeProps, false);
    final Serde<VehicleDetectionMessage> countryMessageSerde = Serdes.serdeFrom(vehicleMsgSerializer, vehicleMsgDeserializer);

    KStreamBuilder builder = new KStreamBuilder();

/*
    Deserializer<String> deserializer = new StringDeserializer();
    Serializer<String> serializer = new StringSerializer();

    final Serde<String> serde = Serdes.serdeFrom(serializer, deserializer);
*/
    final Serde<VehicleDetectionMessage> serde = Serdes.serdeFrom(vehicleMsgSerializer, vehicleMsgDeserializer);

    final Serde<Map<String, Long>> serdeResMap =
        Serdes.serdeFrom(new MapSerializer<String, Long>(), new MapDeserializer<String, Long>());

    final Properties streamingConfig = getProperties();

    KStream<String, VehicleDetectionMessage> stream = builder.stream(Serdes.String(), serde, "origin");

    final KStream<String, VehicleDetectionMessage> keepKnownNationality = stream
        .filterNot((k, v) -> (v.nation.equals("?")));

    final KGroupedStream<String, VehicleDetectionMessage> group =
        keepKnownNationality.groupByKey(stringSerde, serde);

    final KTable<String, Map<String, Long>> countNat = group.aggregate(
            () -> new HashMap<String, Long>(),
            (String key, VehicleDetectionMessage value, Map<String, Long> aggregate) -> {
              aggregate.putIfAbsent(
                  value.nation,
                  aggregate.getOrDefault(value.nation,new Long(0L)) + 1L
              );
              return aggregate;
            },
        serdeResMap, "countNat"
    );

    countNat.to(stringSerde, serdeResMap, "kafka-destination");

    final KafkaStreams streams = new KafkaStreams(builder, streamingConfig);
    streams.start();
  }
}
