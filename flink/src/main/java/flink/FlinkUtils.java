package flink;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.Set;

public class FlinkUtils {
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

  static public class Gate {
    public int gate;
    public double position;

    public Gate(int gate, double position){
      this.gate = gate;
      this.position = position;
    }

    @Override public String toString() {
      return gate + " " + position;
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

  public static class VehicleDetectionSchema implements DeserializationSchema<VehicleDetection>,
      SerializationSchema<VehicleDetection> {

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

  public static class PlateCountingOutputFormat implements OutputFormat<Map<String, Long>> {
    @Override public void configure(final Configuration parameters) {
    }

    @Override public void open(final int taskNumber, final int numTasks) throws IOException {
    }

    @Override public void writeRecord(final Map<String, Long> record) throws IOException {
      System.out.println(record);
    }

    @Override public void close() throws IOException {
    }
  }

  public static class SpeedingTicketsOutputFormat implements OutputFormat<Set<String>> {
    @Override public void configure(final Configuration parameters) {
    }

    @Override public void open(final int taskNumber, final int numTasks) throws IOException {
    }

    @Override public void writeRecord(final Set<String> record) throws IOException {
      System.out.println(record);
    }

    @Override public void close() throws IOException {
    }
  }
}
