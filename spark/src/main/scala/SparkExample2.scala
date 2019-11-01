import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

object SparkExample2 {

  def main(args: Array[String]) {

    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkTrafficJob2")
      .getOrCreate()
    var df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "origin")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    df = df.select(
      col("key").cast("string"),
      split(df("value").cast("string"), ",") as "csv",
      col("timestamp")
    )
      .select(
        col("key"),
        col("csv")(0).cast("string") as("plate"),
        col("csv")(1).cast("int") as("gate"),
        col("csv")(2).cast("int") as("lane"),
        unix_timestamp(col("csv")(3), "yyyy-MM-dd HH:mm:ss").cast("long") as("ts"),
        col("csv")(4).cast("string") as("nation"),
        col("timestamp")
      )

    df.printSchema()

    val nationality_df = df.groupBy("nation")
      .count()
      .select(col("nation"), col("count"))
      .orderBy(desc("count"))

    val ds = nationality_df
      .writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .start()

    ds.awaitTermination()
  }
}