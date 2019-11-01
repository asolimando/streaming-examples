import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object SparkExample {

  val locations: Seq[String] = Seq(
    "1,124.7",
    "2,74.6",
    "3,124.7",
    "4,46.5",
    "5,106.1",
    "8,117.8",
    "9,31.8",
    "10,54.5",
    "12,113.8",
    "13,134.5",
    "15,98.4",
    "16,60.5",
    "18,66.8",
    "20,88.4",
    "21,20.8",
    "22,135.3",
    "23,80.8",
    "25,106.1",
    "26,48.5",
    "27,22.6",
    "28,31.8"
  )

  def main(args: Array[String]) {

    import org.apache.log4j.{Level, Logger}

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkTrafficJob")
      .getOrCreate()
    var df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "origin")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    val parLoc = locations.toList.map(_.split(",")).map(Row.fromSeq(_))

    val loc =
      spark.createDataFrame(
        spark.sparkContext.parallelize(parLoc),
        StructType(
          Array(
            StructField("gate", StringType, true),
            StructField("loc", StringType, true)
          )
        )
      ).toDF
        .select(
          col("gate") cast("int") as "gate",
          col("loc") cast("double") as "loc"
        )

    val countNumSpeedingUDF = udf((l: Seq[Row]) => {
      val locProj = l.view.map{case Row(_: Int, loc: Double, ts: Long) => (loc, ts)}
                     .sortBy(_._2)

      locProj.zip(locProj.drop(1))
        .filterNot{case ((loc1, _), (loc2, _)) => loc1 == loc2} // avoid same location, if ever
        .map{case ((loc1, ts1), (loc2, ts2)) => Math.abs(loc2 - loc1) / (ts2 - ts1).toDouble * 3600} // speed in km/h
        .filter(_ > 130)
        .toList
    })

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
      //.withWatermark("timestamp", "10 minutes") // 10m is enough for speed limit violation detection

    df.printSchema()
    loc.printSchema()

    val speed = df.join(loc, df("gate") === loc("gate"))
      .groupBy("plate", "nation")
      .agg(collect_list(struct(df("gate"), loc("loc"), df("ts")))  as "info", count(df("gate")) as "cnt")
      .select(
        col("nation"),
        countNumSpeedingUDF(col("info")) as "speed_values",
        col("cnt")
      )
      .filter(size(col("speed_values")) > 0)
      .select(
        col("nation"),
        size(countNumSpeedingUDF(col("info"))) as "fines"
      )
      .groupBy("nation")
      .agg(sum(col("fines")).as("tot_fines"))

    val ds = speed.writeStream
      .format("console")
      .outputMode(OutputMode.Update())
      .start()

    ds.awaitTermination()
  }
}