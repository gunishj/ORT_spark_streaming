
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object ORT_streaming {
  val spark = SparkSession
    .builder()
    .appName("kafka integration")
    .master("local[2]")
    .getOrCreate()

  val KafkaInputSchema = StructType(Array(
    StructField("emp", StringType),
    StructField("grade", StringType),
    StructField("effective_dt", DateType),
    StructField("suprvsr", StringType),
    StructField("status", StringType),
    StructField("posted_dt", DateType)
  ))

  def readFromKafka()={
    val kafkaDf :DataFrame= spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe","ORG_HIER_STREAM")
      .load()
      .select(col("topic"),expr("cast(value as string) actualValue"))
      .select(from_json(col("actualValue"), KafkaInputSchema).as("input")) // composite column (struct)
      .selectExpr("input.*")
      .selectExpr("emp","grade","effective_dt","suprvsr","status","posted_dt")

    val driver = "org.postgresql.Driver"
    val url = "jdbc:postgresql://localhost:5432/docker"
    val user = "docker"
    val password = "docker"

    kafkaDf.writeStream.foreachBatch{(batch :DataFrame,_:Long)=>
      //each executor can control the batch

      batch.write
        .format("jdbc")
        .option("driver",driver)
        .option("url",url)
        .option("user",user)
        .option("password",password)
        .option("dbtable","public.org_hier_strm_tbl")
        .mode(SaveMode.Append)
        .save()
    }.start().awaitTermination()

  }



  def main(args: Array[String]): Unit = {
    readFromKafka()
  }

}
