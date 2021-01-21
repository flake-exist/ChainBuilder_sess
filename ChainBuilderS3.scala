import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

import CONSTANTS._

object ChainBuilderS3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("ChainBuilder on S3").getOrCreate()
    import spark.implicits._

    val channel_creator_udf = spark.udf.register("channel_creator_udf", channel_creator1)
    val searchInception_udf = spark.udf.register("searchInception_udf", searchInception)
    val htsTube_udf = spark.udf.register("htsTube_udf", htsTube)
    val channelTube_udf = spark.udf.register("channelTube_udf", channelTube)
    val path_creator_udf = spark.udf.register("path_creator_udf", pathCreator)

    val optionsMap  = argsPars(args) //Parse input arguments from command line
    val validMap    = argsValid(optionsMap) // Valid input arguments and their types

    // Cast dates to correct Time Zone and then to UNIX time
    val date_base  = DateWork(validMap("date_tHOLD").head.toString, validMap("date_start").head.toString, validMap("date_finish").head.toString)

    // Validate date chronology
    val(date_tHOLDValid, date_startValid, date_finishValid) = date_base.correct_chronology match {
      case true  => (date_base.get_tHOLD,date_base.get_start,date_base.get_finish)
      case false => throw new Exception(s"Incorrect date chronology. Check input dates")
    }

    val data = spark.read.
      format("parquet").
      option("inferSchema","false").
      option("mergeSchema","true").
      load(validMap("flat_path").map(_.toString):_*) // CONNECT TO S3 NOT FILES

    val data_work = data.select(
      $"ProjectID".cast(sql.types.StringType),
      $"ClientID".cast(sql.types.StringType),
      $"HitTimeStamp".cast(sql.types.LongType),
      $"utm_source".cast(sql.types.StringType),
      $"utm_medium".cast(sql.types.StringType),
      $"utm_campaign".cast(sql.types.StringType),
      $"utm_content".cast(sql.types.StringType),
      $"utm_term".cast(sql.types.StringType),
      $"interaction_type".cast(sql.types.StringType),
      $"profile_id".cast(sql.types.StringType),
      $"creative_id".cast(sql.types.StringType),
      $"ad_id".cast(sql.types.StringType),
      $"goal".cast(sql.types.StringType),
      $"src".cast(sql.types.StringType),
      $"ga_sessioncount".cast(sql.types.StringType)
    )

    val data_custom_0 = data_work.
      filter($"HitTimeStamp" >= date_tHOLDValid && $"HitTimeStamp" < date_finishValid).
      filter($"ProjectID".isin(validMap("projectID"):_*)).
      filter($"goal".isNull || $"goal".isin(validMap("target_numbers"):_*)).
      filter($"src".isin(validMap("source_platform"):_*))

    val data_custom_1  = validMap("product_name") match {
      case productList @ x :: tail => data_custom_0.filter($"ga_location".isin(productList:_*))
      case _                       => data_custom_0
    }

    val data_preprocess_0 = data_custom_1.withColumn("channel",channel_creator_udf(
      lit(validMap("channel_depth").head.toString),
      $"src",
      $"interaction_type",
      $"utm_source",
      $"utm_medium",
      $"utm_campaign",
      $"utm_content",
      $"utm_term",
      $"profileID",
      $"ga_sessioncount",
      $"creative_id",
      $"ad_id" )).select(
      $"ClientID",
      $"HitTimeStamp",
      $"goal",
      $"channel"
    )

    val data_preprocess_1 = data_preprocess_0.withColumn("conversion",
      when($"goal".isin(validMap("target_numbers"):_*),CONVERSION_SYMBOL).otherwise(NO_CONVERSION_SYMBOL)).
      select($"ClientID",
        $"HitTimeStamp",
        $"conversion",
        $"channel").sort($"ClientID", $"HitTimeStamp".asc).
      cache()

    val actorsID = data_preprocess_1.
      filter($"HitTimeStamp" >= date_startValid && $"HitTimeStamp" < date_finishValid).
      filter($"conversion" === CONVERSION_SYMBOL).
      select($"ClientID").
      distinct()

    val data_bulk = validMap("achieve_mode").head match {

      case true => data_preprocess_1.as("df1").
        join(actorsID.as("df2"),($"df1.ClientID" === $"df2.ClientID"),"inner").
        select($"df1.*")

      case false => {val allID = data_preprocess_1.select($"ClientID").distinct()
        val notConvertedID = allID.except(actorsID)
        data_preprocess_1.as("df1").
          join(notConvertedID.as("df2"),($"df1.ClientID" === $"df2.ClientID"),"inner").
          select($"df1.*")
      }
    }

    val data_union = data_bulk.withColumn("channel_conv",concat($"channel",lit(GLUE_SYMBOL),$"conversion"))

    //Create new metric `touch_data`. `touch_data` contains information about `ClientID` `HitTimeStamp` and the type of contact (`channel_conv`) with the channel
    val data_touch = data_union.withColumn("touch_data",map($"channel_conv",$"HitTimeStamp"))

    //Group `touch_data` in sequence by each `ClientID`
    val data_group = data_touch.groupBy($"ClientID").agg(collect_list($"touch_data").as("touch_data_arr"))








  }
}
