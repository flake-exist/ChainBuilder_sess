import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import java.time._

import CONSTANTS._


object LegoChainBuilder {
  def main(args : Array[String]) : Unit = {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    import spark.implicits._


    val channel_creator_udf = spark.udf.register("channel_creator_udf", channel_creator1)
    val searchInception_udf = spark.udf.register("searchInception_udf", searchInception)
    val htsTube_udf = spark.udf.register("htsTube_udf", htsTube)
    val channelTube_udf = spark.udf.register("channelTube_udf", channelTube)
    val path_creator_udf = spark.udf.register("path_creator_udf", pathCreator)


    val optionsMap  = argsPars(args, "j") //Parse input arguments from command line
    val validMap    = argsValid(optionsMap) // Validation input arguments types and building Map

    val dates_window = DatesWindow(validMap("date_tHOLD").head.toString,
                                   validMap("date_start").head.toString,
                                   validMap("date_finish").head.toString)

    val(date_tHOLDValid,date_startValid,date_finishValid) = dates_window.correct_chronology match {
      case true => (dates_window.get_tHOLD,dates_window.get_start,dates_window.get_finish)
      case false => throw new Exception("problem with DATES !!!!!")
    }

    validMap("channel_depth").head.toString match {
      case PROFILEID | SESSIONID | TRANSACTIONID => true
      case _                                     => new Exception(s"`channel_depth does not have such value.Maybe you mean $PROFILEID,$SESSIONID or $TRANSACTIONID")
    }


    val data = spark.read.
      format("parquet").
      option("inferSchema","false").
      option("mergeSchema","true").
      load(validMap("flat_path").map(_.toString):_*)

    val data_work = data.select(
      $"ProjectID".cast(sql.types.LongType),
      $"ClientID".cast(sql.types.StringType),
      $"HitTimeStamp".cast(sql.types.LongType),
      $"ga_sourcemedium".cast(sql.types.StringType),
      $"utm_source".cast(sql.types.StringType),
      $"utm_medium".cast(sql.types.StringType),
      $"utm_campaign".cast(sql.types.StringType),
      $"interaction_type".cast(sql.types.StringType),
      $"profile_id".cast(sql.types.StringType),
      $"ga_location".cast(sql.types.StringType),
      $"goal".cast(sql.types.LongType),
      $"src".cast(sql.types.StringType),
      $"SessionID".cast(sql.types.StringType)
    )

    val data_custom_0 = data_work.
      filter($"HitTimeStamp" >= date_tHOLDValid && $"HitTimeStamp" < date_finishValid).
      filter($"ProjectID".isin(validMap("projectID"):_*)).
      filter($"goal".isNull || $"goal".isin(validMap("target_numbers"):_*)).
      filter($"src".isin(validMap("source_platform"):_*))

    val data_custom_1  = validMap("product_name") match {
      case List("null")                 => data_custom_0
      case productList @ x :: tail => data_custom_0.filter($"ga_location".isin(productList:_*))
      case _ => data_custom_0
    }


    val data_preprocess_0 = data_custom_1.withColumn("channel",channel_creator_udf(
      lit(validMap("channel_depth").head.toString),
      $"src",
      $"ga_sourcemedium",
      $"utm_source",
      $"utm_medium",
      $"utm_campaign",
      $"interaction_type",
      $"profile_id",
      $"sessionID")).select(
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
//
    val actorsID = data_preprocess_1.
      filter($"HitTimeStamp" >= date_startValid && $"HitTimeStamp" < date_finishValid).
      filter($"conversion" === CONVERSION_SYMBOL).
      select($"ClientID").
      distinct()
//
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

    val data_touchTube = data_group.select(
      $"ClientID",
      searchInception_udf($"touch_data_arr",lit(date_startValid),lit(NO_CONVERSION_SYMBOL)).as("touch_data_arr"))

    //Collect `channel` and `HitTimeStamp` sequences
    val data_seq = data_touchTube.select(
      $"ClientID",
      channelTube_udf($"touch_data_arr").as("channel_seq"),
      htsTube_udf($"touch_data_arr",lit(GLUE_SYMBOL_POS),lit(GLUE_SYMBOL_NEG)).as("hts_seq"))

    val data_chain = validMap("achieve_mode").head match {
      case true  => data_seq.select(
        $"ClientID",
        path_creator_udf($"channel_seq",lit("success"),lit(GLUE_SYMBOL_POS),lit(GLUE_SYMBOL_NEG),lit(TRANSIT)).as("channel_paths_arr"),
        path_creator_udf($"hts_seq",lit("success"),lit(GLUE_SYMBOL_POS),lit(GLUE_SYMBOL_NEG),lit(TRANSIT)).as("hts_paths_arr")
      )

      case false => data_seq.select(
        $"ClientID",
        path_creator_udf($"channel_seq",lit("fail"),lit(GLUE_SYMBOL_POS),lit(GLUE_SYMBOL_NEG),lit(TRANSIT)).as("channel_paths_arr"),
        path_creator_udf($"hts_seq",lit("fail"),lit(GLUE_SYMBOL_POS),lit(GLUE_SYMBOL_NEG),lit(TRANSIT)).as("hts_paths_arr")
      )
    }

    //Detailed report about `ClientID` paths
    val data_pathInfo = data_chain.
      withColumn("path_zip_hts",explode(arrays_zip($"channel_paths_arr",$"hts_paths_arr"))).
      withColumn("target_numbers",lit(validMap("target_numbers").mkString(","))).
      select(
        $"ClientID",
        $"path_zip_hts.channel_paths_arr".as("user_path"),
        $"path_zip_hts.hts_paths_arr".as("timeline"),
        $"target_numbers"
      ).cache()

    data_pathInfo.
      write.format("csv").
      option("header","true").
      mode("overwrite").
      save(validMap("output_pathD").head.toString)

    validMap("channel_depth").head.toString match {
      case PROFILEID => {


        //Aggregated report about `ClientID` paths
        val data_agg = data_pathInfo.
          groupBy("user_path").
          agg(count($"ClientID").as("count")).
          sort($"count".desc)

        data_agg.coalesce(1).
          write.format("csv").
          option("header", "true").
          mode("overwrite").
          save(validMap("output_path").head.toString)
      }

      case _ => println("Agg report created only with `channel_depth==PROFILEID`")
    }


  }

}
