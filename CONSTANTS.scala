import java.time._

object CONSTANTS {
  val DATE_PATTERN = "[12]\\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\\d|3[01])".r // Pattern for RegEx . Check correct string `Date` or not
  val DATE_UNIX_TIME_STAMP = new java.text.SimpleDateFormat("yyyy-MM-dd") // Pattern to convert date(String) into Unix Time Stamp
  val CONVERSION_SYMBOL    : String = "get@conv"
  val NO_CONVERSION_SYMBOL : String = "get@no_conv"
  val GLUE_SYMBOL          : String = "::_" //Use it to concatenate `channel` columns with `conversion` columns
  val GLUE_SYMBOL_POS      : String = GLUE_SYMBOL + CONVERSION_SYMBOL //symbol denotes the contact with channel ended up with conversion
  val GLUE_SYMBOL_NEG      : String = GLUE_SYMBOL + NO_CONVERSION_SYMBOL //symbol denotes the contact with channel ended up without conversion
  val TRANSIT              : String = "=>"

  val necessary_args = Map(
    "projectID" -> "Long",
    "date_start" -> "String",
    "date_tHOLD" -> "String",
    "date_finish" -> "String",
    "target_numbers" -> "Long",
    "product_name" -> "String",
    "source_platform" -> "String",
    "achieve_mode" -> "Boolean",
    "flat_path" -> "String",
    "output_path" -> "String",
    "output_pathD" -> "String")


  case class OptionMap(opt: String, value: String) {

    val getOpt: String = opt.split("-") match {
      case (a@Array(_*)) => a.last
      case as: Array[_] => as(0)
      case _ => throw new Exception("Parsing Error")
    }
    val getVal: List[String] = value.split(",").toList.map(_.trim)
  }


  //Parse input arguments from command line.Convert position arguments to named arguments
  def argsPars(args: Array[String], usage: String): collection.mutable.Map[String, List[String]] = {

    if (args.length == 0) {
      throw new Exception(s"Empty argument Array. $usage")
    }

    val (options, _) = args.partition(_.startsWith("-"))
    val interface = collection.mutable.Map[String, List[String]]()

    options.map { elem =>
      val pairUntrust = elem.split("=")
      val pair_trust = pairUntrust match {
        case (p@Array(_, _)) => OptionMap(p(0).trim, p(1).trim)
        case _ => throw new Exception(s"Can not parse $pairUntrust")
      }
      val opt_val = pair_trust.getOpt -> pair_trust.getVal
      interface += opt_val
    }
    interface
  }


  //Check input arguments types
  def argsValid(argsMapped: collection.mutable.Map[String, List[String]]): collection.mutable.Map[String, List[Any]] = {

    val r1 = necessary_args.keys.map(argsMapped.contains(_)).forall(_ == true)

    val validMap = collection.mutable.Map[String, List[Any]]()

    r1 match {
      case true => argsMapped.keys.toList.map { k =>
        necessary_args(k) match {
          case "Long" => try {
            validMap += k -> argsMapped(k).map(_.toLong)
          } catch {
            case _: Throwable => throw new Exception("ERROR")
          }
          case "Boolean" => try {
            validMap += k -> argsMapped(k).map(_.toBoolean)
          } catch {
            case _: Throwable => throw new Exception("ERROR")
          }
          case "String" => try {
            validMap += k -> argsMapped(k).map(_.toString)
          } catch {
            case _: Throwable => throw new Exception("ERROR")
          }
        }
      }
      case false => throw new Exception("Bleat gde argumenti?")
    }
    validMap
  }

  def DateStrToUnix(simmple_date: String): String = {
    val date_correct = DATE_PATTERN.findFirstIn(simmple_date) match {
      case Some(s) => s
      case _ => throw new Exception("Incorrect Date Format.Use YYYY-MM_dd format")
    }
    date_correct
  }

  def localDateToUTC(simmple_date: String, local_format: String = "T00:00:00+03:00[Europe/Moscow]"): Long = {
    val sd = DateStrToUnix(simmple_date)
    val zone_date: String = sd + local_format //local date format with timezone
    val utcZoneId = ZoneId.of("UTC")
    val zonedDateTime = ZonedDateTime.parse(zone_date)
    val utcDateTime = zonedDateTime.withZoneSameInstant(utcZoneId) //UTC date
    val unix_time: Long = utcDateTime.toInstant.toEpochMilli //UNIX time
    unix_time
  }

  case class DatesWindow(date_tHOLD : String, date_start : String, date_finish : String) {

    val get_tHOLD = localDateToUTC(date_tHOLD)
    val get_start = localDateToUTC(date_start)
    val get_finish = localDateToUTC(date_finish)

    val getChronology = List(get_tHOLD, get_start, get_finish)

    val correct_chronology = getChronology match {
      case List(x, y, z) if (x <= y) && (y < z) => true
      case _ => false
    }

  }

  //function check value if it equals null or is empty
  def isEmpty(x:String) = x == "null" || x.isEmpty || x == null

  def channel_creator(
                        src                  : Option[String]=null,
                        ga_sourcemedium      : Option[String]=null,
                        utm_source           : Option[String]=null,
                        utm_medium           : Option[String]=null,
                        utm_campaign         : Option[String]=null,
                        interaction_type     : Option[String]=null,
                        profileID            : Option[String]=null,
                        sessionID            : Option[String]=null,
                        transactionID        : Option[String]=null): String = {

    val channel = src match {
      case Some("adriver") | Some("dcm") if interaction_type == Some("view")  => List(interaction_type,utm_source,utm_medium,utm_campaign,profileID).map(_.getOrElse("none")).mkString(" / ")
      case Some("adriver") | Some("dcm") if interaction_type == Some("click") => List(interaction_type,utm_source,utm_medium,utm_campaign,profileID).map(_.getOrElse("none")).mkString(" / ")
      case Some("seizmik")                       => "seizmik_channel" //ALLERT NEED TO EDIT IN FUTURE!!!
      case Some("ga") | Some("bq")               => List(ga_sourcemedium,utm_campaign,sessionID,transactionID).map(_.getOrElse("none")).mkString(" / ")
      case _                                     => throw new Exception("Unknown data source")
    }
    channel

  }
  val channel_creator1 = (
    src                 : String,
  ga_sourcemedium      : String,
  utm_source           : String,
  utm_medium           : String,
  utm_campaign         : String,
  interaction_type     : String,
  profileID            : String,
  sessionID            : String) => {

    val channel = src match {
      case "adriver" | "dcm" if interaction_type == "view"  => List(interaction_type,utm_source,utm_medium,utm_campaign,profileID).mkString(" / ")
      case "adriver" | "dcm" if interaction_type == "click" => List(interaction_type,utm_source,utm_medium,utm_campaign,profileID).mkString(" / ")
      case "seizmik"                                        => "seizmik_channel" //ALLERT NEED TO EDIT IN FUTURE!!!
      case "ga" | "bq"               => List(ga_sourcemedium,utm_campaign,sessionID).mkString(" / ")
      case _                                     => throw new Exception("Unknown data source")
    }
    channel

  }

  val searchInception = (arr:Seq[Map[String,Long]],date_start:Long,no_conversion:String) => {
        val arr_reverse = arr.reverse
        val (before,after) = arr_reverse.partition(elem =>elem(elem.keySet.head) < date_start)
        val before_sort = before.takeWhile(elem => elem.keySet.head.endsWith(no_conversion))
        val r = before_sort.reverse  ++ after.reverse
        r
  }
    val htsTube = (arr         : Seq[Map[String,Long]],
                   contact_pos : String,
                   contact_neg : String) => {
      //Create user paths
      val htsSeq = arr.map{elem => elem match {
        case conversion if(elem.keys.head.endsWith(contact_pos)) => elem.values.head.toString + contact_pos
        case touch if(elem.keys.head.endsWith(contact_neg))      => elem.values.head.toString + contact_neg
      }}
      htsSeq
    }

  val channelTube = (arr:Seq[Map[String,Long]]) => {
    arr.map(_.keys.head)
  }



  val touch_creator1 = (channel:String, hts:Long ,conversion:String) => {
    val l = List(channel,hts.toString,conversion)
    l
  }


  val pathCreator = (arr         : Seq[String],
                  mode        : String,
                  contact_pos : String,
                  contact_neg : String,
                  transit     : String) => {
    //Create user paths
    val touchpoints = arr.mkString(transit)
    val paths = touchpoints.split(contact_pos).map(_.trim)
    val target_paths = mode match {
      case "success" => paths.filterNot(_.endsWith(contact_neg)).map(_.stripPrefix(transit))
      case "fail"    => paths.filter(_.endsWith(contact_neg)).map(_.stripPrefix(transit))
    }
    //    val success_paths = paths.filterNot(_.endsWith(contact_neg)).map(_.stripPrefix(transit))
    val chains = target_paths.map(_.replace(contact_neg,""))
    chains
  }

//  case class Touch(channel: String, hts: Long , conversion : String) {
//    val getChannel = channel
//    val getHTS     = hts
//    val getConversion = conversion
//  }
//
//  def recursion_chain[T](inputList: List[T])(p: T => Boolean): List[List[T]] = inputList match {
//    case List() => List()
//    case lvalid @(x :: xs) =>
//      val (prev,next ) = lvalid.span(!p(_))
//      (prev, next) match {
//        case  alpha @ (List(List(_,_,"no_conv"),_*),List(List(_,_,"conv"),_*)) => alpha._1 ++ alpha._2.take(1) :: recursion_chain(alpha._2.drop(1))(p)
//        case beta @ (Nil,List(List(_,_,"conv"),_*))                             => List() ++ beta._2.take(1) :: recursion_chain(beta._2.drop(1))(p)
//        case gamma @ (List(List(_,_,"no_conv"),_*),Nil)                         => recursion_chain(List())(p)
//        case _                                                                   => recursion_chain(List())(p)
//      }
//  }

}