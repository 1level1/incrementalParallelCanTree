import levko.cantree.utils.{CanTreeFPGrowth}
import levko.cantree.utils.cantreeutils._
import org.apache.spark.{HashPartitioner,SparkException}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.log4j.Logger
import org.apache.spark.mllib.fpm.FPGrowth

import scala.io.Source
import scala.sys.exit

object CanTreeMain {
  @transient lazy val log = Logger.getLogger(getClass.getName)

  val usage = """
    Usage: CanTreeMain [--num-partitions int] [--min-support double] [--in-file-list-path str] [--pfp 1] [--freq-sort 1] [--song 1]
  """
  def main(args: Array[String]): Unit = {

    if (args.length == 0) println(usage)
    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]

    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      def isSwitch(s : String) = (s(0) == '-')
      list match {
        case Nil => map
        case "--num-partitions" :: value :: tail =>
          nextOption(map ++ Map('numpartitions -> value.toInt), tail)
        case "--min-support" :: value :: tail =>
          nextOption(map ++ Map('minsupport -> value.toDouble), tail)
        case "--in-file-list-path" :: value :: tail =>
          nextOption(map ++ Map('infilelistpath -> value), tail)
        case "--pfp" :: value :: tail =>
          nextOption(map ++ Map('pfp -> value.toInt), tail)
        case "--song" :: value :: tail =>
          nextOption(map ++ Map('song -> value.toInt), tail)
        case "--local" :: value :: tail =>
          nextOption(map ++ Map('local -> value.toInt), tail)
        case "--freq-sort" :: value :: tail =>
          nextOption(map ++ Map('freqsort -> value.toInt), tail)
        case "--app-name" ::  value :: tail =>
          nextOption(map ++ Map('appname -> value), tail)
        case "--use-cache" ::  value :: tail =>
          nextOption(map ++ Map('usecache -> value.toBoolean), tail)
        case "--min-min-support" :: value :: tail =>
          nextOption(map ++ Map('minminSupport -> value.toDouble), tail)
        case "--set-cover" :: value :: tail =>
          nextOption(map ++ Map('setCover -> value.toInt), tail)
        case option :: tail => println("Unknown option "+option)
          exit(1)
      }
    }

    val options = nextOption(Map(),arglist)
    val fileName = options.get('infilelistpath).get
    val numPartitions = options.getOrElse('numpartitions,1).asInstanceOf[Int]
    val minSupport = options.getOrElse('minsupport,0.1).asInstanceOf[Double]
    val pfp = options.getOrElse('pfp,0).asInstanceOf[Int]
    val song = options.getOrElse('song,0).asInstanceOf[Int]
    val local = options.getOrElse('local,0).asInstanceOf[Int]
    val freqsort = options.getOrElse('freqsort,0).asInstanceOf[Int]
    val appName = options.getOrElse('appname,"CAN_TREE_DEFAULT_APP").asInstanceOf[String]
    val usecache = options.getOrElse('usecache,true).asInstanceOf[Boolean]
    val minminSupport = options.getOrElse('minminSupport,0.001).asInstanceOf[Double]
    val setCover = options.getOrElse('setCover,0).asInstanceOf[Int]

    val spark = if (local==1)
      SparkSession.builder.master("local").appName(appName).getOrCreate()
    else
      SparkSession.builder.appName(appName).getOrCreate()

    val customSchema = StructType(Array(
      StructField("InvoiceNo", IntegerType, true),
      StructField("StockCode", IntegerType, true))
    )

    val fileList = new ListBuffer[String]()
    val bufferedSource = Source.fromFile(fileName.asInstanceOf[String])
    for (line <- bufferedSource.getLines) {
      fileList.append(line)
    }
    bufferedSource.close()
    if (fileList.size==0) {
      println("--in-file-list-path is expected to have at least 1 file per line -> " + fileName.asInstanceOf[String])
      exit(2)
    }
    val partitioner : HashPartitioner = new HashPartitioner(numPartitions)
    if (pfp!=0) {
      val fpModel = new FPGrowth()
        .setMinSupport(minSupport)
        .setNumPartitions(partitioner.numPartitions)
      iterateAndReportFPGrowth(fpModel,fileList.toList,spark,customSchema)
    } else {
      val dfGrouped = prepareTransactions(fileList(0),spark,customSchema)
      val transactions = dfGrouped.rdd.map(t=>t(1).asInstanceOf[mutable.WrappedArray[Int]].toArray)
      val minMinSupport = math.floor(transactions.count()*minSupport*minminSupport).toLong
      val model = new CanTreeFPGrowth().setMinSupport(minSupport).setPartitioner(partitioner).setMinMinSupport(minMinSupport)
      if (freqsort==1) {
        val countMap = transactions.flatMap { t =>
          val uniq = t.toSet
          if (t.length != uniq.size) {
            throw new SparkException(s"Items in a transaction must be unique but got ${t.toSeq}.")
          }
          t
        }.map(v => (v, 1L))
          .reduceByKey(_ + _)
          .collect().toMap
        val customSort = customSorter(countMap)
        if (song !=0) {
          iterateAndReportSong[Int](model, fileList.toList, spark, customSchema, minSupport, customSort, partitioner)
        } else if (setCover>0) {
          iterateAndReportSetCover(model, fileList.toList, spark, customSchema, minSupport, customSort, usecache, minminSupport,numPartitions,setCover)
        } else {
          iterateAndReport[Int](model, fileList.toList, spark, customSchema, minSupport, customSort, usecache, minminSupport)
        }
      } else {
        iterateAndReport[Int](model, fileList.toList, spark, customSchema, minSupport, intSorter, usecache, minminSupport)
      }
    }
  }
}
