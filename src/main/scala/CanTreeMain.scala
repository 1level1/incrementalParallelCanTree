import levko.cantree.utils.{CanTreeFPGrowth, CanTreeV1}
import org.apache.spark.{HashPartitioner, Partitioner, SparkException}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions.{col, collect_list, min}

import scala.reflect.ClassTag
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.log4j.Logger
import org.apache.spark.mllib.fpm.FPGrowth

import scala.io.Source
import scala.sys.exit

object CanTreeMain {
  @transient lazy val log = Logger.getLogger(getClass.getName)
  type Sorter[T] = (T, T) => Boolean
  type StringSorter = Sorter[String]
  type IntSorter = Sorter[Int]
  def stringSorter : StringSorter = (s1,s2) => s1<s2
  def intSorter : IntSorter = (i1,i2) => i1<i2

  def customSorter(map: Map[Int, Long]): Sorter[Int] = { (i1, i2) =>
    (map.get(i1), map.get(i2)) match {
      case (Some(ic1), Some(ic2)) => ic1 < ic2
      case(Some(ic1),_)           => false
      case(_,Some(ic2))           => true
      case _                      => i1 < i2
    }
  }

  def iterateAndReportFPGrowth[Item:ClassTag](model : FPGrowth,
                                              fileList : List[String],
                                              spark : SparkSession,
                                              schema: StructType) : Unit = {
    import java.time.LocalDateTime
    var df : DataFrame = prepareTransactions(fileList(0), spark, schema)
    var transactions = df.rdd.map(t => t(1).asInstanceOf[mutable.WrappedArray[Item]].toArray)
    var fisCount = model.run(transactions).freqItemsets.count()
    log.info(LocalDateTime.now + "-iterateAndReportFPGrowth- Found " + fisCount + " at iteration number " + 0)
    for (i <- fileList.indices) {
      if (i!=0) {
        val f = fileList(i)
        df = df.union(prepareTransactions(f, spark, schema))
        transactions = df.rdd.map(t => t(1).asInstanceOf[mutable.WrappedArray[Item]].toArray)
        fisCount = model.run(transactions).freqItemsets.count()
        log.info(LocalDateTime.now + "-iterateAndReportFPGrowth- Found " + fisCount + " at iteration number " + i)
      }
    }

  }

  def iterateAndReport[Item:ClassTag](model: CanTreeFPGrowth,
                             fileList : List[String],
                             spark : SparkSession,
                             schema: StructType,
                             minSupPercentage : Double,
                             sorter: Sorter[Item],
                             usecache :Boolean): Unit = {
    import java.time.LocalDateTime
    var totTransactions = 0L
    var baseCanTreeRDD : RDD[(Int,CanTreeV1[Item])] = spark.sparkContext.emptyRDD
    var iter = 0
    for (f <- fileList) {
      val dfGrouped = prepareTransactions(f,spark,schema)
      val transactions = dfGrouped.rdd.map(t=>t(1).asInstanceOf[mutable.WrappedArray[Item]].toArray)
      transactions.cache()
      totTransactions += transactions.count()
      val minSuppLong = math.ceil(totTransactions*minSupPercentage).toLong
//      log.info(LocalDateTime.now + "-iterateAndReport- Finished reading transactions from: "+f+" ; new support count is: "+minSuppLong)
      val canTrees = model.genCanTrees(transactions,sorter)
      val nextCanTreeRDD = baseCanTreeRDD.fullOuterJoin(canTrees).map{
        case (part,(Some(tree1),Some(tree2))) => (part,tree1.merge(tree2))
        case (part,(Some(tree1),_)) => (part,tree1)
        case (part,(_,Some(tree2))) => (part,tree2)
      }
      if (usecache) {
        nextCanTreeRDD.persist()
        baseCanTreeRDD.unpersist()
      }
//      baseCanTreeRDD.map{case (group,tree) => (group,tree.nodesNum)}.foreach{case (group,treeNodesCount) => log.info(LocalDateTime.now + " -iterateAndReport- iteration:"+iter+" - group "+ group+" tree size "+treeNodesCount)}
      val fisCount =   model.run(nextCanTreeRDD,minSuppLong).map(fis => {
        fis.items.size
      }).count()
      log.info(LocalDateTime.now + "-iterateAndReport- Found "+ fisCount+" at iteration number "+iter)
      baseCanTreeRDD = nextCanTreeRDD
      iter+=1
    }
  }

  def prepareTransactions[Item](filePath: String, spark: SparkSession, customSchema : StructType):  DataFrame = {
    val df = spark.read.format("csv").option("header", "false").schema(customSchema).load(filePath)
    df.groupBy("InvoiceNo").agg(collect_list(col("StockCode")))
  }

  val usage = """
    Usage: CanTreeMain [--num-partitions int] [--min-support double] [--in-file-list-path str] [--pfp 1] [--freq-sort 1]
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
        case option :: tail => println("Unknown option "+option)
          exit(1)
      }
    }
    def getItemsCount[Item: ClassTag](
     data: RDD[Array[Item]],
     partitioner: Partitioner): Map[Item, Long] = {
      val itemsMap = data.flatMap { t =>
        val uniq = t.toSet
        if (t.length != uniq.size) {
          throw new SparkException(s"Items in a transaction must be unique but got ${t.toSeq}.")
        }
        t
      }.map(v => (v, 1L))
        .reduceByKey(partitioner, _ + _).collect().toMap
      itemsMap
    }

    val options = nextOption(Map(),arglist)
    val fileName = options.get('infilelistpath).get
    val numPartitions = options.getOrElse('numpartitions,1).asInstanceOf[Int]
    val minSupport = options.getOrElse('minsupport,0.1).asInstanceOf[Double]
    val pfp = options.getOrElse('pfp,0).asInstanceOf[Int]
    val local = options.getOrElse('local,0).asInstanceOf[Int]
    val freqsort = options.getOrElse('freqsort,0).asInstanceOf[Int]
    val appName = options.getOrElse('appname,"CAN_TREE_DEFAULT_APP").asInstanceOf[String]
    val usecache = options.getOrElse('usecache,true).asInstanceOf[Boolean]
    val minminSupport = options.getOrElse('minminSupport,0.001).asInstanceOf[Double]

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
      val model = new CanTreeFPGrowth().setMinSupport(minSupport).setPartitioner(partitioner)
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
        iterateAndReport[Int](model, fileList.toList, spark, customSchema, minSupport, customSort,usecache)
      } else {
        iterateAndReport[Int](model, fileList.toList, spark, customSchema, minSupport, intSorter,usecache)
      }
    }
  }
}
