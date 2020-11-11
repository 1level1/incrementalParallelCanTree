import levko.cantree.utils.{CanTree, CanTreeFPGrowth, CanTreePartitioner, CanTreeV1, NodeManager, NodeManagerTrait}
import org.apache.spark.{Partitioner, SparkException}
import org.apache.spark.sql.{Row, SparkSession}
import java.{util => ju}

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions.{broadcast, col, collect_list, flatten, max, min, row_number}

import scala.collection.immutable
import scala.reflect.ClassTag
import levko.cantree.utils.CanTreeFPGrowth.FreqItemset
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.util.SizeEstimator

import scala.sys.exit

object CanTreeMain {

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

  def addTransitionToTree[T](items : ListBuffer[T], tree : CanTree[T], sorter : Sorter[T]): Unit = {
    val sorted = items.sortWith(sorter);
    tree.add(sorted);
  }

  def addToMap[T](m: mutable.Map[T, ListBuffer[T]], key: T, value: ListBuffer[T])  : Unit =  {
    val currL = m.getOrElse(key,null);
    if (currL==null)
      m += key -> value;
  }

  def seqOp[T] = (accum: CanTree[T] , element: mutable.WrappedArray[T]) =>
    accum.add(element.asInstanceOf[mutable.WrappedArray[T]],1)

  def combOp[T] = (accum1: CanTree[T],accum2: CanTree[T]) => {
    (accum1.merge(accum2))
  }

  def seqFIS[T](map1:mutable.HashMap[T,Long], element:CanTree[T]) : mutable.HashMap[T,Long] = {
    for ((k,v) <- element.getNodeRankMap() ) {
      val mapV = map1.getOrElse(k,0L)
      map1.update(k,mapV+v)
    }
    map1
  }

  def combFISOp[T](map1:mutable.HashMap[T,Long],map2:mutable.HashMap[T,Long]) : mutable.HashMap[T,Long] = {
    for ((k,v) <- map2 ) {
      val mapV = map1.getOrElse(k,0L)
      map1.update(k,mapV+v)
    }
    map1
  }

  def flattenTransactions[T](trans: (ListBuffer[T], Long)) : ListBuffer[ListBuffer[T]] = {
    val res: ListBuffer[ListBuffer[T]] = ListBuffer.empty
    for (i <- 0L until trans._2 ) {
      res.append(trans._1)
    }
    res
  }
  def seqTreeTransactionMine[T] = (accum:ListBuffer[(ListBuffer[T],Long)],element:(CanTree[T],immutable.Map[T,Long])) =>
  (accum++element._1.calcTransactions(element._2))
  def combineTreeTransactions[T] = (accum1:ListBuffer[(ListBuffer[T],Long)],accum2:ListBuffer[(ListBuffer[T],Long)]) =>
    (accum1++accum2)
  def seqTreeTransactionMineExploded[T] (accum:ListBuffer[ListBuffer[T]],element:(CanTree[T],immutable.Map[T,Long])) : ListBuffer[ListBuffer[T]] = {
    val res : ListBuffer[ListBuffer[T]] = ListBuffer.empty
    val treeItems = element._1.calcTransactions(element._2)
    for (l<-treeItems) {
      for (i <- 0 until l._2.toInt)
        res.append(l._1)
    }
    accum++res
  }
  def combineTreeTransactionsExploded[T] = (accum1:ListBuffer[ListBuffer[T]],accum2:ListBuffer[ListBuffer[T]]) =>
    (accum1++accum2)



  val usage = """
    Usage: CanTreeMain [--num-partitions int] [--min-support double]
    [--in-file-path str] [--base-db-percentage double] [--inc-db-percentage double]
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
        case "--in-file-path" :: value :: tail =>
          nextOption(map ++ Map('infilepath -> value), tail)
//        case string :: Nil =>  nextOption(map ++ Map('infile -> string), list.tail)
        case "--base-db-percentage" :: value :: tail =>
          nextOption(map ++ Map('baseper -> value.toDouble), tail)
        case "--inc-db-percentage" :: value :: tail =>
          nextOption(map ++ Map('incper -> value.toDouble), tail)
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
    val fileName = options.get('infilepath).get
    val numPartitions = options.getOrElse('numpartitions,1).asInstanceOf[Int]
    val minSupport = options.getOrElse('minsupport,0.1).asInstanceOf[Double]
    val spark = SparkSession.builder.appName("CAN_TREE").getOrCreate()
//    spark.sparkContext.setLogLevel("DEBUG")
    val sc = spark.sparkContext
//    sc.setCheckpointDir("hdfs://localhost:9000/RddCheckPoint")
    val customSchema = StructType(Array(
      StructField("InvoiceNo", IntegerType, true),
      StructField("StockCode", IntegerType, true))
    )

    val df = spark.read.format("csv").option("header", "true").schema(customSchema).load(fileName.asInstanceOf[String])
    val df2 = df.groupBy("InvoiceNo").agg(collect_list(col("StockCode")))

//    val m : mutable.Map[String, ListBuffer[String]] = mutable.Map.empty;
    val partitioner : CanTreePartitioner = new CanTreePartitioner(numPartitions)
    println("-0-")
//    val freqMap = getItemsCount(df2.rdd.map(r=>r(1).asInstanceOf[mutable.WrappedArray[Int]].toArray),partitioner)
//    println("-1- "+freqMap.size)
//    val customSorterInst = customSorter(freqMap)
//    val df3 = df2.rdd.map(x => genCondTransactions(x(1).asInstanceOf[mutable.WrappedArray[Int]],intSorter,partitioner))
    val transactions = df2.rdd.map(t=>t(1).asInstanceOf[mutable.WrappedArray[Int]].toArray)
//    val fpModel = new FPGrowth()
//      .setMinSupport(minSupport)
//      .setNumPartitions(partitioner.numPartitions)
//      .run(transactions)
//    println(s"Number of frequent itemsets: ${fpModel.freqItemsets.count()}")
//    fpModel.freqItemsets.collect().foreach { itemset =>
//      println(s"${itemset.items.mkString("[", ",", "]")}, ${itemset.freq}")
//    }
//    transactions.checkpoint()
    val model = new CanTreeFPGrowth()
      .setMinSupport(minSupport)
      .setNumPartitions(partitioner.numPartitions)
      .run(transactions,intSorter)
//    model.checkpoint()
//    println(s"-1- Number of frequent itemsets: ${model.count()}")
    model.collect.foreach { itemset =>
      println(s"${itemset.items.mkString("[", ",", "]")}, ${itemset.freq}")
    }
//    val test2 = df3.take(10)
//    for (t<-test2)
//      println(t)
//    val df4 = df3.aggregateByKey(zeroVal,partitioner.numPartitions)(seqOp[String],combOp[String])
//
////    val globalFIS : mutable.HashMap[Iterable[String],Long] = mutable.HashMap.empty
//    val minCount : Long = (df2.count()*0.1).toLong
////    df4.foreach(t => {
////      val part = t._1
////      val nm = t._2.getNodeRankMap()
////      println(part, nm)
////    })
//    val zeroFISMap : mutable.HashMap[String,Long] = mutable.HashMap.empty[String,Long]
//    val test0 = df4.take(10)
//    for (elem<-test0) {
//      println(elem)
//    }
//    val globalFIS = df4.map(t=>t._2)
//    val test=globalFIS.take(10)
//    for (elem <- test) {
//      println(elem)
//    }
//      .aggregate(zeroFISMap)(seqFIS[String],combFISOp[String]).filter((t)=>t._2>=minCount)
//    val zeroTransactionVal : ListBuffer[(ListBuffer[String],Long)] = ListBuffer.empty
//    val allFilteredTransactions = df4.map(t=> (t._1,(t._2,globalFIS.toMap))).aggregateByKey(zeroTransactionVal)(seqTreeTransactionMine[String],combineTreeTransactions[String])
//    val zeroTransactionValExploded : ListBuffer[ListBuffer[String]] = ListBuffer.empty
//    val allFilteredTransactionsExploded = df4.map(t=> (t._1,(t._2,globalFIS.toMap))).aggregateByKey(zeroTransactionValExploded)(seqTreeTransactionMineExploded[String],combineTreeTransactionsExploded[String])
//    df4.foreach( i => {
//      println(i._1,i._2)
//    })
//    allFilteredTransactions.foreach(v => {
//      val rank = v._1
//      val list = v._2
//      for ((l,count) <- list) {
//        println(rank.toString, l,count)
//      }
//    })
//    val model = new FPGrowth()
//      .setMinSupport(0.4)
//      .setNumPartitions(3)
//      .run(allFilteredTransactionsExploded.flatMap(t=>t._2).map(t=>t.toArray))
//
//    model.freqItemsets.collect().foreach { itemset =>
//      println(s"${itemset.items.mkString("[", ",", "]")}, ${itemset.freq}")
//    }

    //    allFilteredTransactions.flatMap(t => flattenTransactions(t))
  }
}
