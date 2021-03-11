package levko.cantree.utils

import org.apache.log4j.Logger
import org.apache.spark.{Partitioner, SparkException}
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, max}
import org.apache.spark.sql.types.StructType
import levko.cantree.utils.CanTreeFPGrowth.FreqItemset

import scala.collection.mutable
import scala.reflect.ClassTag
import util.control.Breaks._


package object cantreeutils {
  type Sorter[T] = (T, T) => Boolean
  type StringSorter = Sorter[String]
  type IntSorter = Sorter[Int]
  def stringSorter : StringSorter = (s1,s2) => s1<s2
  def intSorter : IntSorter = (i1,i2) => i1<i2

  @transient lazy val log = Logger.getLogger(getClass.getName)

  def customSorter(map: Map[Int, Long]): Sorter[Int] = { (i1, i2) =>
    (map.get(i1), map.get(i2)) match {
      case (Some(ic1), Some(ic2)) => ic1 > ic2
      case(Some(ic1),_)           => true
      case(_,Some(ic2))           => false
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
                                      usecache :Boolean,
                                      minMinSup : Double): Unit = {
    import java.time.LocalDateTime
    var totTransactions = 0L
    var baseCanTreeRDD : RDD[(Int,CanTreeV1[Item])] = spark.sparkContext.emptyRDD
    var iter = 0
    var freqItems : mutable.HashMap[Item,Long] = mutable.HashMap.empty
    for (f <- fileList) {
      val dfGrouped = prepareTransactions(f,spark,schema)
      val transactions = dfGrouped.rdd.map(t=>t(1).asInstanceOf[mutable.WrappedArray[Item]].toArray)
      transactions.cache()
      totTransactions += transactions.count()
      val minSuppLong = math.ceil(totTransactions*minSupPercentage).toLong
      val minMinSupLong = (minSuppLong*minMinSup).toLong
      //      log.info(LocalDateTime.now + "-iterateAndReport- Finished reading transactions from: "+f+" ; new support count is: "+minSuppLong)
      val currFreq = getItemsCount(transactions).filter(_._2 >= minMinSupLong).collect().toMap
      for ((k,v)<-currFreq) {
        if (freqItems.contains(k))
          freqItems(k)+=v
        else
          freqItems.put(k,v)
      }
      val canTrees = model.genCanTrees(transactions,sorter,freqItems.toMap)
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
      val fisCount =   model.run(nextCanTreeRDD,minSuppLong).count()
      val treeSize = nextCanTreeRDD.map(part => part._2.nodesNum)
      treeSize.cache()
      val maxTreeSize = treeSize.max()
      val minTreeSize = treeSize.min()
      val meanTreeSize = treeSize.mean()
      val partitionsNum = model.getPartition().numPartitions
      log.info(LocalDateTime.now + "-iterateAndReport- Found "+ fisCount+" at iteration number "+iter + " ("+minTreeSize+","+meanTreeSize+","+maxTreeSize+") Partitions: "+partitionsNum)
//      log.info(LocalDateTime.now + "-iterateAndReport- Found "+ fisCount+" at iteration number "+iter)
      baseCanTreeRDD = nextCanTreeRDD
      iter+=1
    }
  }


  def getAllTransactions[Item:ClassTag](fileList : List[String], lastFile : String,spark : SparkSession,schema: StructType) : RDD[Array[Item]] = {
    var transactions : RDD[Array[Item]] = spark.sparkContext.emptyRDD
    var res : RDD[Array[Item]] = transactions
    if (!fileList.contains(lastFile)) {
      res = transactions
    } else {
      breakable {
        for (f <- fileList) {
          val dfGrouped = prepareTransactions(f, spark, schema)
          val nextTransactions = dfGrouped.rdd.map(t => t(1).asInstanceOf[mutable.WrappedArray[Item]].toArray)
          transactions = transactions.union(nextTransactions)
          transactions.cache()
          if (f == lastFile) {
            res = transactions
            break
          }
        }
      }
    }
    res
  }

  def repartitionTransactions[Item:ClassTag](fileList : List[String],
                                             lastFile : String,
                                             spark : SparkSession,
                                             schema: StructType,
                                             model: CanTreeFPGrowth,
                                             fis:RDD[FreqItemset[Item]],
                                             freqItems : mutable.HashMap[Item,Long],
                                             sorterFunction : Sorter[Item]) :  RDD[(Int, CanTreeV1[Item])] = {
    val freqItemset = fis.collect().map(fi => fi.items.toSet)
    val minCoverGroups = greedySetCoverAlgo(freqItems.keySet.toList,freqItemset.toList)
    val newPartition : CanTreePartitioner[Item] = new CanTreePartitioner[Item](minCoverGroups.size,minCoverGroups)
    model.setPartitioner(newPartition)
    val transactions = getAllTransactions(fileList,lastFile,spark,schema)
    val canTrees = model.genCanTrees(transactions,sorterFunction,freqItems.toMap)
    canTrees
  }

  def iterateAndReportSetCover[Item:ClassTag](model: CanTreeFPGrowth,
                                      fileList : List[String],
                                      spark : SparkSession,
                                      schema: StructType,
                                      minSupPercentage : Double,
                                      sorter: Sorter[Item],
                                      usecache :Boolean,
                                      minMinSup : Double,
                                      repartitionIdx:Int = 5): Unit = {
    import java.time.LocalDateTime
    var totTransactions = 0L
    var baseCanTreeRDD : RDD[(Int,CanTreeV1[Item])] = spark.sparkContext.emptyRDD
    var iter = 0
    var freqItems : mutable.HashMap[Item,Long] = mutable.HashMap.empty
    for (f <- fileList) {
      val dfGrouped = prepareTransactions(f,spark,schema)
      val transactions = dfGrouped.rdd.map(t=>t(1).asInstanceOf[mutable.WrappedArray[Item]].toArray)
      transactions.cache()
      totTransactions += transactions.count()
      val minSuppLong = math.ceil(totTransactions*minSupPercentage).toLong
      val minMinSupLong = (minSuppLong*minMinSup).toLong
      //      log.info(LocalDateTime.now + "-iterateAndReport- Finished reading transactions from: "+f+" ; new support count is: "+minSuppLong)
      val currFreq = getItemsCount(transactions).filter(_._2 >= minMinSupLong).collect().toMap
      for ((k,v)<-currFreq) {
        if (freqItems.contains(k))
          freqItems(k)+=v
        else
          freqItems.put(k,v)
      }
      val canTrees = model.genCanTrees(transactions,sorter,freqItems.toMap)
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
      val fis =   model.run(nextCanTreeRDD,minSuppLong)
      fis.cache()
      val fisCount = fis.count()
      val treeSize = nextCanTreeRDD.map(part => part._2.nodesNum)
      treeSize.cache()
      val maxTreeSize = treeSize.max()
      val minTreeSize = treeSize.min()
      val meanTreeSize = treeSize.mean()
      val partitionsNum = model.getPartition().numPartitions
      log.info(LocalDateTime.now + "-iterateAndReport- Found "+ fisCount+" at iteration number "+iter + " ("+minTreeSize+","+meanTreeSize+","+maxTreeSize+") Partitions: "+partitionsNum)
      if (iter!=0 && iter%repartitionIdx == 0) {
        baseCanTreeRDD = repartitionTransactions(fileList,f,spark,schema,model,fis,freqItems,sorter)
        log.info(LocalDateTime.now + "-iterateSetCover- Finished repartition")
      } else {
        baseCanTreeRDD = nextCanTreeRDD
      }
      iter+=1
    }
  }

  def prepareTransactions[Item:ClassTag](filePath: String, spark: SparkSession, customSchema : StructType):  DataFrame = {
    val df = spark.read.format("csv").option("header", "false").schema(customSchema).load(filePath)
    df.groupBy("InvoiceNo").agg(collect_list(col("StockCode")))
  }

  def iterateAndReportSong[Item:ClassTag](model: CanTreeFPGrowth,
  fileList : List[String],
  spark : SparkSession,
  schema: StructType,
  minSupPercentage : Double,
  sorter: Sorter[Item],
  partitioner : Partitioner): Unit = {
    import java.time.LocalDateTime
    log.info(LocalDateTime.now + "-iterateAndReport- start ")
    val df = prepareTransactions(fileList(0),spark,schema)
    val baseTransactions = df.rdd.map(t => t(1).asInstanceOf[mutable.WrappedArray[Item]].toArray)
    var itemFreq = mutable.Map[Item,Long]() ++getItemsCount(baseTransactions).collect().toMap
    val baseCanTrees = model.genCanTrees(baseTransactions,sorter,itemFreq.toMap)
    var totTransactions = baseTransactions.count()
    val baseMinSuppLong = math.ceil(totTransactions*minSupPercentage).toLong
    val freqItemsList = itemFreq.filter(_._2 >= baseMinSuppLong).keys.toList
    /*.groupBy(partitioner.getPartition(_))*/
    var currIncMiningRDD = baseCanTrees.map { item => (item._1, new IncMiningPFP[Item](item._2)) }
    var currCalculatedIncTrees = currIncMiningRDD.map{item => (item._1,item._2.calcFreqItems(freqItemsList,baseMinSuppLong,x => partitioner.getPartition(x) == item._1,sorter))}
    currCalculatedIncTrees.cache()
    val currIncFreqItemsets = currCalculatedIncTrees.map{item => (item._1,item._2.getFreqItems())}
    var fisCount = currIncFreqItemsets.map(_._2.size).sum().toLong
    log.info(LocalDateTime.now + "-iterateAndReport- Found "+ fisCount +" at iteration number 0")
    currIncMiningRDD.cache()
    for (i <- fileList.indices) {
      if (i!=0) {
        val f = fileList(i)
        val dfGrouped = prepareTransactions(f, spark, schema)
        val transactions = dfGrouped.rdd.map(t => t(1).asInstanceOf[mutable.WrappedArray[Item]].toArray)
        transactions.cache()
        totTransactions += transactions.count()
        val nextMinSuppLong = math.ceil(totTransactions*minSupPercentage).toLong
        val incItemFreq = getItemsCount(transactions).collect().toMap
        incItemFreq.foreach(x => {
          if  (itemFreq.contains(x._1))
            itemFreq(x._1) += x._2
          else
            itemFreq += x
        })

//        val incGroups = incItemFreq.keys.toList.groupBy(partitioner.getPartition(_))
        val incCanTrees = model.genCanTrees(transactions,sorter,itemFreq.toMap)
        val nextIncTreeRDD = currCalculatedIncTrees.fullOuterJoin(incCanTrees).map{
          case (part,(Some(incData1),Some(nextCanTree))) => (part,incData1.merge(nextCanTree))
          case (part,(Some(incData1),_)) => (part,incData1)
          case (part,(_,Some(nextCanTree))) => (part,new IncMiningPFP[Item](nextCanTree))
        }
        val nextFreqList = itemFreq.filter(_._2 >= nextMinSuppLong).keys.toList
        currCalculatedIncTrees = nextIncTreeRDD.map{item => (item._1,item._2.calcFreqItems(nextFreqList,nextMinSuppLong,x => partitioner.getPartition(x) == item._1,sorter))}
        currCalculatedIncTrees.cache()
        val nextFreqItemSets = currCalculatedIncTrees.map{item => (item._1,item._2.getFreqItems())}
//        currIncMiningRDD = nextIncTreeRDD
//        currIncMiningRDD.cache()
        fisCount = nextFreqItemSets.map(_._2.size).sum().toLong
        val treeSize = currCalculatedIncTrees.map(part => part._2.canTree.nodesNum)
        treeSize.cache()
        val maxTreeSize = treeSize.max()
        val minTreeSize = treeSize.min()
        val meanTreeSize = treeSize.mean()
        val partitionsNum = model.getPartition().numPartitions
        log.info(LocalDateTime.now + "-iterateAndReport- Found "+ fisCount+" at iteration number "+i + " ("+minTreeSize+","+meanTreeSize+","+maxTreeSize+") Partitions: "+partitionsNum)
//        log.info(LocalDateTime.now + "-iterateAndReport- Found "+ fisCount +" at iteration number "+i)
      }
    }
  }

  def getItemsCount[Item: ClassTag](data: RDD[Array[Item]]): RDD[(Item, Long)] = {
    data.flatMap { t =>
      val uniq = t.toSet
      if (t.length != uniq.size) {
        throw new SparkException(s"Items in a transaction must be unique but got ${t.toSeq}.")
      }
      t
    }.map(v => (v, 1L))
      .reduceByKey(_ + _)
  }

  private def calcMaxSubSet[Item: ClassTag](items : Set[Item], subsets: List[Set[Item]]): Set[Item] = {
    var ret : Set[Item] = Set.empty
    var maxItemsCnt = 0
    subsets.foreach{subset =>
      var cnt = subset.intersect(items).size
//      subset.foreach{i =>
//        if (items.contains(i))
//          cnt+=1
//      }
      if (cnt>maxItemsCnt) {
        ret = subset
        maxItemsCnt = cnt
      }
    }
    ret
  }

  def greedySetCoverAlgo[Item: ClassTag](items : List[Item], subsets: List[Set[Item]]): List[Set[Item]] = {
    var retList : List[Set[Item]] = List.empty
    var coveredItems : List[Item] = List.empty
    var remainingSubsets = subsets
    while (coveredItems.size != items.size) {
      val maxSubSet = calcMaxSubSet((items diff coveredItems).toSet,subsets)
      if (maxSubSet.size==0)
        return retList
      retList =  maxSubSet :: retList
      remainingSubsets = remainingSubsets diff List(maxSubSet)
      coveredItems = (coveredItems ++ maxSubSet).distinct
    }
    retList
  }

}
