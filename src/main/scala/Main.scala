package FD

import org.apache.spark.rdd.RDD._
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.Map
import java.text.SimpleDateFormat

object Main {
  def main(args: Array[String]): Unit = {
    val INPUT_PARTS = 16
    val conf = new SparkConf().setAppName("Functional Dependency")
      .set("spark.driver.maxResultSize", "0")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "2047m")
      .registerKryoClasses(Array(classOf[SearchSpaceTree]))
    val sc = new SparkContext(conf)
    val inputFolder = args(0)
    val outputFolder = args(1)
    val tempFolder = args(2)
    val inputFile = inputFolder + "/bots_20m_15.csv"
    val outputFile = outputFolder
    val readInRDD = sc.textFile(inputFile, INPUT_PARTS).map(_.split(",")).cache()
    val attributesNums = readInRDD.first.length
        println("Unique Attr Nums:")
    val columnUniqueNums = 0.until(attributesNums).toList.map {
      x => readInRDD.map(each => each(x)).distinct.count
    }
        columnUniqueNums.foreach(
          x => {
            print(x)
            print(" ")
          }
        )
        println("\nUnique Attr Sorted:")
    val columnSorted = columnUniqueNums.zipWithIndex.sortBy(x => -x._1).map(x => x._2)
        columnSorted.foreach(
          x => {
            print(x)
            print(" ")
          }
        )
        println
    val broadColumn = sc.broadcast(columnSorted)
    val space = new SearchSpaceTree(attributesNums)
    val logger = new LogAccumulator(0)
    for (i <- columnSorted.indices) {
      val hashed = Map[String,Int]()
      var curhashmax = 0
      val broadSpace = sc.broadcast(space)
      val linespre = readInRDD.map {
        arr => ({
          if(hashed.getOrElse(arr(columnSorted(i)),-1) == -1){
            hashed.put(arr(columnSorted(i)),curhashmax)
            curhashmax = curhashmax + 1
            curhashmax - 1
          }else{
            hashed.getOrElse(arr(columnSorted(i)),-1)
          }
        }, arr)
      }
      log("Max: " + hashed.toString)
      val lines = linespre.partitionBy(new MyHashPartitioner(curhashmax)).cache()

      val allLHSCombinations = // 2m45
        Combinator.genRealFullCombinations(columnSorted.drop(i + 1).sorted)
          .map { x =>
            (x :+ columnSorted(i))
              .sorted
          }
      /*val needlhsset = columnSorted.take(i+1).toSet // 2m51.019
      val allLHSCombinations = space.vertices.toArray
      .flatMap(x => {
        val tmpset = x._1.toSet
        if(x._2.toArray.map(x => x._2).reduce((x,y) => (x | y)) && needlhsset.subsetOf(tmpset)){
          List[List[Int]](tmpset.diff(needlhsset).+(columnSorted(i)).toList.sorted).iterator
        }else{
          List[List[Int]]().iterator
        }
      }).toList*/
      val broadLHS = sc.broadcast(allLHSCombinations)
      val mapped = lines.mapPartitionsWithIndex(
        (partindex, x) =>
          List[(ReversedSearchSpaceTree, LogAccumulator)]
            (Validator.validatePartition(partindex, x.toList.map(x => x._2), broadSpace, i, broadColumn, broadLHS)).iterator)
      val result = mapped.reduce((x, y) => {
        x._1.merge(y._1)
        x._2.merge(y._2)
        x
      })
      space.merge(result._1)
      logger.merge(result._2)
      broadSpace.unpersist()
      lines.unpersist()
      broadLHS.unpersist()
    }
    broadColumn.unpersist()
    val outputstrs = IOController.FDstoString(IOController.FDsShrink(space.toFDs))
    logger.printlogs()
    sc.parallelize(outputstrs, 1).saveAsTextFile(outputFile)
  }
  /*def hashWithPublicAttribs(arr: Array[String], pubattrid: Int): Int = {
    val realhashv = hashWithPublicAttribsx(arr,pubattrid)
    //log("hashed: " + realhashv.toString)
    realhashv
  }
  def hashWithPublicAttribsx(arr: Array[String], pubattrid: Int): Int = {
    
  }*/
  val gbformatter = new SimpleDateFormat("(HH:mm:ss:SSS)")
  def log(logstr: String):Unit={
    printf("Logger: %s %s\n",gbformatter.format(System.currentTimeMillis),logstr)
  }
  /*
  def hashWithPublicAttribs(arr: Array[String], pubattrid: Int): Int = {
    arr(pubattrid).hashCode
  }
  */
}
