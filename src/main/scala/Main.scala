package FD

import org.apache.spark.rdd.RDD._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Map

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
    //    println("Unique Attr Nums:")
    val columnUniqueMap = 0.until(attributesNums).toList.map {
      x =>
        readInRDD
          .map(each => each(x))
          .distinct
          .zipWithIndex
          .map(x => (x._1, x._2.toInt))
          .collectAsMap
    }
    //    columnUniqueNums.foreach(
    //      x => {
    //        print(x)
    //        print(" ")
    //      }
    //    )
    //    println("\nUnique Attr Sorted:")
    val columnSorted = columnUniqueMap
      .map(_.size)
      .zipWithIndex
      .sortBy(x => -x._1)
      .map(x => x._2)
    //    columnSorted.foreach(
    //      x => {
    //        print(x)
    //        print(" ")
    //      }
    //    )
    //    println
    val broadColumn = sc.broadcast(columnSorted)
    val space = new SearchSpaceTree(attributesNums)
    val logger = new LogAccumulator(0)
    for (i <- columnSorted.indices) {
      val broadSpace = sc.broadcast(space)
      val hashMap = columnUniqueMap(i)
      val lines = readInRDD
        .map {
          arr => (hashMap(arr(i)), arr)
        }.partitionBy(new MyHashPartitioner(hashMap.size))
        .cache()

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

}
