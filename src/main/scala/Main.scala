package FD

import scala.collection.mutable.Map
import org.apache.spark.rdd.RDD._
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.control.Breaks._
import scala.collection.mutable.ListBuffer

object Main {
  def main(args: Array[String]): Unit = {
    val INPUT_PARTS = 16
    val conf = new SparkConf().setAppName("我们就跑一次迭代看看效果")
      .set("spark.driver.maxResultSize", "0")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "2047m")
      .set("spark.executor.extraJavaOptions", "-XX:ThreadStackSize=2048 -XX:+UseCompressedOops -XX:+UseParNewGC -XX:+CMSParallelRemarkEnabled -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=75")
      .registerKryoClasses(Array(classOf[SearchSpaceTree], classOf[ReversedSearchSpaceTree]))
    val sc = new SparkContext(conf)
    val inputFolder = args(0)
    val outputFolder = args(1)
    val tempFolder = args(2)
    val inputFile = inputFolder + "/bots_20m_15.csv"
    val outputFile = outputFolder
    val readInRDD = sc.textFile(inputFile, INPUT_PARTS).map(_.split(","))
    val attributesNums = readInRDD.first.length
    val columnUniqueMap = 0.until(attributesNums).toList.map {
      x => readInRDD .map(each => each(x)) .distinct .zipWithIndex .map(x => (x._1, x._2.toInt)) .collectAsMap
    }
    val broadMap = sc.broadcast(columnUniqueMap)
    val linespre = readInRDD.map {
      x => x.indices.map( index => broadMap.value(index).getOrElse(x(index), -1)).toArray
    }
    val columnSorted = columnUniqueMap .map(_.size) .zipWithIndex .sortBy(x => x._1) .map(x => x._2)
    val broadColumn = sc.broadcast(columnSorted)
    val space = new LazySearchSpaceTree(attributesNums)
    val logger = new LogAccumulator(0)
    var awaits = ListBuffer[List[Int]]()
    val VALIDATE_THRESHOLD = 128
    for(curpubattr <- 0 until attributesNums){
      // init works for this attr!
      val hashMap = columnUniqueMap(curpubattr)
      val lines = linespre.map(arr => (arr(curpubattr),arr))
      .partitionBy(new MyHashPartitioner(hashMap.size)).cache()
      awaits.append(List[Int](curpubattr))
      val curmaxid = attributesNums - curpubattr - 1
      for(currcnt <- 1 until curmaxid + 1){
        val bottombuffer = new ListBuffer[Int]()
        for(i <- 0 until currcnt){
          bottombuffer += i
        }
        val anoresult = bottombuffer.map(x => x+curpubattr+1)
        anoresult.prepend(curpubattr)
        awaits.append(anoresult.toList)
        while(bottombuffer(0)<curmaxid-currcnt){ // EX condition: the largest r-comb in dict order
          breakable{
            for(operpos <- (currcnt-1).to(0,-1)){
              // find last element that satisfy:NOT REACH ITS MAXIMUM
              if(bottombuffer(operpos)<curmaxid-currcnt+operpos){
                bottombuffer(operpos) = bottombuffer(operpos)+1
                for(updatepos <- operpos+1 until currcnt){
                  bottombuffer(updatepos) = bottombuffer(operpos)+updatepos-operpos
                }
                val trueresult = bottombuffer.map(x => x+curpubattr+1)
                trueresult.prepend(curpubattr)
                awaits.append(trueresult.toList)
                if(awaits.size >= VALIDATE_THRESHOLD){
                  // start validation
                  val tovalidlhs = awaits.toList
                  val broadSpace = sc.broadcast(space)
                  val broadLHS = sc.broadcast(tovalidlhs)
                  val mapped = lines.mapPartitionsWithIndex((partindex,x) => 
                  List[(Map[(List[Int],Int),Boolean],LogAccumulator)](
                    Validator.validatePartition(partindex,x.map(x=>x._2).toList,broadSpace,curpubattr,broadColumn,broadLHS)
                  ).iterator)
                  val result = mapped.treeReduce((x,y) => {
                    for((lrcomb,status) <- y._1){
                      if(status == false){
                        x._1(lrcomb) = false
                      }
                    }
                    x._2.merge(y._2)
                    x
                  })
                  for(eachdep <- result._1){
                    space.update(eachdep._1._1,eachdep._1._2,eachdep._2)
                  }
                  logger.merge(result._2)
                  broadSpace.unpersist
                  broadLHS.unpersist
                  awaits = ListBuffer[List[Int]]()
                }
                break
              }
            }
          }
        }
      }
      val tovalidlhs = awaits.toList
      val broadSpace = sc.broadcast(space)
      val broadLHS = sc.broadcast(tovalidlhs)
      val mapped = lines.mapPartitionsWithIndex((partindex,x) => 
      List[(Map[(List[Int],Int),Boolean],LogAccumulator)](
        Validator.validatePartition(partindex,x.map(x=>x._2).toList,broadSpace,curpubattr,broadColumn,broadLHS)
      ).iterator)
      val result = mapped.treeReduce((x,y) => {
        for((lrcomb,status) <- y._1){
          if(status == false){
            x._1(lrcomb) = false
          }
        }
        x._2.merge(y._2)
        x
      })
      for(eachdep <- result._1){
        space.update(eachdep._1._1,eachdep._1._2,eachdep._2)
      }
      logger.merge(result._2)
      broadSpace.unpersist
      broadLHS.unpersist
      awaits = ListBuffer[List[Int]]()
      lines.unpersist()
    }
    broadColumn.unpersist()
    for((x,y) <- space.toFDs){
      println("Log:" + x.toString + " => " + y.toString)
    }
    val outputstrs = IOController.FDstoString(IOController.FDsShrink(space.toFDs))
    logger.printlogs()
    sc.parallelize(outputstrs, 1).saveAsTextFile(outputFile)
  }
}
