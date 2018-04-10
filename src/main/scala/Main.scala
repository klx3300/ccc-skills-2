package FD

import org.apache.spark.rdd.RDD._
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    val INPUT_PARTS = 576
    val logfile = args(1)
    val conf = new SparkConf().setAppName("Functional Dependency")
      .set("spark.driver.maxResultSize", "0")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "2047m")
    conf.registerKryoClasses(Array(classOf[SearchSpaceTree]))
    val sc = new SparkContext(conf)
    val input_folder = args(0)
    val output_folder = args(1)
    val temp_folder = args(2)
    val input_file = input_folder + "/bots_20m_15.csv"
    val output_file = output_folder
    // map input lines to its effective attribs
    val readedlines = sc.textFile(input_file, INPUT_PARTS)
    val splitedlines = readedlines.map(str => str.split(",")).cache()
    val totattribs = splitedlines.first.length
    val possibcombs = Combinator.genCombinations(totattribs).reverse
    val space = new SearchSpaceTree(totattribs)
    for (pubattribs <- possibcombs) {
      val possibrhs = space.vertices(pubattribs)
      val result = possibrhs.toArray.map(x => x._2).reduce((x,y)=> (x || y))
      if(result){
      val broadSpace = sc.broadcast(space)
      val broadcombs = sc.broadcast(possibcombs)
      val linespre = splitedlines.map(arr => (hashWithPublicAttribs(arr, pubattribs), arr))
      val lines = linespre.partitionBy(new MyHashPartitioner(INPUT_PARTS))
      val mapped = lines.mapPartitionsWithIndex(
        (partindex, x) =>
          List[ReversedSearchSpaceTree]
            (Validator.validatePartition(partindex, x.toList.map(x => x._2), broadSpace, pubattribs)).iterator)
      val result = mapped.reduce((x, y) => {
        x.merge(y)
        x
      })
      broadcombs.unpersist()
      space.merge(result)
      broadSpace.unpersist()
      }
    }
    val outputstrs = IOController.FDstoString(IOController.FDsShrink(space.toFDs))
    sc.parallelize(outputstrs).saveAsTextFile(output_file)
    //for(x <- outputstrs) println(x)
  }

  def countAttribs(input: String): Int = {
    input.split(",").length
  }

  def hashWithPublicAttribs(arr: Array[String], pubattrid: List[Int]): Int = {
    pubattrid.map(x => arr(x)).toString.hashCode
  }

}
