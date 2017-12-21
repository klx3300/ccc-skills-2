package FD

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD._
import scala.util.control.Breaks._
import scala.collection.mutable.ListBuffer
import org.apache.spark.HashPartitioner

object Main{
    def main(args: Array[String]):Unit ={
        val INPUT_PARTS = 1;
        val logfile = args(1)
        val conf = new SparkConf().setAppName("Functional Dependency")
        val sc = new SparkContext(conf)
        val input_folder = args(0)
        val output_folder = args(1)
        val temp_folder = args(2)
        val input_file = input_folder + "/bots_200_10.csv"
        val output_file = output_folder + "/my_bots_200_10_result.txt"
        // map input lines to its effective attribs
        val readedlines = sc.textFile(input_file,INPUT_PARTS)
        val splitedlines = readedlines.map(str => str.split(","))
        val totattribs = countAttribs(readedlines.first)
        val possibcombs = Combinator.genCombinations(totattribs)
        val space = new SearchSpaceTree(totattribs)
        val logaccu = new LogAccumulator()
        //Tracker.track(List[Int](2,6))
        for(pubattribs <- possibcombs){
            val broadSpace = sc.broadcast(space)
            val linespre = splitedlines.map(arr => (hashWithPublicAttribs(arr,pubattribs),arr))
            val lines = linespre.partitionBy(new HashPartitioner(INPUT_PARTS))
            val mapped = lines.mapPartitions(x => List[(ReversedSearchSpaceTree,LogAccumulator)](Validator.validatePartition(x.toList.map(x => x._2),broadSpace)).iterator)
            val result = mapped.reduce((x,y) => {x._1.merge(y._1);x._2.merge(y._2);x})
            space.merge(result._1)
            logaccu.merge(result._2)
            broadSpace.unpersist()
        }
        val outputstrs = IOController.FDstoString(space.toFDs)
        logaccu.printlogs
        for(x <- outputstrs) println(x)
    }

    def countAttribs(input:String):Int = {
        return input.split(",").length
    }

    def hashWithPublicAttribs(arr:Array[String],pubattrid:List[Int]):Int={
        val k = ListBuffer[String]()
        for(x<-pubattrid){
            k+=arr(x)
        }
        k.toList.hashCode
        1
    }

}