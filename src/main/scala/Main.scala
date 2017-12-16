package FD

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD._
import scala.util.control.Breaks._
import scala.collection.mutable.ListBuffer

object Main{
    def main(args: Array[String]):Unit ={
        val INPUT_PARTS = 999;
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
        println("Generating possible attributes..")
        val totattribs = countAttribs(readedlines.first)
        val possibcombs = Combinator.genCombinations(totattribs)
        println("Generating search space tree..")
        val space = new SearchSpaceTree(totattribs)
        for(pubattribs <- possibcombs){
            print("Validating public attribute ")
            println(pubattribs.toString)
            val broadSpace = sc.broadcast(space)
            val lines = splitedlines.map(arr => (hashWithPublicAttribs(arr,pubattribs),arr)).partitionBy(new MyHashPartitioner(INPUT_PARTS))
            val mapped = lines.mapPartitions(x => List[ReversedSearchSpaceTree](Validator.validatePartition(x.toList.map(x => x._2),broadSpace)).iterator)
            val result = mapped.reduce((x,y) => {x.merge(y);x})
            space.merge(result)
            broadSpace.unpersist()
        }
        println("Generating output..")
        val outputstrs = IOController.FDstoString(space.toFDs)
        println("All output is:")
        for(x <- outputstrs) println(x)
    }

    def countAttribs(input:String):Int = {
        return input.split(",").length+1
    }

    def hashWithPublicAttribs(arr:Array[String],pubattrid:List[Int]):Int={
        val v = for(x <- pubattrid) yield arr(x)
        v.hashCode
    }

}