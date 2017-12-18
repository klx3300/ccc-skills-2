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
        val reinforcedlines = readedlines.map(str => str.replaceAll(",,",",ZHWK_INVAL_ATTRIB,"))
        val splitedlines = readedlines.map(str => str.split(","))
        println("Generating possible attributes..")
        val totattribs = countAttribs(readedlines.first)
        val possibcombs = Combinator.genCombinations(totattribs)
        println("Generating search space tree..")
        val space = new SearchSpaceTree(totattribs)
        for(pubattribs <- possibcombs){
            print("Validating public attribute ")
            print(pubattribs.toString)
            println("..")
            println(" - Broadcasting search space tree..")
            val broadSpace = sc.broadcast(space)
            println(" - Starting preparation of repartitioning..")
            val linespre = splitedlines.map(arr => (hashWithPublicAttribs(arr,pubattribs),arr))
            println(" - Starting repartitioning..")
            val lines = linespre.partitionBy(new MyHashPartitioner(INPUT_PARTS))
            println(" - Starting Validator at each partition..")
            val mapped = lines.mapPartitions(x => List[ReversedSearchSpaceTree](Validator.validatePartition(x.toList.map(x => x._2),broadSpace)).iterator)
            println(" - Starting to merge reversed tree from each partition..")
            val result = mapped.reduce((x,y) => {x.merge(y);x})
            println(" - Starting to update the seach space tree..")
            space.merge(result)
            println(" - Unpersisting previous search space tree..")
            broadSpace.unpersist()
        }
        println("Generating output..")
        val outputstrs = IOController.FDstoString(space.toFDs)
        println("All output is:")
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
    }

}