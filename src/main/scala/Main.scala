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
        val totattribs = countAttribs(readedlines.first)
        val possibcombs = genCombinations(totattribs)
        for(pubattribs <- possibcombs){
            val lines = splitedlines.map(arr => (hashWithPublicAttribs(arr,pubattribs),arr)).partitionBy(new MyHashPartitioner(INPUT_PARTS))
            
        }
    }

    def countAttribs(input:String):Int = {
        return input.split(",").length+1
    }

    def genCombinations(attribcnt:Int):List[List[Int]]={
        val attribidarr = 0 until attribcnt;
        var topbuffer =new ListBuffer[List[Int]]()
        for(currcnt <- 1 until attribcnt){
            var bottombuffer = new ListBuffer[Int]()
            for(i <- 0 until currcnt){
                bottombuffer += i
            }
            topbuffer += bottombuffer.toList
            while(bottombuffer(0)<attribcnt-currcnt){ // EX condition: the largest r-comb in dict order
                breakable{
                    for(operpos <- (currcnt-1).to(0,-1)){
                        // find last element that satisfy:NOT REACH ITS MAXIMUM
                        if(bottombuffer(operpos)<attribcnt-currcnt+operpos){
                            bottombuffer(operpos) = bottombuffer(operpos)+1
                            for(updatepos <- operpos+1 until currcnt){
                                bottombuffer(updatepos) = bottombuffer(operpos)+updatepos-operpos
                            }
                            topbuffer+=bottombuffer.toList
                            break
                        }
                    }
                }
            }
        }
        topbuffer.toList
    }

    def hashWithPublicAttribs(arr:Array[String],pubattrid:List[Int]):Int={
        val v = for(x <- pubattrid) yield arr(x)
        v.hashCode
    }

}