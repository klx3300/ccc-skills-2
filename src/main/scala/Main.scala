package FD

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD._
import scala.util.control.Breaks._
import scala.collection.mutable.ListBuffer
import org.apache.spark.HashPartitioner

object Main{
    def main(args: Array[String]):Unit ={
        val INPUT_PARTS = 20;
        val logfile = args(1)
        val conf = new SparkConf().setAppName("Functional Dependency")
        val sc = new SparkContext(conf)
        val input_folder = args(0)
        val output_folder = args(1)
        val temp_folder = args(2)
        val input_file = input_folder + "/bots_20m_15.csv"
        val output_file = output_folder
        // map input lines to its effective attribs
        val readedlines = sc.textFile(input_file,INPUT_PARTS)
        val splitedlines = readedlines.map(str => str.split(","))
        val totattribs = splitedlines.first.size
        val possibcombs = Combinator.genCombinations(totattribs).reverse
        //val possibcombs = scala.collection.mutable.Map[List[Int],Boolean]()
        // init possibcombs
        /*for(x <- possibcombs_pre){
            possibcombs(x) = true
        }*/
        val space = new SearchSpaceTree(totattribs)
        val logaccu = new LogAccumulator(-1)
        //Tracker.track(List[Int](2,7))
        for(pubattribs <- possibcombs){
            val broadSpace = sc.broadcast(space)
            val broadcombs = sc.broadcast(possibcombs)
            /*val linespre_p = splitedlines.map(arr => {
                val loga = new LogAccumulator(-1)
                (hashWithPublicAttribs(arr,pubattribs,loga),arr,loga)})
            val partitionlogs_pre = linespre_p.map(x => x._3)
            val partitionlogs = partitionlogs_pre.reduce((x,y) => {x.merge(y);x})
            logaccu.merge(partitionlogs)
            val linespre = linespre_p.map(x => (x._1,x._2))*/
            val linespre = splitedlines.map(arr => (hashWithPublicAttribs(arr,pubattribs),arr))
            val lines = linespre.partitionBy(new MyHashPartitioner(INPUT_PARTS))
            val mapped = lines.mapPartitionsWithIndex((partindex,x) => List[(ReversedSearchSpaceTree,LogAccumulator)]
                (Validator.validatePartition(partindex,
                x.toList.map(x => x._2),broadSpace,pubattribs)).iterator)
            val result = mapped.reduce((x,y) => {x._1.merge(y._1);x._2.merge(y._2);x})
            broadcombs.unpersist()
            space.merge(result._1/*,possibcombs*/)
            logaccu.merge(result._2)
            broadSpace.unpersist()
        }
        val outputstrs = IOController.FDstoString(IOController.FDsShrink(space.toFDs))
        logaccu.printlogs
        //sc.parallelize(outputstrs,1).saveAsTextFile(output_file)
        for(x <- outputstrs) println(x)
    }

    def countAttribs(input:String):Int = {
        return input.split(",").length
    }

    def hashWithPublicAttribs(arr:Array[String],pubattrid:List[Int]/*,loga:LogAccumulator*/):Int={
        val k = ListBuffer[String]()
        for(x<-pubattrid){
            k+=arr(x)
        }
        /*if(pubattrid.contains(2) && pubattrid.contains(6) && pubattrid.size <= 3){
            loga.log("Hashcode of " + k.toList.toString + " is " + k.toList.hashCode.toString )
        }*/
        k.toList.toString.hashCode
    }

}
