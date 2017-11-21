package FD

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Main{
    def main(args: Array[String]):Unit ={
        val logfile = args(1)
        val conf = new SparkConf().setAppName("Functional Dependency")
        val sc = new SparkContext(conf)
        val logdata = sc.textFile(logfile).cache
        val numAs = logdata.filter(line => line.contains("a")).count()
        val numBs = logdata.filter(line => line.contains("b")).count()
        println(s"$numAs $numBs");
        sc.stop()
    }
}