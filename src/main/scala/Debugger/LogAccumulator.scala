package FD

import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat

class LogAccumulator(val id:Int) extends Serializable{
    var logs = ArrayBuffer[(Long,Int,String)]()
    def log(logstr:String):Unit={
        logs.append((System.currentTimeMillis,id,logstr))
    }
    def merge(another:LogAccumulator):Unit={
        logs.appendAll(another.logs)
        logs = logs.sortWith(_._1 < _._1)
    }
    def printlogs():Unit={
        val formatter = new SimpleDateFormat("(HH:mm:ss:SSS)")
        for(x <- logs){
            print("LogAccumulator: ")
            print(formatter.format(x._1))
            print("[")
            print(x._2)
            print("]")
            print(" ")
            println(x._3)
        }
    }
}