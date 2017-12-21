package FD

import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat

class LogAccumulator extends Serializable{
    var logs = ArrayBuffer[(Long,String)]()
    def log(logstr:String):Unit={
        logs.append((System.currentTimeMillis,logstr))
    }
    def merge(another:LogAccumulator):Unit={
        logs.appendAll(another.logs)
        logs = logs.sortWith(_._1 < _._1)
    }
    def printlogs():Unit={
        val formatter = new SimpleDateFormat("(yyyy/MM/dd HH:mm:ss:SSS)")
        for(x <- logs){
            print("LogAccumulator: ")
            print(formatter.format(x._1))
            print(" ")
            println(x._2)
        }
    }
}