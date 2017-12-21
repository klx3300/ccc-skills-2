package FD

import scala.collection.mutable.ListBuffer

object Equivalencer{
    def getEquivalenceCounts(attribs:List[Int],dataset:List[Array[String]],loga:LogAccumulator):Int={
        val counter = scala.collection.mutable.Map[String,Int]()
        for(x <- dataset){
            val buffer = ListBuffer[String]()
            for(y <- attribs){
                buffer += x(y)
            }
            counter(buffer.toList.toString) = 1
        }
        counter.size
    }
}