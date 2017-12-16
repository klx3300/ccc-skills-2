package FD

import scala.collection.mutable.ListBuffer

object Equivalencer{
    def getEquivalenceCounts(attribs:List[Int],dataset:List[Array[String]]):Int={
        val counter = scala.collection.mutable.Map[List[String],Int]()
        var buffer = ListBuffer[String]()
        for(x <- dataset){
            buffer = ListBuffer[String]()
            for(y <- attribs){
                buffer += x(y)
            }
            counter(buffer.toList) = counter(buffer.toList) + 1
        }
        counter.size
    }
}