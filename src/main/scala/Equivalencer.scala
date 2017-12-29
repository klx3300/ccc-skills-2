package FD

import scala.collection.mutable.ListBuffer

object Equivalencer{
    def getEquivalenceCounts(attribs:List[Int],dataset:List[Array[String]],loga:LogAccumulator):Int={
        val counter = scala.collection.mutable.Map[List[String],Int]()
        for(x <- dataset){
            val buffer = ListBuffer[String]()
            for(y <- attribs){
                buffer += x(y)
            }
            counter(buffer.toList) = 1
        }
        if(loga != null && counter.size != 0){
            val strbuffer = new StringBuffer()
            strbuffer append "{"
            for((record,useless) <- counter){
                strbuffer append record.toString
                strbuffer append ", "
            }
            strbuffer append "}"
            loga.log(attribs.toString + ": " + strbuffer.toString)
        }
        counter.size
    }
}