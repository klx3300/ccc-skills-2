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
            if(counter.contains(buffer.toList))
                counter(buffer.toList) += 1
            else
                counter(buffer.toList) = 1
        }
        if(loga != null && counter.size != 0){
            val strbuffer = new StringBuffer()
            strbuffer append "{"
            for((record,times) <- counter){
                strbuffer append "["
                strbuffer append times
                strbuffer append "x]"
                strbuffer append record.toString
                strbuffer append ", "
            }
            strbuffer append "}"
            loga.log(attribs.toString + ": " + strbuffer.toString)
        }
        counter.size
    }
}