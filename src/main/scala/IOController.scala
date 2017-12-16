package FD

import scala.collection.mutable.ListBuffer

object IOController{
    def FDstoString(fds:scala.collection.mutable.Map[List[Int],ListBuffer[Int]]):List[String]={
        val buffer = ListBuffer[String]()
        for((lhs,rhs) <- fds){
            var strbuf = new String()
            strbuf += lhs.toString
            strbuf += "=>"
            strbuf += rhs.toList.toString
            buffer += strbuf
        }
        buffer.toList
    }
}