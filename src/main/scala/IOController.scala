package FD


object IOController {

  import scala.collection.mutable.ListBuffer

  def FDsShrink(fds: List[(List[Int], Int)]): scala.collection.mutable.Map[List[Int], List[Int]] = {
    val answer = scala.collection.mutable.Map[List[Int], List[Int]]()
    for(eachfd <- fds){
      if(answer.contains(eachfd._1)){
        answer(eachfd._1) = answer(eachfd._1).:+(eachfd._2)
      }else{
        answer(eachfd._1) = List[Int](eachfd._2)
      }
    }
    answer.map(x => (x._1.sorted,x._2.sorted))
  }

  def FDstoString(fds: scala.collection.mutable.Map[List[Int], List[Int]]): List[String] = {
    val buffer = ListBuffer[String]()
    for ((lhs, rhs) <- fds) {
      val strbuf = new StringBuffer()
      strbuf.append("[")
      val corrlhs = lhs.map(x => x + 1)
      for (x <- corrlhs) {
        strbuf.append("column" + x)
        if (x != corrlhs.last) strbuf.append(",")
      }
      strbuf.append("]:")
      val corrrhs = rhs.map(x => x + 1)
      for (x <- corrrhs) {
        strbuf.append("column" + x)
        if (x != corrrhs.last) strbuf.append(",")
      }
      buffer += strbuf.toString
    }
    buffer.toList
  }
}