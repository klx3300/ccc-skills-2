package FD


object IOController {

  import scala.collection.mutable.ListBuffer

  def FDsShrink(fds: List[(List[Int], Int)]): scala.collection.mutable.Map[List[Int], List[Int]] = {
    val answer = scala.collection.mutable.Map[List[Int], List[Int]]()
    var lastlhs = List[Int]()
    var each = ListBuffer[Int]()
    for ((lhs, rhs) <- fds) {
      if (lhs != lastlhs) {
        each = ListBuffer[Int]()
      }
      lastlhs = lhs
      val attribs = Combinator.genCombinations(lhs)
      var isexist = false
      for (attrib <- attribs) {
        val exist = answer.get(attrib)
        exist match {
          case Some(lastrhs) => {
            if (lastrhs.contains(rhs)) {
              isexist = true
            }
          }
          case None => None
        }
      }
      if (!isexist) {
        each += rhs
      }
      if (each.size > 0) {
        answer(lhs) = each.sorted.toList
      }
    }
    answer
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