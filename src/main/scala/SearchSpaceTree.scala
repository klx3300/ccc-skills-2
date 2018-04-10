package FD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class SearchSpaceTree(val attribcnt: Int) extends Serializable {
  val vertices: mutable.Map[List[Int], mutable.Map[List[Int], Boolean]] = mutable.Map[List[Int], mutable.Map[List[Int], Boolean]]()

  def init(): Unit = {
    val tmpcombs = Combinator.genCombinations(attribcnt)
    for (x <- tmpcombs) {
      vertices(x) = scala.collection.mutable.Map[List[Int], Boolean]()
      var xcurr = 0
      var attribcurr = 0
      while (xcurr < x.length) {
        while (attribcurr < x(xcurr)) {
          val destbuffer = ListBuffer[Int]()
          for (y <- x) destbuffer += y
          destbuffer += attribcurr
          vertices(x)(destbuffer.toList) = true
          attribcurr = attribcurr + 1
        }
        attribcurr = x(xcurr) + 1
        xcurr = xcurr + 1
      }
      // can i go further than it?
      while (attribcurr < attribcnt) {
        val destbuffer = ListBuffer[Int]()
        for (y <- x) destbuffer += y
        destbuffer += attribcurr
        vertices(x)(destbuffer.toList) = true
        attribcurr = attribcurr + 1
      }
    }
  }

  init()

  def merge(boomedlogs: mutable.HashSet[List[Int]]): Unit = {
    val allboomedlogs = boomedlogs.flatMap(x => {
      val tmpleft = x.dropRight(1)
      val tmpright = x.last
      Combinator.genFullCombinations(tmpleft).map(y => (y, y :+ tmpright)).iterator
    })
    for (x <- allboomedlogs) {
      vertices(x._1)(x._2) = false
    }
  }

  def toFDs: List[(List[Int], Int)] = { // not shrinked yet
    val fds = ListBuffer[(List[Int], Int)]()
    val attribs = Combinator.genCombinations(attribcnt)
    for (lhs <- attribs) {
      val rhsmap = vertices(lhs)
      for ((rhs, iscorr) <- rhsmap) {
        if (iscorr) {
          fds += ((lhs, rhs.last))
        }
      }
    }
    fds.toList
  }
}