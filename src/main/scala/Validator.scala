package FD

import org.apache.spark.broadcast._

object Validator {
  def validatePartition(id: Int, dataset: List[Array[String]], spacetree: Broadcast[SearchSpaceTree], pubattribs: List[Int]): ReversedSearchSpaceTree = {
    val revtree = new ReversedSearchSpaceTree(spacetree.value.attribcnt)
    val possibrhs = spacetree.value.vertices(pubattribs)
    val lhs = pubattribs
    for ((rhs, canvalid) <- possibrhs) {
      val failed = revtree.vertices(lhs).getOrElse(rhs, false)
      if (canvalid && !failed) {
        val lhsequvcnt = Equivalencer.getEquivalenceCounts(lhs, dataset)
        val rhsequvcnt = Equivalencer.getEquivalenceCounts(rhs, dataset)
        if (lhsequvcnt != rhsequvcnt) {
          revtree.update(lhs, rhs.last)
        }
      }
    }
    revtree
  }
}