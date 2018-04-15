package FD

import org.apache.spark.broadcast._

import scala.collection.mutable

object Validator {
  def validatePartition(id: Int,
                        dataset: List[Array[String]],
                        broadSpace: Broadcast[SearchSpaceTree],
                        publicAttributesID: Int,
                        broadColumn: Broadcast[List[Int]],
                        broadLHS: Broadcast[List[List[Int]]]
                       )
  : (ReversedSearchSpaceTree, LogAccumulator) = {
    val revtree = new ReversedSearchSpaceTree(broadSpace.value.attribcnt)
    val logger = new LogAccumulator(id)

    for (lhs <- broadLHS.value) {
      val allRHSAttributes = broadSpace.value.vertices.getOrElse(lhs, mutable.Map.empty)
      for ((rhs, canValid) <- allRHSAttributes) {
        if (canValid) {
          val lhsequvcnt = Equivalencer.getEquivalenceCounts(lhs, dataset)
          val rhsequvcnt = Equivalencer.getEquivalenceCounts(rhs, dataset)

          if (lhsequvcnt != rhsequvcnt) {
            revtree.update(lhs, rhs.last)
          }
        }
      }
    }
    (revtree, logger)
  }
}