package FD

import org.apache.spark.broadcast._

object Validator {
  def validatePartition(id: Int,
                        dataset: List[Array[String]],
                        broadSpace: Broadcast[SearchSpaceTree],
                        publicAttributesID: Int,
                        broadColumn: Broadcast[List[Int]]
                       )
  : (ReversedSearchSpaceTree, LogAccumulator) = {
    val revtree = new ReversedSearchSpaceTree(broadSpace.value.attribcnt)
    val logger = new LogAccumulator(id)
    val allLHSCombinators =
      Combinator.genRealFullCombinations(broadColumn.value.drop(publicAttributesID + 1))
        .map { x =>
          (x :+ broadColumn.value(publicAttributesID)).sorted
        }
    //    if(publicAttributesID == 7){
    //      logger.log(allLHSCombinators.toString)
    //    }
    //    if (broadColumn.value(publicAttributesID) == 0) {
    //      logger.log("AllLHS with attributes" + broadColumn.value(publicAttributesID) + "\n" + allLHSCombinators.toString)
    //    }
    for (lhs <- allLHSCombinators) {
      if (lhs.length != broadColumn.value.size) {
        val allRHSAttributes = broadSpace.value.vertices(lhs)
        //      if (lhs.contains(0) && lhs.contains(8) && lhs.contains(9)) {
        //        logger.log(lhs.toString )
        //      }
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
    }
    (revtree, logger)
  }
}