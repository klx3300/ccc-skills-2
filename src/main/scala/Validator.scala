package FD

import org.apache.spark.broadcast._

import scala.collection.mutable

object Validator {
  def validatePartition(id: Int, dataset: List[Array[String]], spacetree: Broadcast[SearchSpaceTree], pubattribs: List[Int]): mutable.HashSet[List[Int]] = {
    val boomedlogs = new mutable.HashSet[List[Int]]()
    val possibrhs = spacetree.value.vertices(pubattribs)
    val lhs = pubattribs
    for ((rhs, canvalid) <- possibrhs) {
      if (canvalid) {
        val lhsequvcnt = Equivalencer.getEquivalenceCounts(lhs, dataset)
        val rhsequvcnt = Equivalencer.getEquivalenceCounts(rhs, dataset)
        if (lhsequvcnt != rhsequvcnt) {
          val allboomed = Combinator.genFullCombinations(lhs).map(x => x:+rhs.last)
          for(x <- allboomed){
            boomedlogs.add(x)
          }
        }
      }
    }
    boomedlogs
  }
}