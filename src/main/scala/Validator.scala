package FD


object Validator {

  import org.apache.spark.broadcast._

  import scala.collection.mutable.Map

  def validatePartition(id: Int,
                        dataset: List[Array[Int]],
                        broadSpace: Broadcast[LazySearchSpaceTree],
                        publicAttributesID: Int,
                        broadColumn: Broadcast[List[Int]],
                        broadLHS: Broadcast[List[List[Int]]]
                       )
  : (Map[(List[Int],Int),Boolean], LogAccumulator) = {
    val statusmap = Map[(List[Int],Int),Boolean]()
    val logger = new LogAccumulator(id)
    for(lhs <- broadLHS.value){
      val lhsset = lhs.toSet
      for(tryrhs <- (0 until broadSpace.value.attribcnt).filterNot(x => lhsset.contains(x))){
        if(broadSpace.value.shouldValidate(lhs,tryrhs)){
          val rhs = lhs+:tryrhs
          val lhsequvcnt = Equivalencer.getEquivalenceCounts(lhs, dataset)
          val rhsequvcnt = Equivalencer.getEquivalenceCounts(rhs, dataset)
          if(lhsequvcnt != rhsequvcnt){
            statusmap((lhs,tryrhs)) = false
          }else{
            statusmap((lhs,tryrhs)) = true
          }
        }
      }
    }
/*
    for (lhs <- broadLHS.value) {
      val allRHSAttributes = broadSpace.value.vertices.getOrElse(lhs, Map.empty)
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
*/
    (statusmap, logger)
  }
}