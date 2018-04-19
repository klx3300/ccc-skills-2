package FD


object Validator {

  import org.apache.spark.broadcast._

  import scala.collection.mutable.Map

  def validatePartition(id: Int,
                        dataset: List[Array[Int]],
                        broadSpace: Broadcast[LazySearchSpaceTree],
                        publicAttributesID: Int,
                        broadColumn: Broadcast[List[Int]],
                        broadLHS: Broadcast[List[(List[Int],Int)]]
                       )
  : (Map[(List[Int],Int),Boolean], LogAccumulator) = {
    val statusmap = Map[(List[Int],Int),Boolean]()
    val logger = new LogAccumulator(id)
    for(lhs <- broadLHS.value){
      val rhs = lhs._1.+:(lhs._2)
      val lhsequvcnt = Equivalencer.getEquivalenceCounts(lhs._1, dataset)
      val rhsequvcnt = Equivalencer.getEquivalenceCounts(rhs, dataset)
      if(lhsequvcnt != rhsequvcnt){
        statusmap((lhs._1,lhs._2)) = false
      }else{
        statusmap((lhs._1,lhs._2)) = true
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