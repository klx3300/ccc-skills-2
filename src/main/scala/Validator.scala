package FD

import org.apache.spark.broadcast._
import scala.collection.mutable._

object Validator{
    def validatePartition(dataset:List[Array[String]],spacetree:Broadcast[SearchSpaceTree],possibcombs:Broadcast[Map[List[Int],Boolean]]):(ReversedSearchSpaceTree,LogAccumulator)={
        val revtree = new ReversedSearchSpaceTree(spacetree.value.attribcnt,possibcombs.value)
        val loga = new LogAccumulator()
        for((lhs,possibrhs) <- spacetree.value.vertices){
            for((rhs,canvalid) <- possibrhs){
                val failed = revtree.vertices(lhs).getOrElse(rhs,false)
                if(canvalid==true && failed == false){
                    val lhsequvcnt = Equivalencer.getEquivalenceCounts(lhs,dataset,loga)
                    val rhsequvcnt = Equivalencer.getEquivalenceCounts(rhs,dataset,loga)
                    if(lhsequvcnt != rhsequvcnt){
                        //if(lhs.contains(2) && lhs.contains(6)) loga.log("Invalidation at "+lhs.toString + " => " + rhs.toString)
                        revtree.update(lhs,rhs(rhs.length-1))
                    }
                }
            }
        }
        (revtree,loga)
    }
}