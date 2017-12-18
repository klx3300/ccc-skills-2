package FD

import org.apache.spark.broadcast._

object Validator{
    def validatePartition(dataset:List[Array[String]],spacetree:Broadcast[SearchSpaceTree]):ReversedSearchSpaceTree={
        val revtree = new ReversedSearchSpaceTree(spacetree.value.attribcnt)
        for((lhs,possibrhs) <- spacetree.value.vertices){
            for((rhs,canvalid) <- possibrhs){
                val failed = revtree.vertices(lhs).getOrElse(rhs,false)
                if(canvalid==true && failed == false){
                    val lhsequvcnt = Equivalencer.getEquivalenceCounts(lhs,dataset)
                    val rhsequvcnt = Equivalencer.getEquivalenceCounts(rhs,dataset)
                    if(lhsequvcnt != rhsequvcnt){
                        revtree.update(lhs,rhs(rhs.length-1))
                    }
                }
            }
        }
        revtree
    }
}