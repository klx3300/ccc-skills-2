package FD

import org.apache.spark.broadcast._
import scala.collection.mutable._

object Validator{
    def validatePartition(id:Int,dataset:List[Array[String]],spacetree:Broadcast[SearchSpaceTree]/*,possibcombs:Broadcast[Map[List[Int],Boolean]]*/,pubattribs:List[Int]):(ReversedSearchSpaceTree,LogAccumulator)={
        val revtree = new ReversedSearchSpaceTree(spacetree.value.attribcnt/*,possibcombs.value*/)
        val loga = new LogAccumulator(id)
        val possibrhs = spacetree.value.vertices(pubattribs)
        val lhs = pubattribs
        for((rhs,canvalid) <- possibrhs){
            val failed = revtree.vertices(lhs).getOrElse(rhs,false)
            if(canvalid==true && failed == false){
                /*if(lhs.contains(1) && lhs.contains(8) && rhs.contains(9) && lhs.size == 2){
                    val lhsequvcnt = Equivalencer.getEquivalenceCounts(lhs,dataset,loga)
                    val rhsequvcnt = Equivalencer.getEquivalenceCounts(rhs,dataset,loga)
                    if(lhsequvcnt != rhsequvcnt){
                        revtree.update(lhs,rhs(rhs.length-1))
                    }
                }else{*/
                    val lhsequvcnt = Equivalencer.getEquivalenceCounts(lhs,dataset,null)
                    val rhsequvcnt = Equivalencer.getEquivalenceCounts(rhs,dataset,null)
                    if(lhsequvcnt != rhsequvcnt){
                        revtree.update(lhs,rhs(rhs.length-1))
                    }
                //}
                }
        }
        (revtree,loga)
    }
}