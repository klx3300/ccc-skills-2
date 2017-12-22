package FD

import scala.collection.mutable._
import scala.collection.mutable.ListBuffer

class ReversedSearchSpaceTree(val attribcnt:Int,val possibcombs:Map[List[Int],Boolean]) extends Serializable{
    val vertices = scala.collection.mutable.Map[List[Int],scala.collection.mutable.Map[List[Int],Boolean]]()
    def init():Unit={
        val tmpcombs = Combinator.genCombinations(attribcnt)
        for(x <- tmpcombs){
            vertices(x) = scala.collection.mutable.Map[List[Int],Boolean]()
        }
    }
    init()
    def update(lhs:List[Int],rhs:Int):Unit={
        // append everything into the vertices map
        val allcombs = Combinator.genCombinations(lhs)
        // first, cancel the LHS one
        val buffer = ListBuffer[Int]()
        buffer.appendAll(lhs)
        buffer.append(rhs)
        vertices(lhs)(buffer.toList) = true
        // second, cancel the all combs one
        for(x <- allcombs){
            val abuffer = ListBuffer[Int]()
            abuffer.appendAll(x)
            abuffer.append(rhs)
            vertices(x)(abuffer.toList) = true
        }
    }
    def merge(revtree:ReversedSearchSpaceTree):Unit={
        for((lhs,rhsmap) <- revtree.vertices){
            for((dest,iscorr) <- rhsmap){
                vertices(lhs)(dest) = iscorr
            }
        }
    }
}
