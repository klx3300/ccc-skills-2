package FD

import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer

class ReversedSearchSpaceTree(val attribcnt:Int){
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

    }
}