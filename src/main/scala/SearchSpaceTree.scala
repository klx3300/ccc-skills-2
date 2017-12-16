package FD

import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer

class SearchSpaceTree(val attribcnt:Int){
    val vertices = scala.collection.mutable.Map[List[Int],scala.collection.mutable.Map[List[Int],Boolean]]()
    def init():Unit={
        val tmpcombs = Combinator.genCombinations(attribcnt)
        for(x <- tmpcombs){
            vertices(x) = scala.collection.mutable.Map[List[Int],Boolean]()
            var xcurr=0
            var attribcurr=0
            while(xcurr < x.length){
                while(attribcurr < x(xcurr)){
                    val destbuffer=ListBuffer[Int]()
                    for(y <- x) destbuffer+=y
                    destbuffer += attribcurr
                    vertices(x)(destbuffer.toList) = true
                    attribcurr = attribcurr + 1
                }
                attribcurr = x(xcurr)+1
                xcurr = xcurr + 1
            }
            // can i go further than it?
            while(attribcurr < attribcnt){
                val destbuffer = ListBuffer[Int]()
                for(y<-x) destbuffer+=y
                destbuffer += attribcurr
                vertices(x)(destbuffer.toList) = true
                attribcurr = attribcurr + 1
            }
        }
    }
    init()
    def merge(revtree:ReversedSearchSpaceTree):Unit={
        for((lhs,rhsmap) <- revtree.vertices){
            for((rhs,isboomed) <- rhsmap){
                vertices(lhs)(rhs) = !isboomed
            }
        }
    }
}