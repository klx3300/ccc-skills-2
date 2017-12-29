package FD

import scala.collection.mutable._
import scala.collection.mutable.ListBuffer

class SearchSpaceTree(val attribcnt:Int) extends Serializable{
    val vertices = Map[List[Int],Map[List[Int],Boolean]]()
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
    def merge(revtree:ReversedSearchSpaceTree,possibcombs:Map[List[Int],Boolean]):Unit={
        for((lhs,rhsmap) <- revtree.vertices){
            for((rhs,isboomed) <- rhsmap){
                if(isboomed){
                    Tracker.activity("Invalidated => "+rhs.toString,lhs)
                    if(possibcombs.getOrElse(lhs,false)) possibcombs.remove(lhs)
                    vertices(lhs)(rhs) = false
                }
            }
        }
    }
    def toFDs():Map[List[Int],List[Int]]={ // not shrinked yet
        val fds = Map[List[Int],List[Int]]()
        for((lhs,rhsmap) <- vertices){
            val tmpbuffer = ListBuffer[Int]()
            for((rhs,iscorr) <- rhsmap){
                if(iscorr) tmpbuffer+=rhs(rhs.length-1)
            }
            fds(lhs)=tmpbuffer.toList
        }
        fds
    }
}