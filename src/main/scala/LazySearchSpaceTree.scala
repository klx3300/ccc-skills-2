package FD

import scala.collection.mutable.Map

class LazySearchSpaceTree(val attribcnt:Int) extends Serializable {
    // RHS(sing) => (LHS,status): true: POSITIVE false:NEGATIVE
    val vertices = Map[Int, Map[List[Int],Boolean]]()
    init()
    def init():Unit ={
        for(x <- 0 until attribcnt){
            vertices(x) = Map[List[Int],Boolean]()
        }
    }
    def shouldValidate(lhs:List[Int],rhs:Int):Boolean = {
        val validatedlhs = vertices.getOrElse(rhs,Map())
        val lhsset = lhs.toSet
        for((vlhs,status) <- validatedlhs){
            val vlhsset = vlhs.toSet
            if(status){
                if(vlhsset.subsetOf(lhsset)){
                    return false // must POSITIVE. no need
                }
            }else{
                if(lhsset.subsetOf(vlhsset)){
                    return false // must NEGATIVE. no need
                }
            }
        }
        true
    }
    def update(lhs:List[Int],rhs:Int,status:Boolean):Unit = {
        val validatedlhs = vertices.getOrElse(rhs,Map())
        val updated = validatedlhs.clone
        // if it's not in the VERTICES, should panic. but we just omit it..
        val lhsset = lhs.toSet
        for((vlhs,vstatus) <- validatedlhs){
            val vlhsset = vlhs.toSet
            if(status){
                if(vlhsset.subsetOf(lhsset)){
                    updated.remove(vlhs)
                }
            }else{
                if(lhsset.subsetOf(vlhsset)){
                    updated.remove(vlhs)
                }
            }
        }
        updated(lhs) = status
        vertices(rhs) = updated
    }
    def toFDs():List[(List[Int],Int)] = {
        var lst = List[(List[Int],Int)]()
        for((rhs,lhsmap) <- vertices){
            for((lhs,status) <- lhsmap){
                if(status){
                    lst = lst :+ ((lhs,rhs))
                }
            }
        }
        lst
    }
}