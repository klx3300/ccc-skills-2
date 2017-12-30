import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

object Combinator{
  def genCombinations(attribcnt:Int):List[List[Int]]={
    /*val topbuffer =new ListBuffer[List[Int]]()
    for(currcnt <- 1 until attribcnt){
      val bottombuffer = new ListBuffer[Int]()
      for(i <- 0 until currcnt){
        bottombuffer += i
      }
      topbuffer += bottombuffer.toList
      while(bottombuffer(0)<attribcnt-currcnt){ // EX condition: the largest r-comb in dict order
        breakable{
          for(operpos <- (currcnt-1).to(0,-1)){
            // find last element that satisfy:NOT REACH ITS MAXIMUM
            if(bottombuffer(operpos)<attribcnt-currcnt+operpos){
              bottombuffer(operpos) = bottombuffer(operpos)+1
              for(updatepos <- operpos+1 until currcnt){
                bottombuffer(updatepos) = bottombuffer(operpos)+updatepos-operpos
              }
              topbuffer+=bottombuffer.toList
              break
            }
          }
        }
      }
    }
    topbuffer.toList*/
    val range = (0 until attribcnt).toList
    (1.until(attribcnt)).flatMap(range.combinations).toList
  }
  def genCombinations(attribs:List[Int]):List[List[Int]]={
    /*val topbuffer = ListBuffer[List[Int]]()
    val indexbasedcombs = genCombinations(attribs.length)
    for(x <- indexbasedcombs){
      val bottombuffer = ListBuffer[Int]()
      for(y <- x) bottombuffer += attribs(y)
      topbuffer += bottombuffer.toList
    }
    topbuffer.toList*/
    (1.until(attribs.length)).flatMap(attribs.combinations).toList
  }
}