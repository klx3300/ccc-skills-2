package FD

object Combinator {
  def genCombinations(attribcnt: Int): List[List[Int]] = {
    val range = (0 until attribcnt).toList
    1.until(attribcnt).flatMap(range.combinations).toList
  }

  def genCombinations(attribs: List[Int]): List[List[Int]] = {
    1.until(attribs.length).flatMap(attribs.combinations).toList
  }
  def genFullCombinations(attribs: List[Int]): List[List[Int]] = {
    1.until(attribs.length+1).flatMap(attribs.combinations).toList
  }
  def genRealFullCombinations(attribs: List[Int]): List[List[Int]] = {
    0.until(attribs.length+1).flatMap(attribs.combinations).toList
  }
}