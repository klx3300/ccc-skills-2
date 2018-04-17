package FD

object Equivalencer {
  def getEquivalenceCounts(attribs: List[Int], dataset: List[Array[MyString]]): Int = {
    dataset.map(x => attribs.collect(x)).distinct.size
    //    val counter = scala.collection.mutable.Map[List[String], Int]()
    //    for (x <- dataset) {
    //      val buffer = attribs.collect(x)
    //      if (counter.contains(buffer))
    //        counter(buffer) += 1
    //      else
    //        counter(buffer) = 1
    //    }
    //    counter.size
  }
}