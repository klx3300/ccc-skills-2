package FD

class MyHashPartitioner[V](
                            partitions: Int
                          ) extends org.apache.spark.Partitioner {
  val numPartitions: Int = partitions

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Int]
    ((k % partitions) + partitions) % partitions
  }
}