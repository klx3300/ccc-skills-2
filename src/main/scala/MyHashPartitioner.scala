package FD

import org.apache.spark.Partitioner

class MyHashPartitioner[V](
    partitions:Int
) extends Partitioner{
    val numPartitions = partitions
    def getPartition(key: Any):Int = {
        val k = key.asInstanceOf[Int]
        return k%partitions
    }
}