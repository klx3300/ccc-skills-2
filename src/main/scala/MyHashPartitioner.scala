package FD

import org.apache.spark.Partitioner

class MyHashPartitioner[V](
    partitions:Int
) extends Partitioner{
    def getPartition(key: Any):Int = {
        val k = key.asInstanceOf[Int]
        return k%partitions
    }
}