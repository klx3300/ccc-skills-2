package FD

import scala.collection.mutable.ListBuffer

object IOController{

    def FDsShrink(fds:scala.collection.mutable.Map[List[Int],List[Int]]):scala.collection.mutable.Map[List[Int],List[Int]]={
        // the key is to reverse the key-value relation
        val shrinked = scala.collection.mutable.Map[List[Int],List[Int]]()
        val reversed = scala.collection.mutable.Map[List[Int],List[Int]]()
        for((lhs,rhs) <- fds){
            if(!shrinked.contains(rhs)){
                shrinked(rhs) = lhs
            }else{
                // get the common
                var vca=0
                var vcb=0
                val prevlhs = shrinked(rhs)
                val buffer = ListBuffer[Int]()
                while(vca < lhs.length && vcb < prevlhs.length){
                    if(lhs(vca) == prevlhs(vcb)){
                        buffer += lhs(vca)
                        vca=vca+1
                        vcb=vcb+1
                    }else if(lhs(vca) < prevlhs(vcb)){
                        vca=vca+1
                    }else{
                        vcb=vcb+1
                    }
                }
                shrinked(rhs)=buffer.toList
            }
        }
        for((rhs,lhs) <- shrinked){
            reversed(lhs) = rhs
        }
        reversed
    }

    def FDstoString(fds:scala.collection.mutable.Map[List[Int],List[Int]]):List[String]={
        val buffer = ListBuffer[String]()
        for((lhs,rhs) <- fds){
            val strbuf = new StringBuffer()
            val corrlhs = lhs.map(x=>x+1)
            for(x <- corrlhs){
                strbuf.append(x)
                if(x != corrlhs.last) strbuf.append(",")
            }
            strbuf.append(":")
            val corrrhs = rhs.map(x=>x+1)
            for(x <- corrrhs){
                strbuf.append(x)
                if(x != corrrhs.last) strbuf.append(",")
            }
            buffer += strbuf.toString
        }
        buffer.toList
    }
}