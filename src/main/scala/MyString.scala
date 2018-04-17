package FD


class MyString extends Serializable {
    var content = Array[Byte]()
    var hcd = 0
    def init(str: String): Unit ={
        content = (for(x <- str) yield x.toByte).toArray
        hcd = content.toList.hashCode
    }
    override def toString(): String = {
        val tmpstr = content.toList.toString
        tmpstr
    }
    override def hashCode(): Int = {
        hcd
    }
    override def equals(another: Any): Boolean = {
        // comparing by hash codes has potential collision bugs.
        another.asInstanceOf[MyString].content.toList == content.toList
    }
}