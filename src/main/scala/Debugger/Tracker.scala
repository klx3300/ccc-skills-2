package FD

object Tracker{
    val tracking = scala.collection.mutable.Map[List[Int],Boolean]()
    def track(target:List[Int]):Unit = {
        tracking(target) = true
    }
    def release(target:List[Int]):Unit = {
        tracking(target) = false
    }
    def activity(act:String,person:List[Int]):Unit = {
        if(tracking.getOrElse(person,false)){
            print("Tracker: ")
            print(person.toString)
            print(" ")
            println(act)
        }
    }
}