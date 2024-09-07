package distributedTest

class Task extends Serializable {   //若是Java中是接口，要implements，scala就extend
    var datas = List(1, 2, 3, 4)

    val logic : (Int)=>Int = _*2

    //计算
    def compute() = {
        datas.map(logic)
    }

}
