package com.ssort

class SecondSort(val v1 : String, val v2 : Int) extends Ordered[SecondSort] with Serializable {

  override def compare(other: SecondSort): Int = {
    //先按照第一列作升序排序
    val result = this.v1.compareTo(other.v1)
    println("第一列排序结果" + result);
    if(result == 0){
      other.v2.compareTo(this.v2)
    } else {
      result
    }
  }
}
