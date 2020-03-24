package com.kryo

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

class MyKryoRegister extends KryoRegistrator{

  override def registerClasses(kryo: Kryo): Unit = {
    //将Person类做注册, 在序列化Person对象,用的Kryo
    kryo.register(classOf[Person])

  }
}
