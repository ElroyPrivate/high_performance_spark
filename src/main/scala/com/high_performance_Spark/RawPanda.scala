package com.high_performance_Spark

import java.util
import java.util.Objects

/**
  *
  * @param id panda id
  * @param zip  zip code of panda residence
  * @param pt Type of panda as a string
  * @param happy  if panda is happy
  * @param attributes array of panda attributes
  */
case class RawPanda(id:Long,zip:String,pt:String,
                    happy:Boolean,attributes:Array[Double]){

  override def equals(o:Any)=o match{
    case other:com.high_performance_Spark.RawPanda =>(id==other.id&&pt==other.pt)
  }

  override def hashCode(): Int = {
    3 *Objects.hashCode(id)+7*Objects.hashCode(zip)+
      11 * Objects.hashCode(pt)+13 * util.Arrays.hashCode(attributes)
  }
}

/**
  *
  * @param name panda place
  * @param pandas pandas in that place
  */

case class PandaPlace(name:String,pandas:Array[RawPanda])

case class CoffeeShop(zip:String,name:String)