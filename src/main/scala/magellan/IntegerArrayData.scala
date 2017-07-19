/**
 * Copyright 2015 Ram Sriharsha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package magellan

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}


class IntegerArrayData(v: Array[Int]) extends ArrayData {

  override def numElements(): Int = v.length

  override def copy(): ArrayData = {
    val a = new Array[Int](v.length)
    Array.copy(v, 0, a, 0, v.length)
    new IntegerArrayData(a)
  }

  def setNullAt(i: Int): Unit = ???

  def update(i: Int,value: Any): Unit = ???

  override val array: Array[Any] = v.map(_.asInstanceOf[Any])

  override def getUTF8String(ordinal: Int): UTF8String = ???

  override def get(ordinal: Int, dataType: DataType): AnyRef = ???

  override def getBinary(ordinal: Int): Array[Byte] = ???

  override def getDouble(ordinal: Int): Double = v(ordinal).toDouble

  override def getArray(ordinal: Int): ArrayData = ???

  override def getInterval(ordinal: Int): CalendarInterval = ???

  override def getFloat(ordinal: Int): Float = ???

  override def getLong(ordinal: Int): Long = ???

  override def getMap(ordinal: Int): MapData = ???

  override def getByte(ordinal: Int): Byte = ???

  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = ???

  override def getBoolean(ordinal: Int): Boolean = ???

  override def getShort(ordinal: Int): Short = ???

  override def getStruct(ordinal: Int, numFields: Int): InternalRow = ???

  override def getInt(ordinal: Int): Int = v(ordinal)

  override def isNullAt(ordinal: Int): Boolean = false

  override def toIntArray() = v

}

