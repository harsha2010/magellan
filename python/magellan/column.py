#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from pyspark import SparkContext
from pyspark.sql.column import Column

def _bin_op(name, doc="binary operator"):
    """ Create a method for given binary operator
    """
    def _(col, other):
        jc = other._jc if isinstance(other, Column) else other
        jcol = col._jc
        sc = SparkContext._active_spark_context
        loader = sc._jvm.Thread.currentThread().getContextClassLoader()
        wclass = loader.loadClass(name)
        expr_class = sc._jvm.java.lang.Object
        expr_array = sc._gateway.new_array(expr_class, 2)
        expr_array[0] = jcol.expr()
        expr_array[1] = jc.expr()
        w = wclass.getConstructors()[0].newInstance(expr_array)
        wcol = sc._jvm.org.apache.spark.sql.Column(w)
        return Column(wcol)
    _.__doc__ = doc
    return _

within = _bin_op("org.apache.magellan.catalyst.Within")
Column.within = within