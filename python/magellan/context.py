#
# Copyright 2015 Ram Sriharsha
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys

from pyspark import SparkContext
from pyspark.sql import SQLContext

__all__ = ["MagellanContext"]

class MagellanContext(SQLContext):
    """A variant of Spark SQL that integrates with spatial data.

    :param sparkContext: The SparkContext to wrap.
    :param magellanContext: An optional JVM Scala MagellanContext. If set, we do not create a new
        :class:`MagellanContext` in the JVM, instead we make all calls to this object.
    """

    def __init__(self, sparkContext, magellanContext=None):
        SQLContext.__init__(self, sparkContext)
        if magellanContext:
            self._scala_MagellanContext = magellanContext

    @property
    def _ssql_ctx(self):
        if not hasattr(self, '_scala_MagellanContext'):
            self._scala_MagellanContext = self._get_magellan_ctx()
        return self._scala_MagellanContext

    def _get_magellan_ctx(self):
        sc = SparkContext._active_spark_context
        loader = sc._jvm.Thread.currentThread().getContextClassLoader()
        wclass = loader.loadClass("org.apache.spark.sql.magellan.MagellanContext")
        expr_class = sc._jvm.java.lang.Object
        expr_array = sc._gateway.new_array(expr_class, 1)
        expr_array[0] = self._jsc.sc()
        w = wclass.getConstructors()[0].newInstance(expr_array)
        return w

