from pyspark.sql import DataFrame
from spylon.spark.utils import SparkJVMHelpers


def _immutable_scala_map(jvm_helpers, dict_like):
    jvm_helpers.to_scala_map(dict_like).toMap(jvm_helpers.jvm.scala.Predef.conforms())


def index(df, precision):
    jvm_helpers = SparkJVMHelpers(df._sc)
    jdf = df._jdf
    sparkSession = jdf.sparkSession()
    SpatialJoinHint = df._sc._jvm.magellan.catalyst.SpatialJoinHint
    Dataset = df._sc._jvm.org.apache.spark.sql.Dataset

    new_jdf = Dataset(
        sparkSession,
        SpatialJoinHint(
            jdf.logicalPlan(),
            _immutable_scala_map(jvm_helpers, {"magellan.index.precision": str(precision)})
        ),
        jdf.exprEnc())

    return DataFrame(new_jdf, df.sql_ctx)


DataFrame.index = index
