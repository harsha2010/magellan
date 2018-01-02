from spylon.spark.utils import SparkJVMHelpers


def inject_rules(spark_session):
    from magellan.column import *
    from magellan.dataframe import *

    jvm_helpers = SparkJVMHelpers(spark_session._sc)
    magellan_utils = jvm_helpers.import_scala_object('magellan.Utils')
    magellan_utils.incjectRules(spark_session._jsparkSession)