from pyspark.sql.types import UserDefinedType
from spylon.spark.utils import SparkJVMHelpers


if not hasattr(UserDefinedType, 'original_from_json'):
    UserDefinedType.original_from_json = UserDefinedType.fromJson

@classmethod
def _fixed_from_json(cls, json):
    # print json
    if not json["pyClass"]:
        json["pyClass"] = json['class'].replace("org.apache.spark.sql.types", "magellan.types")
    return cls.original_from_json(json)


def _fix_from_json():
    UserDefinedType.fromJson = _fixed_from_json

_fix_from_json()


def inject_rules(spark_session):
    from magellan.column import *
    from magellan.dataframe import *

    jvm_helpers = SparkJVMHelpers(spark_session._sc)
    magellan_utils = jvm_helpers.import_scala_object('magellan.Utils')
    magellan_utils.injectRules(spark_session._jsparkSession)