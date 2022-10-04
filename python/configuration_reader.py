"""
Methods to retrieve the configuration of the given analytical view from the config db in SQL Server
and store it in a nested Python dictionary.
"""
import collections


def retrieve_config(spark, ds, analytical_view_name):
    """You need to review this function.

    Retrieve the configuration of a given analytical view from the config db.

    Args:
        spark: spark session.
        ds: data source to access the sql server db.
        analytical_view_name: the name of the analytical view.
    Returns:
        a nested dictionary which contains the configuration of the analytical view. The configuration consists of three
        parts: definition, dimensions, and measures.
    """
    return apply_func({'definition': analytical_view_definition(spark, ds, analytical_view_name).collect()[0].asDict(),
                       'dimensions': [x.asDict() for x in
                                      analytical_view_dimensions(spark, ds, analytical_view_name).collect()],
                       'measures': [x.asDict() for x in
                                    analytical_view_measures(spark, ds, analytical_view_name).collect()]},
                      do_normalize,
                      {})


def apply_func(obj, func, skipped_keys):
    """You need to review this function.

    Recursively apply func to the value of obj and skip the keys from the skipped_keys set.

    When obj is a list, apply func to all the items in the list and return the transformed list.

    When obj is a dict, apply func to the value of each item if key of the item is not in skipped_keys set
    and return the transformed dict.

    When obj is a scalar type, apply func to it and return the transformed value.

    When obj is None, do nothing and return None.

    Args:
        obj: a dict, list, scalar type, or None.
        func: a function to be applied to obj and returns the transformed obj.
        skipped_keys: a set of keys to be skipped when obj is a dict.
    Returns:
        the transformed obj.
    """
    if isinstance(obj, collections.Mapping):
        result = {}
        for k, v in obj.items():
            if k in skipped_keys:
                result[k] = v
            else:
                result[k] = apply_func(v, func, skipped_keys)
        return result
    elif isinstance(obj, list):
        return [apply_func(v, func, skipped_keys) for v in obj]
    elif obj is None:
        return None
    else:
        return func(obj)


def analytical_view_definition(spark, ds, analytical_view_name):
    """You can skip this function for this code review exercise.

    Query the analytical_view_definition table from the config db in SQL Server and return the definition of
    the analytical view with the given analytical_view_name.

    Args:
        spark: spark session.
        ds: data source to access the sql server db.
        analytical_view_name: the name of the analytical view.
    Returns:
        a pyspark dataframe containing the definition of the given analytical view.

        DATAFRAME_SCHEMA_ANALYTICAL_VIEW_DEFINITION =
        StructType([
            StructField("analytical_view_name", StringType(), False),
            StructField("source_table_name", StringType(), False),
            StructField("processing_mode", StringType(), False),
            StructField("result_table_name", StringType(), False)
        ])
    """


def analytical_view_dimensions(spark, ds, analytical_view_name):
    """You can skip this function for this code review exercise.

    Query the analytical_view_dimension table from the config db in SQL Server and return the dimensions
    of the analytical view with the given analytical_view_name.

    Args:
        spark: spark session.
        ds: data source to access the sql server db.
        analytical_view_name: the name of the analytical view.
    Returns:
        a pyspark dataframe containing the dimensions of the given analytical view.

        DATAFRAME_SCHEMA_ANALYTICAL_VIEW_DIMENSION =
        StructType([
            StructField("analytical_view_name", StringType(), False),
            StructField("dimension", StringType(), False),
            StructField("column_in_source_table", StringType(), False),
        ])
    """


def analytical_view_measures(spark, ds, analytical_view_name):
    """You can skip this function for this code review exercise.

    Query the analytical_view_measure table from the config db in SQL Server and return the measures
    of the analytical view with the given analytical_view_name.

    Args:
        spark: spark session.
        ds: data source to access the sql server db.
        analytical_view_name: the name of the analytical view.
    Returns:
        a pyspark dataframe containing the measures of the given analytical view.

        DATAFRAME_SCHEMA_ANALYTICAL_VIEW_MEASURE =
        StructType([
            StructField("analytical_view_name", StringType(), False),
            StructField("measure", StringType(), False),
            StructField("column_in_source_table", StringType(), False)
        ])
    """


def do_normalize(x):
    """You can skip this function for this code review exercise.

    Normalize the format of the given string, e.g., strip away spaces and turn into upper cases.
    Return the normalized string.

    Args:
        x: the string to be normalized, e.g., strip away spaces and turn into upper cases.
    Returns:
        the normalized string.
    """
