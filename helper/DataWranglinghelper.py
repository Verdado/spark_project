from pyspark.sql.functions import *
from pyspark.sql.window import Window


class Transformer:
    def apply_transformations(sourceDF, transformations):
        final_df = sourceDF
        for t in transformations:
            final_df = getattr(Transformer, t[0])(*(t[1:] + [final_df]))
        return final_df

    def get_session_duration(sourceCol, targetCol, partitionByCols, orderByCols, sourceDF: DataFrame):
        dummyTargetColName = "_" + targetCol
        windowSpec = Window.partitionBy(partitionByCols.split(",")).orderBy(col(*orderByCols.split(",")))
        final_df = sourceDF.withColumn(dummyTargetColName, (lead(sourceCol, 1).over(windowSpec) - col(sourceCol)).cast("long"))
        ''' session_duration with Nulls: (because there is no following action it is assumed the session times out after 60 seconds). '''
        return final_df.select(col("*"), when(col(dummyTargetColName).isNull(), 60).otherwise(col(dummyTargetColName)).alias(targetCol))

    def str_to_timestamp(sourceCol, format_ts, sourceDF):
        return sourceDF.withColumn(sourceCol, to_timestamp(sourceCol, format_ts))

    def get_session_number(sourceCol, targetCol, idle_value_in_minutes ,partitionByCols, orderByCols, sourceDF: DataFrame):
        '''
        Formula: 5minutes * 60secs = 300secs
        Session Definitions: Sessions are defined by any periods of inactivity longer than 5 minutes
        '''
        windowSpec = Window.partitionBy(partitionByCols.split(",")).orderBy(orderByCols.split(","))
        df = sourceDF.withColumn("new_session", when(lag(sourceCol, 1).over(windowSpec) > int(idle_value_in_minutes) * 60, 1).otherwise(0))
        return df.withColumn(targetCol, lpad(sum("new_session").over(windowSpec), 8, "0")).drop("new_session")

    def get_session_start_time(sourceCol, targetCol, partitionByCols, sourceDF: DataFrame):
        windowSpec = Window.partitionBy(partitionByCols.split(","))
        return sourceDF.withColumn(targetCol, min(sourceCol).over(windowSpec))

    def get_count_total_url(sourceCol, targetCol, partitionByCols, sourceDF: DataFrame):
        windowSpec = Window.partitionBy(partitionByCols.split(","))
        return sourceDF.withColumn(targetCol, count(sourceCol).over(windowSpec))

    def get_count_unique_url(sourceCol, targetCol, partitionByCols, sourceDF: DataFrame):
        windowSpec = Window.partitionBy(partitionByCols.split(","))
        return sourceDF.withColumn(targetCol, approx_count_distinct(sourceCol).over(windowSpec))

    def get_groupby_aggregate(groupByCols, aggregateCol, sourceDF: DataFrame):
        return sourceDF \
            .groupBy(*groupByCols.split(",")) \
            .sum(aggregateCol) \
            .withColumnRenamed("sum(session_duration)", aggregateCol)

    def rearrangeCols(selectCols, sourceDF: DataFrame):
        return sourceDF.select(*selectCols.split(","))
