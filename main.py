from pyspark.sql import SparkSession
from helper.DataWranglinghelper import Transformer
from helper.ConfigLoader import configLoader
from helper.TableLoader import tableLoader
from helper.WriteHelper import WriteDF

''' Get/create sparkSession'''
spark = SparkSession.builder.appName('Sessionize_data').getOrCreate()
''' Read table config '''
table_metadata = configLoader.read_config_json()
''' LOAD '''
final_df = tableLoader.load_file(spark, table_metadata)
''' Transform '''
final_df = Transformer.apply_transformations(final_df, table_metadata['transformations'])
''' Write dataframe '''
WriteDF.write(final_df)
''' Show final Table'''
final_df\
    .orderBy("User_ID","Session_number")\
    .show(2000, truncate=False)
