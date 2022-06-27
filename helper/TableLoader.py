
class tableLoader:
    def load_file(sparkSession, table_metadata):
        return sparkSession.read.format(table_metadata['format']) \
            .option("delimiter", table_metadata['delimiter']) \
            .option("header", table_metadata['header']) \
            .load(table_metadata['location'])
