from DataConversors import dfToJson, dfToAvro, dfToCsv, dfToParquet, dfToExcel, dfToOrc

class DataConversors:
    # Constructor of the class
    def __init__(self, df):
        self.df = df

    # json conversion
    def toJson(self, fileName) -> None:
        dfToJson.dfToJson(self.df, fileName)

    # avro conversion
    def toAvro(self, fileName) -> None:
        dfToAvro.dfToAvro(self.df, fileName)

    # csv conversion
    def toCsv(self, fileName) -> None:
        dfToCsv.dfToCsv(self.df, fileName)
    
    # parquet conversion
    def toParquet(self, fileName) -> None:
        dfToParquet.dfToParquet(self.df, fileName)

    # excel conversion
    def toExcel(self, fileName) -> None:
        dfToExcel.dfToExcel(self.df, fileName)

    # orc conversion
    def toOrc(self, fileName) -> None:
        dfToOrc.dfToOrc(self.df, fileName)