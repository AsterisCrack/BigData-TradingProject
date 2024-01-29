import dfToJson, dfToAvro, dfToCsv, dfToParquet, dfToExcel, dfToOrc
import sys


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

    
def main():

    if len(sys.argv) < 3:
        print("Usage: python main.py <file_name>, <file_type>")
        sys.exit(1)
    
    df = sys.argv[1]
    fileType = sys.argv[2]

    dataConversors = DataConversors(df)

    if fileType == "json":
        dataConversors.toJson("output.json")

    elif fileType == "avro":
        dataConversors.toAvro("output.avro")
    
    elif fileType == "csv":
        dataConversors.toCsv("output.csv")

    elif fileType == "parquet":
        dataConversors.toParquet("output.parquet")

    elif fileType == "excel":
        dataConversors.toExcel("output.xlsx")

    elif fileType == "orc":
        dataConversors.toOrc("output.orc")

    else:
        print("File type not supported")
        sys.exit(1)
    
if __name__ == "__main__":
    main()



        
