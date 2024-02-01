from DataConversors import dfToJson, dfToAvro, dfToCsv, dfToParquet, dfToExcel#, dfToOrc
import get_historical_data as DataExtractor
import datetime as dt
import sys, os
from dotenv import load_dotenv

load_dotenv()

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
        #dfToOrc.dfToOrc(self.df, fileName)
        pass

    
def main(year, fileTypes):
    # Get the start date
    start_date = dt.datetime(year, 1, 1)
    end_date = dt.datetime(year, 12, 31)
    df = DataExtractor.get_historical_data(start_date, end_date)
    dataConversors = DataConversors(df)
    otput_folder = os.getenv("HISTORICAL_DATA_LOCATION")
    if os.getenv("CREATE_SUBFOLDERS_FOR_EACH_FILETYE") == "1" or os.getenv("CREATE_SUBFOLDERS_FOR_EACH_FILETYE") == "True":
        create_subfolders = True 
    else:
        create_subfolders = False
        
    for fileType in fileTypes:
        if fileType == "json":
            output_file = f"{otput_folder}/json/{year}.json" if create_subfolders else f"{otput_folder}/{year}.json"
            dataConversors.toJson(output_file)

        elif fileType == "avro":
            output_file = f"{otput_folder}/avro/{year}.avro" if create_subfolders else f"{otput_folder}/{year}.avro"
            dataConversors.toAvro(output_file)
        
        elif fileType == "csv":
            output_file = f"{otput_folder}/csv/{year}.csv" if create_subfolders else f"{otput_folder}/{year}.csv"
            dataConversors.toCsv(output_file)

        elif fileType == "parquet":
            output_file = f"{otput_folder}/parquet/{year}.parquet" if create_subfolders else f"{otput_folder}/{year}.parquet"
            dataConversors.toParquet(output_file)

        elif fileType == "excel":
            output_file = f"{otput_folder}/excel/{year}.xlsx" if create_subfolders else f"{otput_folder}/{year}.xlsx"
            dataConversors.toExcel(output_file)

        elif fileType == "orc":
            output_file = f"{otput_folder}/orc/{year}.orc" if create_subfolders else f"{otput_folder}/{year}.orc"
            dataConversors.toOrc(output_file)

        else:
            print(f"File {fileType} type not supported")
            sys.exit(1)
    
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python main.py <year>, <file_type>")
        print('Use: "python main.py . . "to convert all years and all file types')
        sys.exit(1)
    if sys.argv[2] not in ["json", "avro", "csv", "parquet", "excel", "orc", "."]:
        print("File type not supported")
        sys.exit(1)
    file_types = ["json", "avro", "csv", "parquet", "excel", "orc"]
    years = [2018, 2019, 2020, 2021, 2022, 2023]
    if sys.argv[1] != ".":
        years = [int(sys.argv[1])]
    if sys.argv[2] != ".":
        file_types = [sys.argv[2]]
    
    for year in years:
        main(year, file_types)



        
