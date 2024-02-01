def dfToExcel(df, fileName) -> None:
    df.to_excel(fileName, index=False)

