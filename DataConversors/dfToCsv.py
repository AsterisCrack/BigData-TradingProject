def dfToCsv(df, fileName):
    df.to_csv(fileName, index=False, header=True)