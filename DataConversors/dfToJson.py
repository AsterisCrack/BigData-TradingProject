def dfToJson(df, fileName) -> None:
    df.to_json(fileName, orient='records')
    