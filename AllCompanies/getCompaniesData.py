import pandas as pd 

def getCompaniesData():
    df = pd.read_csv('AllCompanies/AllSP500Companies.csv')
    #We only need the Industrials sector
    df = df[df['GICS Sector'] == 'Industrials']
    return df

