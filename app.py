import pandas as pd
from pp import parallel_df
import sys

@parallel_df
def f(df):
    return g(df)

def g(df):
    # do some operations
    print(type(df))
    return df

if __name__ == '__main__':
    df = pd.read_csv('input.csv')
    df = f(df)
    df.to_csv('result.csv', index=False)
