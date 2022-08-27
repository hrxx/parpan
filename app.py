import pandas as pd
from pp import ParallelDF
import sys

def g(df):
    # do some operations
    df['a'] = df['a'].apply(lambda x:x+1)
    return df

if __name__ == '__main__':
    df = pd.read_csv('input.csv')
    # df = g(df) non parallel
    pldf = ParallelDF(df, g)
    df = pldf.parallel_run()
    df.to_csv('result.csv', index=False)
