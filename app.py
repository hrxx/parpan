import pandas as pd
from pp import ParallelDF
import sys

def g(df,incr):
    # do some operations
    df['a'] = df['a'].apply(lambda x:x+incr)
    return df

if __name__ == '__main__':
    df = pd.read_csv('input.csv')
    # df = g(df, 2) non parallel
    pldf = ParallelDF(df, g, 2)
    df = pldf.parallel_run()
    df.to_csv('result.csv', index=False)
