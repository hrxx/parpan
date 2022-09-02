import pandas as pd
import sys

def g(df):
    # do some operations
    df['a'] = df['a'].apply(lambda x:x+1)
    return df

if __name__ == '__main__':
    print(sys.argv[1])
    idx = int(sys.argv[1])

    df = pd.read_parquet('_tmp_input/{}.parquet'.format(idx))
    df = g(df)
    df.to_parquet('_tmp_output/{}.parquet'.format(idx))
