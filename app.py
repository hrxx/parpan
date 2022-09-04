import pandas as pd
import argparse

def g(df):
    # do some operations
    df['a'] = df['a'].apply(lambda x:x+1)
    return df

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-if', '--input_file', type=str, required=True, default='_tmp_input/0.parquet')
    parser.add_argument('-of', '--output_file', type=str, required=True, default='_tmp_output/0.parquet')
    parser.add_argument('-oa', '--other_augment', type=str, required=False)
    args = parser.parse_args()
    input_file = str(args.input_file)
    output_file = str(args.output_file)

    df = pd.read_parquet(input_file)
    df = g(df)
    df.to_parquet(output_file)
