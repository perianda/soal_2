import json
import pandas as pd
import numpy as np

def open_file(file):
    file = open(file)
    data = json.load(file)
    df = pd.DataFrame(data)
    df['id'] = pd.RangeIndex(0, 0 + len(df)) + 1
    return df

def cleaning_data(df):
    # KOMODITAS
    df["komoditas_"] = df["komoditas"].str.replace(', ', ' ').str.replace(',', ' ').str.replace(' ', ', ').str.split(
        ', ')
    komoditas_df = df.explode('komoditas_')
    komoditas_df['komoditas_'] = komoditas_df['komoditas_'].str.replace(' ', '')

    komoditas_df['komoditas_id'] = komoditas_df.groupby(level=0).cumcount()
    komoditas_df = komoditas_df.reset_index(drop=True)

    # BERAT
    df["berat_"] = df["berat"].str.replace('kurang dari ', '').str.replace('rata2', '').str.replace('rata',
                                                                                                    '').str.split(
        'kg ').str.join(';').str.split('kg, ').str.join(';').str.split('kg,').str.join(';').str.split('kg').str.join(
        ';').str.split(' ').str.join(':').str.replace('1/2', '1.5').str.replace('1,5', '1.5')
    df["berat_per_komoditas"] = df["berat_"].str.split(';')
    berat_df = df.explode('berat_per_komoditas')

    berat_df['komoditas_id'] = berat_df.groupby(level=0).cumcount()
    berat_df = berat_df.reset_index(drop=True)

    berat_df["berat_per_komoditas"] = berat_df["berat_per_komoditas"].str.split('-').str[0]
    berat_df['komoditas_'] = berat_df['berat_per_komoditas'].str.split(':').str[0]
    berat_df['berat_'] = berat_df['berat_per_komoditas'].str.split(':').str[1].str.replace(' ', '').str.replace('g', '')

    berat_df['berat_by_index'] = berat_df['berat_per_komoditas'].str.split(';').str[0].str.replace(':', '').str.replace(
        'g', '')

    # MERGE
    ## Get Berat by Komoditas
    new_df = komoditas_df.merge(berat_df, left_on=['id', 'komoditas_'], right_on=['id', 'komoditas_'], how='left')
    new_df = new_df[["id", "komoditas_id_x", "komoditas_", "berat_"]]
    new_df.columns = ['id', 'komoditas_id', 'komoditas', 'berat']

    ## Get Berat by Index
    new_df = new_df.merge(berat_df, left_on=['id', 'komoditas_id'], right_on=['id', 'komoditas_id'], how='left')
    new_df = new_df[['id', 'komoditas_id', 'komoditas_x', 'berat_x', 'berat_by_index']]
    new_df.columns = ['id', 'komoditas_id', 'komoditas', 'berat_by_name', 'berat_by_id']

    ## Get Berat when Berat is only single number
    berat_id_0_df = berat_df.loc[berat_df['komoditas_id'] == 0]

    new_df = new_df.merge(berat_id_0_df, left_on=['id'], right_on=['id'], how='left')
    new_df = new_df[['id', 'komoditas_id_x', 'komoditas_x', 'berat_by_name', 'berat_by_id', 'berat_by_index']]
    new_df.columns = ['id', 'komoditas_id', 'komoditas', 'berat_by_name', 'berat_by_id', 'berat_by_index_0']

    ## Cleaning Berat
    new_df['berat'] = new_df.berat_by_name.replace(r'^\s*$', np.nan, regex=True).fillna(
        new_df.berat_by_id.replace(r'^\s*$', np.nan, regex=True)).fillna(new_df.berat_by_index_0)
    new_df['berat'] = new_df['berat'].str.extract('([0-9][,.]*[0-9]*)')
    new_df['berat'] = new_df['berat'].astype(float)

    return new_df

def get_summary(df):
    # Summarized data
    summary_df = df.groupby(['komoditas'])['berat'].agg('sum').reset_index()
    summary_df = summary_df.sort_values('berat', ascending=False)

    return summary_df

if __name__ == '__main__':
    file = open_file("soal-2.json")
    df = cleaning_data(file)
    summary = get_summary(df)

    print(summary)



