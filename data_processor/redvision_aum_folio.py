import pandas as pd

from postgres import perform_upsert
from utils import validate_pan


def process_pans(pan_column):
    return pan_column.apply(lambda x: validate_pan(x))


def process_folio_with_aum():
    # To Do Set AUM to 0.0
    df = pd.read_excel("data/redvision/redvision aum folio.xlsx")
    df['AUM'] = pd.to_numeric(df['AUM'].str.replace(',', ''), errors='coerce')
    # df = df.dropna(subset=['AUM'])
    grouped_df = df.groupby('Folio No.', as_index=False)['AUM'].sum()
    updated_df = grouped_df[
        ['Folio No.', 'AUM']].rename(columns={'Folio No.': 'folio_number', 'AUM': 'folio_aum'})
    table_name = 'client_details.folio_details'
    values = list(updated_df.itertuples(index=False, name=None))
    perform_upsert(table_name, updated_df.columns, values, 'folio_number')
    print("Updated Folio Number and AUM from Redvision AUM excel")
