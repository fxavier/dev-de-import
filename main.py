import pandas as pd
import numpy as np
import os
import uuid

def remove_old_files(dir_list):
    for directory in dir_list:
        for filename in os.listdir(directory):
            os.remove(os.path.join(directory, filename))

def merge_orgunits_and_unpivot_columns(df_orgunit, df_datim, files):
    file_count = 0
    for file in files:
        file_count += 1
        df2 = pd.read_csv(os.path.join('initial_files', file), encoding='ISO-8859-1')
        df2 = df2.replace(np.nan, '', regex=True)
        df2.columns = df2.columns.str.lstrip()

        df = pd.merge(df_orgunit, df2, on='Keycode', how='inner')
        df.to_csv('files_with_orgunits/' + file, index=False)
      
        print(f'File {file_count}: {file}')

        df = pd.read_csv('files_with_orgunits/' + file)
        columns_to_exclude = [
            'Keycode', 'period', 'orgunitlevel2', 'orgunitlevel3', 'dhis_organisationunitid',
            'dhis_organisationunitname', 'datim_organisationunitid', 'datim_organisationunitname'
        ]
        columns = df.loc[:, ~df.columns.isin(columns_to_exclude)]

        column_list = [x for x in columns]

        df_unpivoted = df.melt(
            id_vars=['Keycode', 'period', 'orgunitlevel2', 'orgunitlevel3', 'dhis_organisationunitid',
                     'dhis_organisationunitname', 'datim_organisationunitid', 'datim_organisationunitname'],
            value_vars=column_list,
            var_name='dataelement'
        )

        df_unpivoted.to_csv('transformed_files/' + file, index=False)

        df = pd.read_csv('transformed_files/' + file)
        df_final = pd.merge(df_datim, df, on='dataelement', how='inner')
        df_final.to_csv('final_files/' + str(uuid.uuid4()) + file, index=False)
    
    return file_count

def concat_final_files(final_files):
    df_list = []
    for final_file in final_files:
        df = pd.read_csv(os.path.join('final_files', final_file))
        df_list.append(df)
    final_file = pd.concat(df_list).drop_duplicates().reset_index(drop=True)
    final_file.to_csv('merged/final_file.csv', index=False)

    return pd.read_csv('merged/final_file.csv')

def prepare_final_file(df):
    df = df[['dataelementuid', 'period', 'datim_organisationunitid', 'categoryoptioncombouid', 'value']]
    df.columns = ['dataelement', 'period', 'orgunit', 'categoryoptioncombo', 'value']
    values = ['70212'] * df.shape[0]
    
    df1 = df.assign(attributeoptioncombo=values)
    df = df1.iloc[:, [0, 1, 2, 3, 5, 4]]

    return df

def remove_blank_values(df):
    df = df.dropna()
    return df

def create_and_save_pivot_table(df, output_file):
    # Create a pivot table
    df = pd.read_csv('merged/final_file.csv')
    pivot_table = pd.pivot_table(
        df,
        index='datim_dataelement',
        columns='orgunitlevel2',
        values='value',
        aggfunc='sum')

    # Save the pivot table to a CSV file
    pivot_table.to_csv(output_file)
    
def create_and_save_pivot_table_with_hf(df, output_file):
    # Create a pivot table
    df = pd.read_csv('merged/final_file.csv')
    pivot_table = pd.pivot_table(
        df,
        index='dhis_organisationunitname',
        columns='datim_dataelement',
        values='value',
        aggfunc='sum')

    # Save the pivot table to a CSV file
    pivot_table.to_csv(output_file)


def main():
    dir_list = [
        'files_with_orgunits',
        'transformed_files',
        'final_files',
        'merged',
        'verificacao/final_files',
        'verificacao/files_with_org_units_no_match'
    ]

    print('Removing old files...')
    remove_old_files(dir_list)

    print('Merging orgunits and unpivoting columns...')
    orgunit_files = [file for file in os.listdir('orgunits')]
    df_orgunit = pd.read_csv(os.path.join('orgunits', orgunit_files[0]))
    datim_dataelements_files = [file for file in os.listdir('datim_dataelements')]
    datim_file = os.path.join('datim_dataelements', datim_dataelements_files[0])
    df_datim = pd.read_csv(datim_file, encoding='ISO-8859-1')
    initial_files = os.listdir('initial_files')
    
    file_count = merge_orgunits_and_unpivot_columns(df_orgunit, df_datim, initial_files)
    
    final_files = os.listdir('final_files')
    df = concat_final_files(final_files)
    
    df = prepare_final_file(df)
    
    # Remove blank values
    df = remove_blank_values(df)
    
    create_and_save_pivot_table(df, 'merged/pivot_table.csv')
    create_and_save_pivot_table_with_hf(df, 'merged/pivot_table_hf.csv')
    
    df.to_csv('merged/file_to_import.csv', index=False)
    print(f'Process completed! Found {file_count} initial files! The file to be imported to dev-de is file_to_import.csv')

if __name__ == "__main__":
    main()
