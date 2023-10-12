import pandas as pd
import numpy as np
import os
import uuid
import logging

# Set up logging
logging.basicConfig(filename="data_processing.log", level=logging.INFO,
                    format='%(asctime)s - %(levelname)s: %(message)s')


def remove_old_files(dir_list):
    for directory in dir_list:
        if os.path.exists(directory):
            for filename in os.listdir(directory):
                os.remove(os.path.join(directory, filename))
        else:
            logging.warning(f"Directory {directory} not found.")


def merge_orgunits_and_unpivot_columns(df_orgunit, df_datim, files):
    file_count = 0
    for file in files:
        try:
            file_count += 1
            df2 = pd.read_csv(os.path.join('initial_files', file), encoding='ISO-8859-1')
            df2 = df2.replace(np.nan, '', regex=True)
            df2.columns = df2.columns.str.lstrip()

            df_orgunit['Keycode'] = df_orgunit['Keycode'].astype(str)
            df2['Keycode'] = df2['Keycode'].astype(str)

            df = pd.merge(df_orgunit, df2, on='Keycode', how='inner')
            df.to_csv('files_with_orgunits/' + file, index=False)

            logging.info(f'File {file_count}: {file}')

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

            df_final = pd.merge(df_datim, df_unpivoted, on='dataelement', how='inner')
            df_final.to_csv('final_files/' + str(uuid.uuid4()) + file, index=False)

        except Exception as e:
            logging.error(f"Error processing file {file}: {e}")

    return file_count


def concat_final_files(final_files):
    df_list = []
    for final_file in final_files:
        try:
            df = pd.read_csv(os.path.join('final_files', final_file))
            df_list.append(df)
        except Exception as e:
            logging.error(f"Error reading file {final_file}: {e}")
    final_df = pd.concat(df_list).drop_duplicates().reset_index(drop=True)
    final_df.to_csv('merged/final_file.csv', index=False)
    return final_df


def prepare_final_file(df):
    df = df[['dataelementuid', 'period', 'datim_organisationunitid', 'categoryoptioncombouid', 'value']]
    df.columns = ['dataelement', 'period', 'orgunit', 'categoryoptioncombo', 'value']
    df = df.assign(attributeoptioncombo=['70212'] * df.shape[0])
    return df.iloc[:, [0, 1, 2, 3, 5, 4]]


# def remove_blank_values(df):
#     return df.dropna()


def create_and_save_pivot_table(df, output_file, index_col, columns_col):
    try:
        pivot_table = pd.pivot_table(
            df,
            index=index_col,
            columns=columns_col,
            values='value',
            aggfunc='sum'
        )
        pivot_table.to_csv(output_file)
        logging.info(f"Pivot table saved to {output_file}")
    except Exception as e:
        logging.error(f"Error creating pivot table: {e}")


def main():
    dir_list = [
        'files_with_orgunits',
        'transformed_files',
        'final_files',
        'merged',
        'verificacao/final_files',
        'verificacao/files_with_org_units_no_match'
    ]

    logging.info('Starting the data processing script...')
    logging.info('Removing old files...')
    remove_old_files(dir_list)

    logging.info('Merging orgunits and unpivoting columns...')
    if not os.path.exists('orgunits') or not os.path.exists('datim_dataelements'):
        logging.error("Required directories 'orgunits' or 'datim_dataelements' not found.")
        return

    orgunit_files = [file for file in os.listdir('orgunits')]
    datim_dataelements_files = [file for file in os.listdir('datim_dataelements')]

    if not orgunit_files or not datim_dataelements_files:
        logging.error("Required files in 'orgunits' or 'datim_dataelements' not found.")
        return

    df_orgunit = pd.read_csv(os.path.join('orgunits', orgunit_files[0]))
    df_datim = pd.read_csv(os.path.join('datim_dataelements', datim_dataelements_files[0]), encoding='ISO-8859-1')

    initial_files = os.listdir('initial_files')

    file_count = merge_orgunits_and_unpivot_columns(df_orgunit, df_datim, initial_files)

    df = concat_final_files(os.listdir('final_files'))

    df = prepare_final_file(df)

   # create_and_save_pivot_table(df, 'merged/pivot_table.csv', 'datim_dataelement', 'orgunitlevel2')
   # create_and_save_pivot_table(df, 'merged/pivot_table_hf.csv', 'dhis_organisationunitname', 'datim_dataelement')

    df.to_csv('merged/file_to_import.csv', index=False)
    logging.info(f'Process completed! Found {file_count} initial files. The file to be imported to dev-de is file_to_import.csv')

if __name__ == "__main__":
    main()
