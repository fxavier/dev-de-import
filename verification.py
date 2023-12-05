import os
import uuid
import pandas as pd

ORGUNIT_DIR = 'orgunits'
DATIM_DATAELEMENTS_DIR = 'datim_dataelements'
INITIAL_FILES_DIR = 'initial_files'
VERIFICATION_DIR = 'verificacao'
TRANSFORMED_FILES_DIR = 'transformed_files'

def process_file(file_name, df_orgunit, df_datim):
    # Read the file with encoding
    df2 = pd.read_csv(os.path.join(INITIAL_FILES_DIR, file_name), encoding='ISO-8859-1')
    
    # Clean the data
    df2 = df2.replace(pd.NA, '', regex=True)
    df2.columns = df2.columns.str.lstrip()

    # Ensure 'Keycode' columns are of the same type
    df_orgunit['Keycode'] = df_orgunit['Keycode'].astype(str)
    df2['Keycode'] = df2['Keycode'].astype(str)

    # Merge dataframes
    df_merged = pd.merge(df_orgunit, df2, how='outer', left_on='Keycode', right_on='Keycode', indicator=True)
    df_merged.to_csv(os.path.join(VERIFICATION_DIR, 'files_with_org_units_no_match', file_name), index=False)

    # Unpivot columns
    columns_to_unpivot = df_merged.columns.difference(
        ['Keycode', 'period', 'orgunitlevel2', 'orgunitlevel3', 'dhis_organisationunitid',
         'dhis_organisationunitname', 'datim_organisationunitid', 'datim_organisationunitname']
    )
    df_unpivoted = df_merged.melt(
        id_vars=['Keycode', 'period', 'orgunitlevel2', 'orgunitlevel3', 'dhis_organisationunitid',
                 'dhis_organisationunitname', 'datim_organisationunitid', 'datim_organisationunitname'],
        value_vars=columns_to_unpivot,
        var_name='dataelement'
    )
    df_unpivoted.to_csv(os.path.join(TRANSFORMED_FILES_DIR, file_name), index=False)

    # Final merge
    df_final = pd.merge(df_datim, df_unpivoted, on='dataelement', how='outer', indicator=True)
    df_final.to_csv(os.path.join(VERIFICATION_DIR, 'final_files', str(uuid.uuid4()) + file_name), index=False)

def main():
    # Load initial dataframes
    orgunit_file = os.listdir(ORGUNIT_DIR)[0]
    df_orgunit = pd.read_csv(os.path.join(ORGUNIT_DIR, orgunit_file))
    
    datim_file = os.listdir(DATIM_DATAELEMENTS_DIR)[0]
    df_datim = pd.read_csv(os.path.join(DATIM_DATAELEMENTS_DIR, datim_file), encoding='ISO-8859-1')

    # Process each file
    files = os.listdir(INITIAL_FILES_DIR)
    for idx, file in enumerate(files, start=1):
        if not file.endswith('.csv'):  # skip non-csv files (e.g., .DS_Store)
            continue
        print(f"Processing File {idx}: {file}")
        process_file(file, df_orgunit, df_datim)

if __name__ == "__main__":
    print("Starting the data verification script...")
    main()
