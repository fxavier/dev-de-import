import pandas as pd
import numpy as np
import os
import uuid

orgunit = [file for file in os.listdir('orgunits')]
df_orgunit = pd.read_csv(os.path.join('orgunits', orgunit[0]))
datim_dataelements = [file for file in os.listdir('datim_dataelements')]
datim_file = os.path.join('datim_dataelements', datim_dataelements[0])
df_datim = pd.read_csv(datim_file, encoding='ISO-8859-1')
files = os.listdir('initial_files')
count = 0
for file in files:
    count += 1
    df2 = pd.read_csv(os.path.join('initial_files', file))
    df2 = df2.replace(np.nan, '', regex=True)
    # Removing whitespace at the begining of dataframe
    df2.columns = df2.columns.str.lstrip()

    df = pd.merge(df_orgunit, df2, how='outer', left_on='Keycode', right_on='Keycode', indicator=True) 
    df.to_csv('verificacao/files_with_org_units_no_match/'+ file, index=False)
  
    print(f'File {count}:{file}')


    df = pd.read_csv('files_with_orgunits/'+ file)

# 2. Unpivot columns
    columns = df.loc[:,~df.columns.isin(
    ['Keycode', 'period','orgunitlevel2', 'orgunitlevel3', 'dhis_organisationunitid',
     'dhis_organisationunitname', 'datim_organisationunitid', 'datim_organisationunitname'
     ])]

    column_list = [x for x in columns]

    df_unpivoted = df.melt(
       id_vars =  ['Keycode', 'period',	'orgunitlevel2', 'orgunitlevel3', 'dhis_organisationunitid',
                   'dhis_organisationunitname', 'datim_organisationunitid', 'datim_organisationunitname'
                 ], 
        value_vars = column_list,
        var_name = 'dataelement'
    )

    df_unpivoted.to_csv('transformed_files/'+ file, index=False)
    df = pd.read_csv('transformed_files/'+ file)

    df_final = pd.merge(df_datim, df, left_on='dataelement', right_on='dataelement', how='outer', indicator=True)
  
    df_final.to_csv('verificacao/final_files/' + str(uuid.uuid4()) + file, index=False)
