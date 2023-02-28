import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

logging.getLogger().setLevel(logging.DEBUG)

cols_main = [
    'ASSET.ANCESTOR', 'ASSET.ASSETNUM', 'ASSET.ASSETTYPE', 'ASSET.ASSETUID', 'ASSET.CHANGEDATE', 'ASSET.CHILDREN',
    'ASSET.INVCOST', 'ASSET.ISRUNNING', 'ASSET.LOCATION', 'ASSET.SITEID', 'ASSET.SPVB_COSTCENTER',
    'ASSET.SPVB_COSTCENTER_DESCRIPTION', 'ASSET.SPVB_FIXEDASSETNUM', 'ASSET.STATUS_DESCRIPTION', 'ASSET.TOTALCOST',
    'ASSET.DESCRIPTION', 'ASSET.TOTDOWNTIME'
]
cols_status = [
    'ASSETSTATUS.CODE', 'ASSETSTATUS.CODE_DESCRIPTION', 'ASSETSTATUS.CHANGEDATE', 'ASSETSTATUS.DOWNTIME',
    'ASSETSTATUS.ISRUNNING', 'ASSETSTATUS.ASSETSTATUSID', 'ASSETSTATUS.LOCATION', 'ASSETSTATUS.SPVB_ISSUE',
    'ASSETSTATUS.WONUM', 'ASSETSTATUS.SPVB_PA', 'ASSETSTATUS.SPVB_CA', 'ASSETSTATUS.REMARKS'
]
cols_ancestor = ['ASSETANCESTOR.ANCESTOR', 'ASSETANCESTOR.HIERARCHYLEVELS', 'ASSETANCESTOR.ASSETANCESTORID']

cols_main_map = {x: x.removeprefix('ASSET.').lower() for x in cols_main}
cols_status_map = {x: x.removeprefix('ASSETSTATUS.').lower() for x in cols_status}
cols_ancestor_map = {x: x.removeprefix('ASSETANCESTOR.').lower() for x in cols_ancestor}

if __name__ == '__main__':
    path = Path(r"D:\TC Data\_data\prod_Feb24\asset3.xlsx")
    path_dir_out = Path(r"D:\TC Data\SPP API JSONs\SPP\asset")

    # with open(path, encoding='latin-1') as fp:
    excel = pd.ExcelFile(path)

    for sheet in ['Export Worksheet']:  # 'Sheet1'
        logging.info(f"Load sheet: {sheet}")

        assets = []
        df_raw = excel.parse(sheet_name=sheet)

        for ast_uid in tqdm(pd.unique(df_raw['ASSET.ASSETUID'])):
            df = df_raw[df_raw['ASSET.ASSETUID'] == ast_uid]

            asset = {}

            df_a = df.groupby(cols_main, dropna=False) \
                .size() \
                .reset_index() \
                .rename(columns=cols_main_map) \
                .drop([0], axis=1)
            asset = df_a.replace({np.nan: None}).to_dict(orient='records')[0]

            # Process: asset_status
            df_a_status = df.groupby(cols_status, dropna=False) \
                .size() \
                .reset_index() \
                .rename(columns=cols_status_map) \
                .drop([0], axis=1) \
                .dropna(subset=['assetstatusid'], how='all')
            out = df_a_status.replace({np.nan: None}).to_dict(orient='records')
            asset['assetstatus'] = out if out != [] else None

            # Process: asset_ancestor
            df_a_ancestor = df.groupby(cols_ancestor, dropna=False) \
                .size() \
                .reset_index() \
                .rename(columns=cols_ancestor_map) \
                .drop([0], axis=1) \
                .dropna(subset=['assetancestorid'], how='all')
            out = df_a_ancestor.replace({np.nan: None}).to_dict(orient='records')
            asset['assetancestor'] = out if out != [] else None

            assets.append(asset)

        path_out = path_dir_out / f"{path.stem}_{sheet}.json"

        with open(path_out, 'w+', encoding='utf-8') as fp:
            json.dump({'member': assets}, fp, indent=2, ensure_ascii=False)
