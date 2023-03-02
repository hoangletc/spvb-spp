import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger
from tqdm import tqdm

logging.getLogger().setLevel(logging.DEBUG)

cols_main = [
    'ANCESTOR', 'ASSETNUM', 'ASSETTYPE', 'ASSETUID', 'ASSET_CHANGEDATE',
    'CHILDREN', 'INVCOST', 'ISRUNNING', 'LOCATION', 'SITEID',
    'SPVB_COSTCENTER', 'SPVB_COSTCENTER_DESCRIPTION', 'SPVB_FIXEDASSETNUM',
    'STATUS_DESCRIPTION', 'TOTALCOST', 'DESCRIPTION', 'TOTDOWNTIME'
]
cols_status = [
    'CODE', 'CODE_DESCRIPTION', 'ASSETSTATUS_CHANGEDATE', 'DOWNTIME', 'ISRUNNING_1',
    'ASSETSTATUSID', 'LOCATION_1', 'SPVB_ISSUE', 'SPVB_T', 'SPVB_L',
    'SPVB_I', 'SPVB_C', 'WONUM', 'SPVB_PA', 'SPVB_CA', 'REMARKS'
]
cols_ancestor = ['ANCESTOR_1', 'HIERARCHYLEVELS', 'ASSETANCESTORID']

cols_main_map = {x: x.removeprefix('ASSET_').removesuffix('_1').lower() for x in cols_main}
cols_status_map = {x: x.removeprefix('ASSETSTATUS_').removesuffix('_1').lower() for x in cols_status}
cols_ancestor_map = {x: x.removesuffix('_1').lower() for x in cols_ancestor}

if __name__ == '__main__':
    path = Path(r"D:\TC_Data\_data\prod_Mar1\asset.xlsx")
    path_dir_out = Path(r"D:\TC_Data\_data\_pre_processed\asset")

    # with open(path, encoding='latin-1') as fp:
    excel = pd.ExcelFile(path)

    for sheet in ['Export Worksheet', 'Sheet1', 'Sheet2']:  #
        logger.info(f"Load sheet: {sheet}")

        assets = []
        df_raw = excel.parse(sheet_name=sheet)

        for ast_uid in tqdm(pd.unique(df_raw['ASSETUID'])):
            df = df_raw[df_raw['ASSETUID'] == ast_uid]

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

        path_out = path_dir_out / f"{sheet}.json"

        with open(path_out, 'w+', encoding='utf-8') as fp:
            json.dump({'member': assets}, fp, indent=2, ensure_ascii=False)
