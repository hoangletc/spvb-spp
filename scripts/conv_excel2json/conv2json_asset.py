
import json
import logging
from io import StringIO
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger
from xlsx2csv import Xlsx2csv

logging.getLogger().setLevel(logging.DEBUG)

cols_main = [
    'ANCESTOR', 'ASSETNUM', 'ASSETTYPE', 'ASSETUID', 'ASSET_CHANGEDATE',
    'CHILDREN', 'INVCOST', 'ISRUNNING', 'LOCATION', 'SITEID',
    'SPVB_COSTCENTER', 'SPVB_COSTCENTER_DESCRIPTION', 'SPVB_FIXEDASSETNUM',
    'STATUS_DESCRIPTION', 'TOTALCOST', 'DESCRIPTION', 'TOTDOWNTIME'
]
cols_status = [
    'ASSETUID', 'ASSETNUM', 'CODE', 'CODE_DESCRIPTION', 'ASSETSTATUS_CHANGEDATE', 'DOWNTIME',
    'ISRUNNING_1', 'ASSETSTATUSID', 'LOCATION_1', 'SPVB_ISSUE', 'SPVB_T', 'SPVB_L',
    'SPVB_I', 'SPVB_C', 'WONUM', 'SPVB_PA', 'SPVB_CA', 'REMARKS', 'SPVB_RELATEDDOWNTIME'
]
cols_ancestor = ['ASSETUID', 'ANCESTOR_1', 'HIERARCHYLEVELS', 'ASSETANCESTORID']


cols_main_map = {x: x.removesuffix('_1').lower() for x in cols_main}
cols_status_map = {x: x.removesuffix('_1').lower() for x in cols_status}
cols_ancestor_map = {x: x.removesuffix('_1').lower() for x in cols_ancestor}


def read_excel(path: str, sheet_name: str) -> pd.DataFrame:
    buffer = StringIO()
    Xlsx2csv(path, outputencoding="utf-8").convert(buffer, sheetname=sheet_name)
    buffer.seek(0)
    df_ = pd.read_csv(buffer, header=0)

    return df_


if __name__ == '__main__':
    path = Path(r"D:\TC_Data\_data\prod_Mar22\ASSET 22MAR2023.xlsx")
    path_dir_out_ast = Path(r"D:\TC_Data\_data\_pre_processed\asset")
    path_dir_out_ast_status = Path(r"D:\TC_Data\_data\_pre_processed\asset_status")
    path_dir_out_ast_ancestor = Path(r"D:\TC_Data\_data\_pre_processed\asset_ancestor")

    path_dir_out_ast.mkdir(parents=True, exist_ok=True)
    path_dir_out_ast_status.mkdir(parents=True, exist_ok=True)
    path_dir_out_ast_ancestor.mkdir(parents=True, exist_ok=True)

    ids_ast, ids_ast_st, ids_ast_anc = set(), set(), set()

    for sheet in pd.ExcelFile(path).sheet_names:
        if sheet == 'SQL':
            continue

        logger.info(f"Load sheet: {sheet}")

        df = read_excel(path, sheet_name=sheet)
        df['ANCESTOR'] = df['ANCESTOR'].fillna(0).astype(np.int64).astype(str).replace('0', '')
        df['ANCESTOR_1'] = df['ANCESTOR_1'].fillna(0).astype(np.int64).astype(str).replace('0', None)
        df['ASSETSTATUSID'] = df['ASSETSTATUSID'].fillna(0).astype(np.int64).astype(str).replace('0', None)
        df['ASSETANCESTORID'] = df['ASSETANCESTORID'].fillna(0).astype(np.int64)
        df['HIERARCHYLEVELS'] = df['HIERARCHYLEVELS'].fillna(0).astype(np.int64)
        # df['SPVB_RELATEDDOWNTIME'] = df['SPVB_RELATEDDOWNTIME'].fillna(-123).astype(np.int64).replace(-123, None)

        logger.info("Extract Asset")

        df_ast = df.groupby(cols_main, dropna=False) \
            .size() \
            .reset_index() \
            .rename(columns=cols_main_map) \
            .drop([0], axis=1) \
            .replace({np.nan: None})

        ast_ = df_ast.to_dict('records')
        ast = []
        for x in ast_:
            if x['assetuid'] not in ids_ast:
                ids_ast.add(x['assetuid'])
                ast.append(x)

        path_ast = path_dir_out_ast / f"{path.stem}_{sheet.replace(' ', '_')}.json"
        with open(path_ast, 'w+', encoding='utf-8') as fp:
            json.dump(ast, fp, indent=2, ensure_ascii=False)

        logger.info("Extract Asset_Status")

        # Process: workorder_status
        df_ast_status = df.groupby(cols_status, dropna=False) \
            .size() \
            .reset_index() \
            .rename(columns=cols_status_map) \
            .drop([0], axis=1) \
            .replace({np.nan: None})

        ast_st_ = df_ast_status.to_dict('records')
        ast_st = []
        for x in ast_st_:
            if x['assetstatusid'] not in ids_ast_st:
                ids_ast_st.add(x['assetstatusid'])
                ast_st.append(x)

        path_ast_status = path_dir_out_ast_status / f"{path.stem}_{sheet.replace(' ', '_')}.json"
        with open(path_ast_status, 'w+', encoding='utf-8') as fp:
            json.dump(ast_st, fp, indent=2, ensure_ascii=False)

        logger.info("Extract Asset_Ancestor")

        # Process: workorder_status
        df_ast_anc = df.groupby(cols_ancestor, dropna=False) \
            .size() \
            .reset_index() \
            .rename(columns=cols_ancestor_map) \
            .drop([0], axis=1) \
            .replace({np.nan: None})

        ast_anc_ = df_ast_anc.to_dict('records')
        ast_anc = []
        for x in ast_anc_:
            if x['assetancestorid'] not in ids_ast_anc:
                ids_ast_anc.add(x['assetancestorid'])
                ast_anc.append(x)

        path_ast_anc = path_dir_out_ast_ancestor / f"{path.stem}_{sheet.replace(' ', '_')}.json"
        with open(path_ast_anc, 'w+', encoding='utf-8') as fp:
            json.dump(ast_anc, fp, indent=2, ensure_ascii=False)
