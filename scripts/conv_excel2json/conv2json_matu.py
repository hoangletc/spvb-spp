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
    'ACTUALCOST', 'ASSETNUM', 'BINNUM', 'CURBAL', 'CURRENCYCODE',
    'DESCRIPTION', 'EXCHANGERATE', 'INVUSEID', 'INVUSELINEID', 'ISSUETYPE',
    'ISSUEUNIT', 'ITEMNUM', 'LINECOST', 'LINETYPE', 'LOCATION',
    'MATUSETRANSID', 'MRNUM', 'ORGID', 'PONUM', 'QTYREQUESTED', 'QUANTITY',
    'REFWO', 'STORELOC', 'TOSITEID', 'MATUSETRANS_TRANSDATE', 'UNITCOST',
    'MATUSETRANS_ACTUALDATE'
]
cols_invu = [
    'DESCRIPTION_1', 'CURRENCYCODE_1',
    'INVUSENUM', 'INVOWNER', 'INVUSEID_1', 'FROMSTORELOC',
    'STATUS_DESCRIPTION', 'CHANGEDATE', 'USETYPE_DESCRIPTION',
    'EXCHANGEDATE', 'RECEIPTS_DESCRIPTION', 'RECEIPTS', 'SPVB_INTERNAL',
    'USETYPE', 'STATUS'
]
cols_invul = [
    'INVUSELINEID_1', 'UNITCOST_1',
    'SPVB_EXTREASONCODE', 'SPVB_EXTREASONCODE_DESCRIPTION',
    'SPVB_MUSTRETURN', 'SPVB_RETURNFROMISSUE', 'SPVB_MUSTRETURN_ORG',
    'RETURNEDQTY', 'REMARK', 'LINETYPE_1', 'DESCRIPTION_2', 'TOSITEID_1',
    'INVUSENUM_1', 'ACTUALDATE', 'RECEIVEDQTY', 'ASSETNUM_1', 'COSTCENTER',
    'FROMSTORELOC_1', 'REFWO_1', 'LINECOST_1', 'QUANTITY_1',
    'COSTCENTER_DESCRIPTION', 'INVUSELINENUM', 'ITEMNUM_1', 'ITEMSETID',
    'LOCATION_1', 'USETYPE_1', 'ENTERBY', 'SPVB_WONUMREF', 'SPVB_REASON'
]

cols_total = cols_main + cols_invu + cols_invul

cols_main_map = {x: x.removeprefix('MATUSETRANS_').lower() for x in cols_main}
cols_invu_map = {x: x.removeprefix('INVUSE_').removesuffix('_1').removesuffix('_2').lower() for x in cols_invu}
cols_invul_map = {x: x.removeprefix('INVUSELINE_').removesuffix('_1').removesuffix('_2').lower() for x in cols_invul}


def read_excel(path: str, sheet_name: str) -> pd.DataFrame:
    buffer = StringIO()
    Xlsx2csv(path, outputencoding="utf-8").convert(buffer, sheetname=sheet_name)
    buffer.seek(0)

    if sheet_name != "Export Worksheet":
        df_ = pd.read_csv(buffer, header=None)
        df_ = df_.set_axis(cols_total, axis=1, copy=False)
    else:
        df_ = pd.read_csv(buffer, header=0)

    return df_


if __name__ == '__main__':
    path = Path(r"D:\TC_Data\_data\UAT_Apr11\MATU_Xuat lai ngay 11Apr2023.xlsx")
    path_dir_out_matu = Path(r"D:\TC_Data\_data\_post_processed\material_use_trans")
    path_dir_out_invu = Path(r"D:\TC_Data\_data\_post_processed\inventory_use")
    path_dir_out_invul = Path(r"D:\TC_Data\_data\_post_processed\inventory_use_line")

    path_dir_out_matu.mkdir(parents=True, exist_ok=True)
    path_dir_out_invu.mkdir(parents=True, exist_ok=True)
    path_dir_out_invul.mkdir(parents=True, exist_ok=True)

    logger.info(f"* Start processing: {path.stem}")

    ids_matu, ids_invu, ids_invul = set(), set(), set()

    for sheet in pd.ExcelFile(path).sheet_names:
        if sheet == 'SQL':
            continue

        logger.info(f"Load sheet: {sheet}")

        df = read_excel(path, sheet_name=sheet)

        logger.info("Extract MATU")

        df_matu = df.groupby(cols_main, dropna=False) \
            .size() \
            .reset_index() \
            .rename(columns=cols_main_map) \
            .drop([0], axis=1) \
            .replace({np.nan: None})

        # Filter out duplication
        matu = df_matu.to_dict(orient="records")
        out = []
        for x in matu:
            if x['matusetransid'] not in ids_matu:
                ids_matu.add(x['matusetransid'])
                out.append(x)

        path_matu = path_dir_out_matu / f"{path.stem}_{sheet}.json".replace(' ', '_')
        with open(path_matu, 'w+', encoding='utf-8') as fp:
            json.dump(out, fp, indent=2, ensure_ascii=False)

        logger.info("Extract INVU_MATU")

        # Process: inventory_use
        df_invu = df.groupby(cols_invu, dropna=False) \
            .size() \
            .reset_index() \
            .rename(columns=cols_invu_map) \
            .drop([0], axis=1) \
            .replace({np.nan: None})

        # Filter out duplication
        invu = df_invu.to_dict(orient="records")
        out = []
        for x in invu:
            if x['invuseid'] not in ids_invu:
                ids_invu.add(x['invuseid'])

                x['from'] = 'MATU'

                out.append(x)

        path_invu = path_dir_out_invu / f"{path.stem}_{sheet}.json".replace(' ', '_')
        with open(path_invu, 'w+', encoding='utf-8') as fp:
            json.dump(out, fp, indent=2, ensure_ascii=False)

        logger.info("Extract INVUL_MATU")

        # Process: inventory_use_line
        df_invul = df.groupby(cols_invul, dropna=False) \
            .size() \
            .reset_index() \
            .rename(columns=cols_invul_map) \
            .drop([0], axis=1) \
            .replace({np.nan: None})

        # Filter out duplication
        invul = df_invul.to_dict(orient="records")
        out = []
        for x in invul:
            if x['invuselineid'] not in ids_invu:
                ids_invul.add(x['invuselineid'])

                x['from'] = 'MATU'

                out.append(x)

        path_invul = path_dir_out_invul / f"{path.stem}_{sheet}.json".replace(' ', '_')
        with open(path_invul, 'w+', encoding='utf-8') as fp:
            json.dump(out, fp, indent=2, ensure_ascii=False)
