
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
    'ACTUALCOST', 'ACTUALDATE', 'ASSETNUM', 'FROMBIN', 'CURBAL',
    'CURRENCYCODE', 'CURRENCYLINECOST', 'DESCRIPTION', 'EXCHANGERATE',
    'FINANCIALPERIOD', 'FROMSITEID', 'FROMSTORELOC', 'ISSUE', 'ISSUETYPE',
    'ITEMNUM', 'LINECOST', 'LINETYPE', 'LINETYPE_DESCRIPTION',
    'MATRECTRANSID', 'MRNUM', 'POLINENUM', 'PONUM', 'POSITEID',
    'QTYOVERRECEIVED', 'QUANTITY', 'RECEIVEDUNIT', 'REFWO', 'SHIPMENTNUM',
    'SITEID', 'SPVB_DND', 'SPVB_SAPPO', 'SPVB_SAPRECEIPT', 'SPVB_SAPREMARK',
    'TOBIN', 'TOSTORELOC', 'TOTALCURBAL', 'TRANSDATE', 'UNITCOST', 'INVUSEID', 'INVUSELINEID'
]
cols_invu = [
    'DESCRIPTION_1', 'CURRENCYCODE_1', 'INVUSENUM', 'INVOWNER',
    'INVUSEID_1', 'FROMSTORELOC_1', 'STATUS_DESCRIPTION', 'INVU_CHANGEDATE',
    'USETYPE_DESCRIPTION', 'INVU_EXCHANGEDATE', 'RECEIPTS_DESCRIPTION',
    'RECEIPTS', 'SPVB_INTERNAL', 'USETYPE', 'STATUS'
]
cols_invul = [
    'INVUSELINEID_1', 'UNITCOST_1', 'SPVB_EXTREASONCODE', 'SPVB_EXTREASONCODE_DESCRIPTION',
    'SPVB_MUSTRETURN', 'SPVB_RETURNFROMISSUE', 'SPVB_MUSTRETURN_ORG',
    'RETURNEDQTY', 'REMARK', 'LINETYPE_1', 'DESCRIPTION_2', 'TOSITEID',
    'INVUSENUM_1', 'INVL_ACTUALDATE', 'RECEIVEDQTY', 'ASSETNUM_1',
    'COSTCENTER', 'FROMSTORELOC_2', 'REFWO_1', 'LINECOST_1', 'QUANTITY_1',
    'COSTCENTER_DESCRIPTION', 'INVUSELINENUM', 'ITEMNUM_1', 'ITEMSETID',
    'LOCATION', 'USETYPE_1', 'ENTERBY', 'SPVB_WONUMREF', 'SPVB_REASON'
]
cols_total = cols_main + cols_invu + cols_invul

cols_main_map = {x: x.lower() for x in cols_main}
cols_invu_map = {x: x.removeprefix('INVU_').removesuffix('_1').removesuffix('_2').lower() for x in cols_invu}
cols_invul_map = {x: x.removeprefix('INVL_').removesuffix('_1').removesuffix('_2').lower() for x in cols_invul}


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
    path = Path(r"D:\TC_Data\_data\UAT_Mar28\MATR_Mar28.xlsx")
    path_dir_out_matr = Path(r"D:\TC_Data\_data\_post_processed\material_receipt_trans")
    path_dir_out_invu = Path(r"D:\TC_Data\_data\_post_processed\inventory_use")
    path_dir_out_invul = Path(r"D:\TC_Data\_data\_post_processed\inventory_use_line")

    path_dir_out_matr.mkdir(parents=True, exist_ok=True)
    path_dir_out_invu.mkdir(parents=True, exist_ok=True)
    path_dir_out_invul.mkdir(parents=True, exist_ok=True)

    logger.info(f"* Start processing: {path.stem}")

    ids_matr, ids_invu, ids_invul = set(), set(), set()

    for sheet in pd.ExcelFile(path).sheet_names:
        if sheet == 'SQL':
            continue

        logger.info(f"Load sheet: {sheet}")

        df = read_excel(path, sheet_name=sheet)

        logger.info("Extract MATR")

        df_matr = df.groupby(cols_main, dropna=False) \
            .size() \
            .reset_index() \
            .rename(columns=cols_main_map) \
            .replace({np.nan: None})

        # Filter out duplication
        matr = df_matr.to_dict(orient="records")
        out = []
        for x in matr:
            if x['matrectransid'] not in ids_matr:
                ids_matr.add(x['matrectransid'])
                out.append(x)

        path_matr = path_dir_out_matr / f"{path.stem}_{sheet.replace(' ', '_')}.json"
        with open(path_matr, 'w+', encoding='utf-8') as fp:
            json.dump(out, fp, indent=2, ensure_ascii=False)

        logger.info("Extract INVU_MATR")

        # Process: inventory_use
        df_invu = df.groupby(cols_invu, dropna=False) \
            .size() \
            .reset_index() \
            .rename(columns=cols_invu_map) \
            .drop([0], axis=1)

        df_invu = df_invu[~df_invu['invuseid'].isna()] \
            .replace({np.nan: None})

        # Filter out duplication
        invu = df_invu.to_dict(orient="records")
        out = []
        for x in invu:
            if x['invuseid'] not in ids_invu:
                ids_invu.add(x['invuseid'])

                x['from'] = 'MATR'

                out.append(x)

        path_invu = path_dir_out_invu / f"{path.stem}_{sheet.replace(' ', '_')}.json"
        with open(path_invu, 'w+', encoding='utf-8') as fp:
            json.dump(out, fp, indent=2, ensure_ascii=False)

        logger.info("Extract INVUL_MATR")

        # Process: inventory_use_line
        df_invul = df.groupby(cols_invul, dropna=False) \
            .size() \
            .reset_index() \
            .rename(columns=cols_invul_map) \
            .drop([0], axis=1)

        df_invul = df_invul[~df_invul['invuselineid'].isna()] \
            .replace({np.nan: None})

        # Filter out duplication
        invul = df_invul.to_dict(orient="records")
        out = []
        for x in invul:
            if x['invuselineid'] not in ids_invu:
                ids_invu.add(x['invuselineid'])

                x['from'] = 'MATR'

                out.append(x)

        path_invul = path_dir_out_invul / f"{path.stem}_{sheet.replace(' ', '_')}.json"
        with open(path_invul, 'w+', encoding='utf-8') as fp:
            json.dump(out, fp, indent=2, ensure_ascii=False)
