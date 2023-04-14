import json
import logging
from io import StringIO
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger
from xlsx2csv import Xlsx2csv

logging.getLogger().setLevel(logging.DEBUG)
cols_main = ['ASSETNUM', 'CHANGEDATE', 'CURRENCYCODE', 'CURRENCYLINECOST',
             'CURRENCYUNITCOST', 'DESCRIPTION', 'ENTERDATE', 'EXCHANGERATE',
             'EXTERNALREFID', 'FINANCIALPERIOD', 'ISSUETYPE',
             'ISSUETYPE_DESCRIPTION', 'ITEMNUM', 'LINECOST', 'LINETYPE', 'LOCATION',
             'POLINENUM', 'PONUM', 'QUANTITY', 'REFWO', 'REMARK', 'SENDERSYSID',
             'SERVRECTRANSID', 'SITEID', 'SPVB_COSTCENTER', 'SPVB_SAPPO',
             'SPVB_SAPRECEIPT', 'SPVB_SAPREMARK', 'STATUS', 'STATUS_DESCRIPTION',
             'TRANSDATE', 'UNITCOST']

cols_main_map = {x: x.lower() for x in cols_main}


def read_excel(path: str, sheet_name: str) -> pd.DataFrame:
    buffer = StringIO()
    Xlsx2csv(path, outputencoding="utf-8").convert(buffer, sheetname=sheet_name)
    buffer.seek(0)

    if sheet_name != "Export Worksheet":
        df_ = pd.read_csv(buffer, header=None)
        df_ = df_.set_axis(cols_main, axis=1, copy=False)
    else:
        df_ = pd.read_csv(buffer, header=0)

    return df_


if __name__ == '__main__':
    path = Path(r"D:\TC_Data\_data\UAT_Feb25\SERV_Feb25.xlsx")
    path_dir_out_serv = Path(r"D:\TC_Data\_data\_post_processed\serv_rec_trans")

    path_dir_out_serv.mkdir(parents=True, exist_ok=True)

    logger.info(f"* Start processing: {path.stem}")

    ids_serv = set()

    for sheet in pd.ExcelFile(path).sheet_names:
        if sheet == 'SQL':
            continue

        logger.info(f"Load sheet: {sheet}")

        df = read_excel(path, sheet_name=sheet)

        logger.info("Extract serv")

        df_serv = df.groupby(cols_main, dropna=False) \
            .size() \
            .reset_index() \
            .rename(columns=cols_main_map) \
            .drop([0], axis=1) \
            .replace({np.nan: None})

        # Filter out duplication
        serv = df_serv.to_dict(orient="records")
        out = []
        for x in serv:
            if x['servrectransid'] not in ids_serv:
                ids_serv.add(x['servrectransid'])
                out.append(x)

        path_serv = path_dir_out_serv / f"{path.stem}_{sheet.replace(' ', '_')}.json"
        with open(path_serv, 'w+', encoding='utf-8') as fp:
            json.dump(out, fp, indent=2, ensure_ascii=False)
