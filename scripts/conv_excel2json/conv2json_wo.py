import logging
from io import StringIO
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger
from xlsx2csv import Xlsx2csv

logging.getLogger().setLevel(logging.DEBUG)

cols_main = [
    'ACTFINISH', 'ACTSTART', 'ASSETNUM', 'DESCRIPTION', 'GLACCOUNT',
    'HASCHILDREN', 'ISTASK', 'JPNUM', 'LOCATION', 'PMDUEDATE', 'PMNUM',
    'REPORTDATE', 'SCHEDFINISH', 'WSCHEDSTART', 'SPVB_OVERHAUL',
    'SPVB_TASK_STATUS', 'STATUS', 'SUPERVISOR', 'TARGCOMPDATE',
    'TARGSTARTDATE', 'WONUM', 'WOPRIORITY', 'WORKTYPE', 'PARENT',
    'SUPERVISOR_1', 'SUPPERVISORNAME'
]
cols_status = ['GLACCOUNT', 'WONUM', 'PARENT_1', 'CHANGEDATE', 'WOSTATUSID', 'STATUS_1']

cols_total = cols_main + cols_status[2:]

cols_main_map = {x: x.lower() for x in cols_main}
cols_status_map = {x: x.removesuffix('_1').lower() for x in cols_status}


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
    path = Path(r"D:\TC_Data\_data\prod_Feb25\WO_02.xlsx")
    path_dir_out_wo = Path(r"D:\TC_Data\_data\_post_processed\work_order")
    path_dir_out_wo_status = Path(r"D:\TC_Data\_data\_post_processed\work_order_status")

    path_dir_out_wo.mkdir(parents=True, exist_ok=True)
    path_dir_out_wo_status.mkdir(parents=True, exist_ok=True)

    for sheet in ['Export Worksheet', 'Sheet1', 'Sheet2', 'Sheet3', 'Sheet4', 'Sheet5']:
        logger.info(f"Load sheet: {sheet}")

        df = read_excel(path, sheet_name=sheet)

        logger.info("Extract WorkOrder")

        df_wo = df.groupby(cols_main, dropna=False) \
            .size() \
            .reset_index() \
            .rename(columns=cols_main_map) \
            .drop([0], axis=1) \
            .replace({np.nan: None})

        path_wo = path_dir_out_wo / f"{path.stem.replace(' ', '_')}_{sheet}.json"
        with open(path_wo, 'w+', encoding='utf-8') as fp:
            df_wo.to_json(fp, orient='records', indent=2, force_ascii=False)

        logger.info("Extract WorkOrder_Status")

        # Process: workorder_status
        df_wo_status = df.groupby(cols_status, dropna=False) \
            .size() \
            .reset_index() \
            .rename(columns=cols_status_map) \
            .drop([0], axis=1) \
            .replace({np.nan: None})

        path_wo_status = path_dir_out_wo_status / f"{path.stem.replace(' ', '_')}_{sheet}.json"
        with open(path_wo_status, 'w+', encoding='utf-8') as fp:
            df_wo_status.to_json(fp, orient='records', indent=2, force_ascii=False)
