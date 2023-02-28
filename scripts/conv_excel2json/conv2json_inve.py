import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd

logging.getLogger().setLevel(logging.DEBUG)

if __name__ == '__main__':
    path = Path(r"D:\TC_Data\_data\prod_Feb25\INVE.xlsx")
    path_dir_out = Path(r"D:\TC_Data\_data\_pre_processed\inventory")

    excel = pd.ExcelFile(path)

    for sheet in ['Sheet1', 'Export Worksheet']:
        logging.info(f"Load sheet: {sheet}")

        df = excel.parse(sheet_name=sheet)

        logging.info(f"Sheet: {sheet} ==> Qty.: {len(df)}")

        columns_map = {x: x.lower() for x in df.columns}
        out = df.rename(columns=columns_map) \
            .replace({np.nan: None}) \
            .to_dict(orient='records')

        path_out = path_dir_out / f"{sheet.replace(' ', '_')}.json"

        with open(path_out, 'w+', encoding='utf-8') as fp:
            json.dump({'member': out}, fp, indent=2, ensure_ascii=False)
