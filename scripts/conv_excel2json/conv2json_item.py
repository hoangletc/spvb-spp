import json
from pathlib import Path

import numpy as np
import pandas as pd

if __name__ == '__main__':
    path = Path(r"D:\TC Data\_data\prod_Feb24\item.xlsx")
    path_dir_out = Path(r"D:\TC Data\SPP API JSONs\SPP\item")
    sheet_name = 'Export Worksheet'

    df = pd.read_excel(path, sheet_name=sheet_name, header=0)
    columns_map = {x: x.lower() for x in df.columns}

    path_out = path_dir_out / f"{sheet_name}.json"

    out = df.rename(columns=columns_map) \
        .replace({np.nan: None}) \
        .to_dict(orient='records')

    with open(path_out, 'w+', encoding='utf-8') as fp:
        json.dump({'member': out}, fp, indent=2, ensure_ascii=False)
