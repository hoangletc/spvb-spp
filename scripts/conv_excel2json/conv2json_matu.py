import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm, trange

logging.getLogger().setLevel(logging.DEBUG)

cols_main = [
    'matusetrans.actualcost', 'matusetrans.assetnum', 'matusetrans.binnum', 'matusetrans.curbal',
    'matusetrans.description', 'matusetrans.exchangerate', 'matusetrans.invuseid', 'matusetrans.invuselineid',
    'matusetrans.issuetype', 'matusetrans.issueunit', 'matusetrans.itemnum', 'matusetrans.linecost',
    'matusetrans.matusetransid', 'matusetrans.mrnum', 'matusetrans.orgid', 'matusetrans.ponum',
    'matusetrans.refwo', 'matusetrans.storeloc', 'matusetrans.tositeid', 'matusetrans.transdate',
    'matusetrans.unitcost', 'matusetrans.actualdate', 'matusetrans.currencycode', 'matusetrans.linetype',
    'matusetrans.location', 'matusetrans.qtyrequested', 'matusetrans.quantity'
]
cols_invu = [
    'invuse.description', 'invuse.currencycode', 'invuse.invusenum', 'invuse.invowner', 'invuse.invuseid',
    'invuse.fromstoreloc', 'invuse.status_description', 'invuse.changedate', 'invuse.usetype_description',
    'invuse.receipts_description', 'invuse.receipts', 'invuse.spvb_internal', 'invuse.usetype', 'invuse.status',
    'invuse.exchangedate'
]
cols_invul = [
    'invuseline.invuselineid', 'invuseline.unitcost', 'invuseline.spvb_extreasoncode',
    'invuseline.spvb_extreasoncode_description', 'invuseline.spvb_mustreturn', 'invuseline.spvb_returnfromissue',
    'invuseline.spvb_mustreturn_org', 'invuseline.returnedqty', 'invuseline.remark', 'invuseline.linetype',
    'invuseline.tositeid', 'invuseline.invusenum', 'invuseline.actualdate', 'invuseline.description',
    'invuseline.receivedqty', 'invuseline.assetnum', 'invuseline.costcenter', 'invuseline.fromstoreloc',
    'invuseline.linecost', 'invuseline.quantity', 'invuseline.refwo', 'invuseline.costcenter_description',
    'invuseline.itemnum', 'invuseline.itemsetid', 'invuseline.location', 'invuseline.invuselinenum',
    'invuseline.usetype', 'invuseline.enterby', 'invuseline.spvb_wonumref', 'invuseline.spvb_reason'
]

cols_main_map = {x: x.removeprefix('matusetrans.') for x in cols_main}
cols_invu_map = {x: x.removeprefix('invuse.') for x in cols_invu}
cols_invul_map = {x: x.removeprefix('invuseline.') for x in cols_invul}

cols_total_map = cols_main_map | cols_invu_map | cols_invul_map

if __name__ == '__main__':
    path = Path(r"D:\TC Data\_data\prod_Feb24\matusetrans.xlsx")
    path_dir_out = Path(r"D:\TC Data\SPP API JSONs\SPP")
    sheet_name = 'Export Worksheet'

    logging.info(f"Load sheet: {sheet_name}")

    df = pd.read_excel(path, sheet_name=sheet_name)
    df = df.replace({np.nan: None})

    matu, invu, invul = [], [], []

    def f(r):
        m, iu, iul = {}, {}, {}

        for c in cols_main:
            m[c.removeprefix('matusetrans.')] = r[c]

        for c in cols_invu:
            iu[c.removeprefix('invuse.')] = r[c]
        iu['matusetransid'] = m['matusetransid']

        for c in cols_invul:
            iul[c.removeprefix('invuseline.')] = r[c]
        iul['matusetransid'] = m['matusetransid']

        matu.append(m)
        invu.append(iu)
        invul.append(iul)

    logging.info("Start applying")

    tqdm.pandas()
    df.progress_apply(f, axis=1)

    path_out = path_dir_out / f"material_use_trans_{sheet_name.replace(' ', '_')}.json"
    with open(path_out, 'w+', encoding='utf-8') as fp:
        json.dump({'member': matu}, fp,
                  indent=2, ensure_ascii=False)

    path_out = path_dir_out / f"invuse_{sheet_name.replace(' ', '_')}.json"
    with open(path_out, 'w+', encoding='utf-8') as fp:
        json.dump({'member': invu}, fp,
                  indent=2, ensure_ascii=False)

    path_out = path_dir_out / f"invuseline_{sheet_name.replace(' ', '_')}.json"
    with open(path_out, 'w+', encoding='utf-8') as fp:
        json.dump({'member': invul}, fp,
                  indent=2, ensure_ascii=False)
