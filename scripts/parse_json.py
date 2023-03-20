import calendar
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

from tqdm import tqdm

DT_FORMAT = "%Y-%m-%dT%H:%M:%S%z"

prev_items = set()


def parser_default(d: List[dict], res_name: str, schema: set = None):
    def _parse(key_list, val):
        if isinstance(val, dict):
            out = {}
            for k, v in val.items():
                if k == '_rowstamp':
                    continue
                if isinstance(v, list):
                    continue
                if "ref" in k and isinstance(v, str) and v.startswith("http"):
                    continue

                res = _parse(key_list + [k], v)

                if isinstance(res, dict) is True:
                    out = {**out, **res}
                else:
                    k = '_'.join(key_list + [k])
                    out[k] = res
        else:
            out = val

        return out

    # Parse nested dict
    parsed = _parse([], d)

    # Make use of schema
    if schema is None:
        print(f"schemmas.json not contain schema for resource '{res_name}'")
    else:
        out_tmp = {}

        for column in schema:
            if column in parsed:
                out_tmp[column] = parsed[column]
            else:
                out_tmp[column] = None

        parsed = out_tmp

    return {res_name: parsed}


def parser_location(d: dict, schemas: dict = None) -> dict:
    # Parse
    loc = parser_default(d, "location", schemas['location'])['location']

    # Supplement info
    loc['site'] = loc['location'][:3]

    loc = [loc]  # Return must be dict whose value is list

    return {'location': loc}


def parser_work_order(d: dict, schemas: dict = None) -> dict:
    WO_STATUS_ORDER = ['WSCH', 'PLANNING', 'APPR', 'INPRG', 'FINISHED', 'COMPLETED', 'COMP', 'CLOSE']

    wo, wo_status = d, d.get('wostatus', None)

    # Parse
    wo = parser_default(wo, "work_order", schemas['work_order'])['work_order']

    if wo_status:
        if isinstance(wo_status, dict):
            wo_status = [wo_status]

        wo_status_tmp = []
        for w in wo_status:
            if w['status'] not in WO_STATUS_ORDER:
                continue

            w = parser_default(
                w,
                "work_order_status",
                schemas['work_order_status']
            )['work_order_status']

            wo_status_tmp.append(w)

        wo_status = wo_status_tmp

    wo = [wo]

    return {'work_order': wo, 'work_order_status': wo_status}


def parser_matu(d: dict, schemas: dict = None) -> dict:
    matu = d
    inv_use = d.get('invuse', None)
    inv_use_line = d.get('invuseline', None)

    # Parse
    if inv_use:
        if isinstance(inv_use, dict):
            inv_use = [inv_use]

        inv_use = [
            parser_default(invu, "inventory_use",
                           schemas['inventory_use']
                           )['inventory_use']
            for invu in inv_use
        ]
    if inv_use_line:
        if isinstance(inv_use_line, dict):
            inv_use_line = [inv_use_line]

        inv_use_line = [
            parser_default(invul, "inventory_use_line",
                           schemas['inventory_use_line']
                           )['inventory_use_line']
            for invul in inv_use_line
        ]

    matu = parser_default(
        matu,
        "material_use_trans",
        schemas['material_use_trans']
    )['material_use_trans']

    # Supplement info for 'inv_use' and 'inv_use_line'
    if inv_use:
        for x in inv_use:
            x['itemnum'] = matu['itemnum']
            x['assetnum'] = matu['assetnum']
            x['matusetransid'] = matu['matusetransid']
    if inv_use_line:
        for x in inv_use_line:
            x['itemnum'] = matu['itemnum']
            x['assetnum'] = matu['assetnum']
            x['matusetransid'] = matu['matusetransid']

    matu = [matu]

    return {
        'material_use_trans': matu,
        'inventory_use': inv_use,
        'inventory_use_line': inv_use_line
    }


def parser_asset(d: dict, schemas: dict = None) -> dict:
    def _to_str(a):
        if isinstance(a, float):
            a = round(a)

        return str(a)

    asset, asset_status, asset_ancestor = d, d.get('assetstatus', None), d.get('assetancestor', None)

    if asset_status in ({}, []):
        asset_status = None
    if asset_ancestor in ({}, []):
        asset_ancestor = None

    # Parse
    if asset_ancestor:
        if isinstance(asset_ancestor, dict):
            asset_ancestor = [asset_ancestor]

        asset_ancestor = [
            parser_default(x, "asset_ancestor", schemas['asset_ancestor'])[
                'asset_ancestor']
            for x in asset_ancestor
        ]

    if asset_status:
        if isinstance(asset_status, dict):
            asset_status = [asset_status]

        asset_status = [
            parser_default(x, "asset_status", schemas['asset_status'])[
                'asset_status']
            for x in asset_status
        ]

        # Tách downtime ra 2 phần
        final_asset_status = []
        for a_st in asset_status:
            try:
                dt = datetime.strptime(a_st['changedate'], DT_FORMAT)
                a_st['changedate'] = dt.strftime(DT_FORMAT)
            except ValueError:
                try:
                    dt = datetime.strptime(a_st['changedate'], "%d/%m/%Y %H:%M:%S")
                    a_st['changedate'] = dt.strftime(DT_FORMAT)
                except ValueError:
                    logging.error(f"Datetime format không đúng ('%d/%m/%Y %H:%M:%S'): {a_st['changedate']}")

            a_st['assetstatusid'] = _to_str(a_st['assetstatusid'])
            a_st['is_split'] = 0
            a_st['changedate_org'] = a_st['changedate']
            a_st['downtime_org'] = a_st['downtime']

            if a_st['downtime'] == 0:
                final_asset_status.append(a_st)
                continue

            if (dt + timedelta(hours=a_st['downtime'])).month != dt.month:
                last_month_date = datetime(year=dt.year, month=dt.month, day=calendar.monthrange(
                    dt.year, dt.month)[1], hour=23, minute=59, second=59, tzinfo=dt.tzinfo)
                first_nextmonth_date = last_month_date + timedelta(seconds=1)

                downtime1 = (last_month_date - dt).total_seconds() / 3600
                downtime2 = a_st['downtime'] - downtime1

                a_st['downtime'] = downtime1

                new_ast = a_st.copy()
                new_ast['downtime'] = downtime2
                new_ast['changedate'] = first_nextmonth_date.strftime(DT_FORMAT)
                new_ast['is_split'] = 1

                final_asset_status.append(a_st)
                final_asset_status.append(new_ast)

            else:
                final_asset_status.append(a_st)

        asset_status = final_asset_status

    asset_tmp = parser_default(asset, "asset", schemas['asset'])['asset']
    asset_tmp['assetnum'] = _to_str(asset_tmp['assetnum'])
    if 'ancestor' in asset_tmp:
        asset_tmp['ancestor'] = _to_str(asset_tmp['ancestor'])
    asset_tmp['assetnum'] = _to_str(asset_tmp['assetnum'])

    try:
        dt = datetime.strptime(asset_tmp['changedate'], DT_FORMAT)
        asset_tmp['changedate'] = dt.strftime(DT_FORMAT)
    except ValueError:
        try:
            dt = datetime.strptime(asset_tmp['changedate'], "%d/%m/%Y %H:%M:%S")
            asset_tmp['changedate'] = dt.strftime(DT_FORMAT)
        except ValueError:
            logging.error(f"Datetime format không đúng: {asset_tmp['changedate']}")

    # Supplement info for 'asset'
    # if 'assetancestor' in asset and asset['assetancestor'] is not None:
    #     if isinstance(asset['assetancestor'], list):
    #         assetancestor = asset['assetancestor']
    #     else:
    #         assetancestor = [asset['assetancestor']]

    #     asset_tmp['asset_hierachical_count'] = len(assetancestor)

    #     if len(assetancestor) == 1:
    #         # Asset là 'line'
    #         asset_tmp['asset_hierachical_type'] = "line"
    #         asset_tmp['line_asset_num'] = asset_tmp['assetnum']

    #     elif len(assetancestor) == 2:
    #         # Asset là 'machine'
    #         asset_tmp['asset_hierachical_type'] = "machine"
    #         asset_tmp['machine_asset_num'] = asset_tmp['assetnum']

    #         # Get parent asset
    #         parent = None
    #         for x in assetancestor:
    #             if x['hierarchylevels'] == 1:
    #                 parent = x
    #                 break
    #         assert parent is not None, "asset['assetancestor'] không có machine asset (hierarchylevels = 1)"

    #         asset_tmp['line_asset_num'] = _to_str(parent['ancestor'])

    #     elif len(assetancestor) == 3:
    #         # Asset là 'component'
    #         asset_tmp['asset_hierachical_type'] = "component"
    #         asset_tmp['component_asset_num'] = asset_tmp['assetnum']

    #         # Get parent asset
    #         parent = None
    #         for x in assetancestor:
    #             if x['hierarchylevels'] == 1:
    #                 parent = x
    #                 break
    #         assert parent is not None, "asset['assetancestor'] không có machine asset (hierarchylevels = 1)"

    #         asset_tmp['machine_asset_num'] = _to_str(parent['ancestor'])

    #         # Get grandparent asset
    #         grandparent = None
    #         for x in assetancestor:
    #             if x['hierarchylevels'] == 2:
    #                 grandparent = x
    #                 break
    #         assert grandparent is not None, "asset['assetancestor'] không có line asset (hierarchylevels = 2)"

    #         asset_tmp['line_asset_num'] = _to_str(grandparent['ancestor'])

    #     elif len(assetancestor) >= 4:
    #         print(f"asset_num đang bị lỗi: {asset_tmp['assetnum']}")
    #         # TODO: HoangLe [Feb-22]: Fix this
    #     else:
    #         raise NotImplementedError()

    # Supplement info for 'asset_status'
    if asset_status:
        for x in asset_status:
            x['assetnum'] = asset_tmp['assetnum']
            x['assetuid'] = asset_tmp['assetuid']
            x['ancestor'] = asset_tmp['ancestor']
    if asset_ancestor:
        for x in asset_ancestor:
            x['assetnum'] = asset_tmp['assetnum']
            x['assetuid'] = asset_tmp['assetuid']

    asset = [asset_tmp]

    # Discard asset if it exists
    if asset[0]['assetuid'] not in prev_items:
        prev_items.add(asset[0]['assetuid'])
    else:
        asset = []

    return {'asset': asset, 'asset_status': asset_status, 'asset_ancestor': asset_ancestor}


def parser_inventory(d: dict, schemas: dict = None) -> dict:
    inv, inv_cost = d, d.get('inv_cost', None)

    # Parse
    if inv_cost:
        inv_cost = [
            parser_default(x, "inventory_cost", schemas['inventory_cost'])[
                'inventory_cost']
            for x in inv_cost
        ]
    inv_tmp = parser_default(inv, "inventory", schemas['inventory'])[
        'inventory']

    # Supplement info for 'inv_cost'
    if inv_cost:
        for x in inv_cost:
            x['itemnum'] = inv_tmp['itemnum']
            x['inventoryid'] = inv_tmp['inventoryid']

    inv = [inv_tmp]
    return {'inventory': inv, 'inventory_cost': inv_cost}


def parser_matr(d: dict, schemas: dict = None) -> dict:
    def _parse_time(dt_s: str):
        dt = None
        try:
            dt = datetime.strptime(dt_s, DT_FORMAT)
        except ValueError:
            try:
                dt = datetime.strptime(dt_s, "%d-%b-%y")
            except ValueError:
                print(f"Err: Cannot parse to datetime this: {dt_s}")

        return dt

    matr = d

    matr = parser_default(matr, "material_receipt_trans",
                          schemas['material_receipt_trans'])[
        'material_receipt_trans']

    # Convert actualdate to from datetime with timezone to datetime
    # and add small amount to second for differentiating purpose
    dt = _parse_time(matr['actualdate'])
    matr['actualdate'] = dt.strftime("%Y-%m-%d %H:%M:%S")

    # Convert transdate to datetime
    dt = _parse_time(matr['transdate'])
    if dt is not None:
        dt_s = dt.strftime("%Y-%m-%d %H:%M:%S.%f")

        matr['transdate'] = dt_s

    matr = [matr]
    return {'material_receipt_trans': matr}


PARSER_MAPPING = {
    'asset': parser_asset,
    'location': parser_location,
    'work_order': parser_work_order,
    'material_use_trans': parser_matu,
    "inventory": parser_inventory,
    "material_receipt_trans": parser_matr
}


def parser_json(data: List[dict], res_name: str,
                schemas: dict = None, default_key: str = 'member'):
    output = {}

    if default_key in data:
        data = data[default_key]

        for d in tqdm(data):
            # Apply tailored parser or default parser
            if res_name in PARSER_MAPPING:
                parsed_result: dict = PARSER_MAPPING[res_name](d, schemas)
            else:
                parsed_result: dict = parser_default(d, res_name,
                                                     schemas[res_name])

            # Append parsed result(s)
            for k, v in parsed_result.items():
                if v is None:
                    continue

                if not isinstance(v, list):
                    v = [v]

                if k not in output:
                    output[k] = v
                else:
                    output[k].extend(v)

    return output


if __name__ == '__main__':
    # Khai báo những biến sau đây:
    # folder lưu kết quả xử lí
    path_out_root = Path(r"D:\TC_Data\_data\_post_processed")
    # folder chứa file JSON
    path_in = Path(r"D:\TC_Data\_data\_pre_processed\inventory_trans")
    # đường dẫn tới
    path_schema = r"D:\TC_Data\spvb-spp\scripts\schemmas.json"

    path_out_root.mkdir(parents=True, exist_ok=True)

    # Load schema
    with open(path_schema) as fp:
        schemas: dict = json.load(fp)

        schemas = {k: set(v) for k, v in schemas.items()}

    # Start looping
    for path in path_in.glob("*"):
        dir_name, file_name = path.parents[0].name, path.name
        if ".json" not in file_name:
            file_name = f"{file_name}.json"

        with open(path, encoding='utf-8') as fp:
            d: dict = json.load(fp)

        # Start parsing
        out: dict = parser_json(d, dir_name, schemas, default_key='member')

        # Write to file
        for res_name, dat in out.items():
            path_out_dir = path_out_root / res_name
            path_out_dir.mkdir(parents=True, exist_ok=True)

            path_out = path_out_dir / f"{res_name}_{path.stem}.json"

            with open(path_out, "w+", encoding="utf-8") as fp:
                json.dump(dat, fp, indent=2, ensure_ascii=False)
