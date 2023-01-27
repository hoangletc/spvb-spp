import json
from pathlib import Path
from typing import List

from tqdm import tqdm


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
    wo, wo_status = d, d.get('wostatus', None)

    # Parse
    if wo_status:
        wo_status_tmp = {}
        for w in wo_status:
            w = parser_default(
                w,
                "work_order_status",
                schemas['work_order_status']
            )['work_order_status']

            if w['status'] not in wo_status_tmp or \
                    wo_status_tmp[w['status']]['changedate'] < w['changedate']:
                wo_status_tmp[w['status']] = w

        wo_status = list(wo_status_tmp.values())

    wo = parser_default(wo, "work_order", schemas['work_order'])['work_order']
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
            for invul in inv_use
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
    asset, asset_status = d, d.get('assetstatus', None)

    # Parse
    if asset_status:
        asset_status = [
            parser_default(x, "asset_status", schemas['asset_status'])[
                'asset_status']
            for x in asset_status
        ]
    asset_tmp = parser_default(asset, "asset", schemas['asset'])['asset']

    # Supplement info for 'asset'
    if 'assetancestor' in asset:
        if len(asset['assetancestor']) == 1:
            # Asset là 'line'
            asset_tmp['asset_hierachical_type'] = "line"
        elif len(asset['assetancestor']) == 2:
            # Asset là 'machine'
            asset_tmp['asset_hierachical_type'] = "machine"

            # Get parent asset
            parent = None
            for x in asset['assetancestor']:
                if x['hierarchylevels'] == 1:
                    parent = x
                    break
            assert parent is not None, "asset['assetancestor'] không có parent asset (hierarchylevels = 1)"

            asset_tmp['parent'] = parent['ancestor']
        elif len(asset['assetancestor']) == 3:
            # Asset là 'component'
            asset_tmp['asset_hierachical_type'] = "component"

            # Get parent asset
            parent = None
            for x in asset['assetancestor']:
                if x['hierarchylevels'] == 1:
                    parent = x
                    break
            assert parent is not None, "asset['assetancestor'] không có parent asset (hierarchylevels = 1)"

            asset_tmp['parent'] = parent['ancestor']

            # Get grandparent asset
            grandparent = None
            for x in asset['assetancestor']:
                if x['hierarchylevels'] == 2:
                    grandparent = x
                    break
            assert grandparent is not None, "asset['assetancestor'] không có grandparent asset (hierarchylevels = 2)"

            asset_tmp['grandparent'] = grandparent['ancestor']
        else:
            raise NotImplementedError()

    # Supplement info for 'asset_status'
    if asset_status:
        for x in asset_status:
            x['assetnum'] = asset_tmp['assetnum']

    asset = [asset_tmp]
    return {'asset': asset, 'asset_status': asset_status}


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


PARSER_MAPPING = {
    'asset': parser_asset,
    'location': parser_location,
    'work_order': parser_work_order,
    'material_use_trans': parser_matu,
    "inventory": parser_inventory
}


def parser_json(data: List[dict], res_name: str,
                schemas: dict = None, default_key: str = 'member'):
    output = {}

    if default_key in data:
        data = data[default_key]

        for d in data:
            # Apply tailored parser or default parser
            if res_name in PARSER_MAPPING:
                parsed_result: dict = PARSER_MAPPING[res_name](d, schemas)
            else:
                parsed_result: dict = parser_default(d, res_name,
                                                     schemas[res_name])

            # Append parsed result(s)
            for k, v in parsed_result.items():
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
    path_out_root = Path("D:\TC Data\SPP API JSONs\edited")
    # folder chứa file JSON
    path_in = Path(r"D:\TC Data\SPP API JSONs\SPP\material_use_trans")
    # đường dẫn tới
    path_schema = r"D:\TC Data\spvb-spp\scripts\schemmas.json"

    path_out_root.mkdir(parents=True, exist_ok=True)

    # Load schema
    with open(path_schema) as fp:
        schemas: dict = json.load(fp)

        schemas = {k: set(v) for k, v in schemas.items()}

    # Start looping
    for path in tqdm(path_in.glob("*")):
        dir_name, file_name = path.parents[0].name, path.name
        if not ".json" in file_name:
            file_name = f"{file_name}.json"

        with open(path, encoding='utf-8') as fp:
            d: dict = json.load(fp)

        # Start parsing
        out: dict = parser_json(d, dir_name, schemas, default_key='member')

        # Write to file
        for res_name, dat in out.items():
            path_out_dir = path_out_root / res_name
            path_out_dir.mkdir(parents=True, exist_ok=True)

            n_existing_files = len(list(path_out_dir.glob("*.json")))
            path_out = path_out_dir / f"{res_name}_{n_existing_files}.json"

            with open(path_out, "w+", encoding="utf-8") as fp:
                json.dump(dat, fp, indent=2, ensure_ascii=False)
