import json
from pathlib import Path
from typing import List

from tqdm import tqdm


def parse_member(schema: dict, d: List[dict]):
    discards = {"_rowstamp", "href", "invcost_collectionref",
                "lochierarchy_collectionref", "wostatus_collectionref",
                "multiassetlocci_collectionref", "_imagelibref", "localref"}

    output = {}

    def _parse(key_list, val):
        if isinstance(val, dict):
            out = {}
            for k, v in val.items():
                if k in discards:
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

    if "member" in d:
        d = d['member']

        output = []
        for v in d:
            parsed = _parse([], v)

            # Fill missing column(s)
            if len(schema) != 0:
                for column in schema:
                    if column not in parsed:
                        parsed[column] = None

            # Append to final
            output.append(parsed)

    return output


if __name__ == '__main__':
    # d = {
    #     "member": [
    #         {
    #             "itemnum": "61234140",
    #             "_rowstamp": "549074940",
    #             "maxlevel": 0.0,
    #             "lastissuedate": "2017-11-01T01:00:00+07:00",
    #             "siteid": "170",
    #             "location": "7S0.S1",
    #             "href": "http://10.100.60.130:9082/maxtest/oslc/os/bi_inve/_NjEyMzQxNDAvU1BWQlNFVDEvN1MwLlMxLzE3MA--",
    #             "invcost_collectionref": "http://10.100.60.130:9082/maxtest/oslc/os/bi_inve/_NjEyMzQxNDAvU1BWQlNFVDEvN1MwLlMxLzE3MA--/invcost",
    #             "invcost": {"avgcost": 0.0}
    #         }
    #     ]
    # }

    path_out_root = Path("D:\TC Data\SPP API JSONs\edited")
    path_out_root.mkdir(parents=True, exist_ok=True)

    path_in = Path(r"D:\TC Data\SPP API JSONs\SPP\asset")
    path_schema = r"D:\TC Data\spvb-spp\scripts\schemmas.json"

    # Load schema
    with open(path_schema) as fp:
        schemas: dict = json.load(fp)

        schemas = {k: set(v) for k, v in schemas.items()}

    # Start looping
    for path in tqdm(path_in.glob("*")):
        dir_name, file_name = path.parents[0].name, path.name
        if not ".json" in file_name:
            file_name = f"{file_name}.json"

        path_out = path_out_root / dir_name / file_name
        path_out.parent.mkdir(parents=True, exist_ok=True)

        with open(path, encoding='utf-8') as fp:
            d: dict = json.load(fp)

        if dir_name not in schemas:
            print(f"schemmas.json not contain schema for file '{dir_name}'")

            schema = {}
        else:
            schema = schemas[dir_name]

        out = parse_member(schema, d)

        with open(path_out, "w+", encoding="utf-8") as fp:
            json.dump(out, fp, indent=2, ensure_ascii=False)
