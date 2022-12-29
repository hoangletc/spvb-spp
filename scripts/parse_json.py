import json
from typing import List


def parse_member(schema: dict, d: List[dict]):
    discards = {"_rowstamp", "href", "invcost_collectionref",
                "lochierarchy_collectionref", "wostatus_collectionref",
                "multiassetlocci_collectionref", "_imagelibref"}

    output = {}

    def _parse(key_list, val):
        if isinstance(val, dict):
            out = {}
            for k, v in val.items():
                if k in discards:
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

    filename = "work_order"

    path_json_in = f"E:\TC Data\SPP API JSONs\{filename}.json"
    path_json_out = f"E:\TC Data\SPP API JSONs\edited\{filename}_edited.json"
    path_schema = f"E:\TC Data\spvb-spp\scripts\schemmas.json"

    with open(path_json_in) as fp:
        d: dict = json.load(fp)
    with open(path_schema) as fp:
        schemas: dict = json.load(fp)

        schemas = {k: set(v) for k, v in schemas.items()}

    if filename not in schemas:
        print(f"schemmas.json not contain schema for file '{filename}'")

        schema = {}
    else:
        schema = schemas[filename]

    out = parse_member(schema, d)

    with open(path_json_out, "w+", encoding="utf-8") as fp:
        json.dump(out, fp, indent=2, ensure_ascii=False)
