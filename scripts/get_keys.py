import json

filename = r"D:\TC Data\SPP API JSONs\SPP\item\responseBI_Item.json"
with open(filename, encoding='utf-8') as fp:
    d = json.load(fp)['member'][0]

    for k, v in d.items():
        if 'ref' in k and isinstance(v, str) and v.startswith('http'):
            continue
        if k == '_rowstamp':
            continue
        if not isinstance(v, (str, bool, int, float)):
            continue

        print(k)
