import json
import requests
import secrets
from helper import get_ng_payload
def convert_pt_to_bbox(handle, url, parameters):
    payload = get_ng_payload(handle, url)
    layers = payload['layers']
    for l in layers:
        if l in layers:
            if 'annotations' in l:
                new_bboxes = []
                for a in l['annotations']:
                    if a['type'] =="point":
                        minpt = [a['point'][i]-parameters['dim'][i]/2 for i in range(3)]
                        maxpt = [minpt[i] + parameters['dim'][i] for i in range(3)]
                        bbox_annotation = {
                            "pointA": minpt,
                            "pointB": maxpt,
                            "type": "axis_aligned_bounding_box",
                            "id": secrets.token_hex(40)
                        }
                        new_bboxes.append(bbox_annotation)
                l['annotations'] += new_bboxes


    r = requests.post('https://www.dynamicannotationframework.com/nglstate/post', data=json.dumps(payload))
    link = r.text.strip()
    link = link[1:-1]

    ng_link = "https://neuromancer-seung-import.appspot.com/?json_url={}".format(link)

    return ng_link
