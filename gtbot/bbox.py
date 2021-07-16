import json
import requests
import secrets
from helper import get_ng_payload
from bot_info import oauth_token
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


    headers = {'Authorization': 'Bearer {}'.format(oauth_token)}
    r = requests.post('https://globalv1.daf-apis.com/nglstate/post', headers=headers, data=json.dumps(payload))
    link = r.text.strip()
    link = link[1:-1]

    ng_link = "https://neuromancer-seung-import.appspot.com/?json_url={}".format(link)

    return ng_link
