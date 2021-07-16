import json
import requests
import secrets
from helper import get_ng_payload, reply
from bot_info import oauth_token
from cloudvolume import CloudVolume
import numpy as np


def find_first_seg_layer(layers):
    for l in layers:
        if l['type'] == 'segmentation' and l.get('visible', True):
            source = l['source']
            if source.startswith("precomputed://"):
                path = source[14:]
                return [l['name'], path]
    return None

def find_first_graphene_layer(layers):
    for l in layers:
        if l['type'] == 'segmentation_with_graph' and l.get('visible', True):
            return [l['name'], l['source']]
    return None

def idx_to_coord(bbox, idx):
    dim = [bbox[i+3] - bbox[i] for i in range(3)]
    strides = [dim[-2]*dim[-1], dim[-1], 1]
    x = int(idx/strides[0])
    y = int(idx%strides[0]/strides[1])
    z = int(idx%strides[1])
    return [bbox[0]+x, bbox[1]+y, bbox[2]+z]


def add_segids(cv_seg, bbox, threshold):
    cutout = np.squeeze(cv_seg[bbox[0]:bbox[3], bbox[1]:bbox[4], bbox[2]:bbox[5]])
    segs, idx, size  = np.unique(cutout, return_counts=True, return_index=True)
    segs[size < threshold] = 0
    seglist = []
    sorted_idx = np.argsort(size)
    for i in sorted_idx[::-1]:
        if segs[i] != 0:
            seglist.append((segs[i], idx_to_coord(bbox, idx[i])))
    print(seglist[0])
    return seglist


def cv_scale_with_data(path):
    print(path)
    vol = CloudVolume(path)
    try:
        for m in vol.available_mips:
            if vol.image.has_data(m):
                return vol.scales[m]['resolution']
    except NotImplementedError:
        print("Cannot check cloudvolume data")
        return vol.scales[0]['resolution']


def convert_pt_to_bbox(handle, url, parameters):
    payload = get_ng_payload(handle, url)
    layers = payload['layers']
    seg_layer = find_first_seg_layer(layers)
    if not seg_layer:
        seg_layer = find_first_graphene_layer(layers)

    scales = payload["navigation"]["pose"]["position"]["voxelSize"]

    if seg_layer:
        reply(handle, f"Select segments > {parameters['size_threshold']} voxels in the bboxes in segmentation layer {seg_layer[0]} ({seg_layer[1]})")
        scales = cv_scale_with_data(seg_layer[1])
        seg_vol = CloudVolume(seg_layer[1], mip=scales)
        if seg_layer[1].startswith("graphene://"):
            seg_vol.agglomerate = True

    seglist = []

    reply(handle, f"Convert point annotations to bboxes of {parameters['dim']}")

    for l in layers:
        if l in layers:
            if 'annotations' in l and l.get('visible', True):
                voxelSize = l['voxelSize']
                print(voxelSize, scales)
                new_bboxes = []
                for a in l['annotations']:
                    if a['type'] =="point":
                        print(a)
                        minpt = [a['point'][i]-parameters['dim'][i]/2 for i in range(3)]
                        maxpt = [minpt[i] + parameters['dim'][i] for i in range(3)]
                        print(minpt, maxpt)
                        bbox = [int(minpt[i]*voxelSize[i]/scales[i]) for i in range(3)] + [int(maxpt[i]*voxelSize[i]/scales[i]) for i in range(3)]
                        reply(handle, f"bbox: {bbox}")
                        if seg_layer:
                            seglist += add_segids(seg_vol, bbox, parameters['size_threshold'])
                        bbox_annotation = {
                            "pointA": minpt,
                            "pointB": maxpt,
                            "type": "axis_aligned_bounding_box",
                            "id": secrets.token_hex(40)
                        }
                        new_bboxes.append(bbox_annotation)
                l['annotations'] += new_bboxes

    if len(seglist) > 0:
        anno_layer = {
            "tool": "annotatePoint",
            "type": "annotation",
            "annotations": [],
            "annotationTags": [
                {
                  "id": 1,
                  "label": "signal"
                },
                {
                  "id": 2,
                  "label": "background"
                },
                {
                  "id": 3,
                  "label": "unknown"
                }
            ],
            "voxelSize": scales,
            "bracketShortcutsShowSegmentation": True,
            "annotationSelectionShowsSegmentation": True,
            "name": "seg_annotations"
        }
        reply(handle, f"Select {len(seglist)} segments")
        anno_layer["linkedSegmentationLayer"] = seg_layer[0]
        for s in seglist:
            pt_annotation = {
                "point": s[1],
                "type": "point",
                "id": secrets.token_hex(40),
                "segments": [str(s[0])]
            }
            anno_layer["annotations"].append(pt_annotation)

        layers.append(anno_layer)

    headers = {'Authorization': 'Bearer {}'.format(oauth_token)}
    r = requests.post('https://globalv1.daf-apis.com/nglstate/post', headers=headers, data=json.dumps(payload))
    link = r.text.strip()
    link = link[1:-1]

    ng_link = "https://neuromancer-seung-import.appspot.com/?json_url={}".format(link)

    return ng_link
