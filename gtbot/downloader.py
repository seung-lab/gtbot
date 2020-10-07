"""
T Macrina
190423

Create VAST directory from CloudVolume cutout
"""
from cloudvolume import CloudVolume
from cloudvolume.lib import Bbox, Vec
from helper import draw_bounding_cube, write_to_dir, reply, user_info, safe_string, create_bucket_url, get_ng_payload
import os
import json
import urllib
import secrets
import requests
from datetime import datetime
from collections import OrderedDict


def get_first_image_layer(layers):
    for l in layers:
        if l['type'] == 'image':
            return l['source'].replace("precomputed://","")


def get_bboxes(layers):
    bboxes = []
    for l in layers:
        if 'annotations' in l:
            for a in l['annotations']:
                if a['type'] == 'axis_aligned_bounding_box':
                    pointA = list(map(int, a['pointA']))
                    pointB = list(map(int, a['pointB']))
                    bbox = pointA + pointB
                    name = secrets.token_hex(8)
                    if 'description' in a:
                        keepcharacters = (' ','.','_')
                        safe_name = "".join(c if c.isalnum() or c in keepcharacters else "_" for c in a['description'] ).rstrip()
                        name = safe_name[:16]+"_"+name

                    bboxes.append({
                        'name': name,
                        'bbox': bbox
                    })

    return bboxes


def parse_nglink(handle, url, parameters):
    reply(handle, "Analysing neuroglancer link...")
    payload = get_ng_payload(handle, url)
    if payload is None:
        return
    layers = payload['layers']

    cv_path = get_first_image_layer(layers)
    bboxes =  get_bboxes(layers)

    reply(handle, "Processing {} bbox annotation".format(len(bboxes)))
    if len(bboxes) == 0:
        return
    author = safe_string(user_info(handle, "display_name"))
    for b in bboxes:
        dirname = os.path.join(author, b['name'])
        cloudvolume_to_dir(handle, cv_path, dirname, b['bbox'], parameters)
    reply(handle, "done!", broadcast=True)


def cloudvolume_to_dir(handle, cv_path, output_path, bbox, parameters,
                       extension='tif', **kwargs):
    """Save bbox from src_path to directory of tifs at dst_path

    Args:
      cv_path: CloudVolume path
      output_path: local output path
      bbox: int list at MIP0 for the bbox
      parameters: dict contains:
                  prefix to generate the full local path
                  the mip level of the cloudvolume image,
                  padding beyond size to be noted in the image at MIP0
      extension: str for image file extension
    """

    author = user_info(handle, "display_name")

    mip = parameters['mip']
    pad = parameters['pad']
    local_prefix = parameters['prefix']
    cv = CloudVolume(cv_path, mip=mip, fill_missing=True)
    pad = Vec(*pad)
    full_raw_path = os.path.join(local_prefix, output_path, "raw")
    os.makedirs(full_raw_path, exist_ok=True)
    mip0_bbox = Bbox.from_list(bbox)
    vol_start = mip0_bbox.minpt
    vol_stop = mip0_bbox.maxpt
    vol_bbox = cv.bbox_to_mip(Bbox(vol_start - pad, vol_stop + pad), 0, mip)
    draw_bbox = cv.bbox_to_mip(mip0_bbox, 0, mip)
    img = cv[vol_bbox.to_slices()][:,:,:,0]
    local_draw_bbox = draw_bbox - vol_bbox.minpt
    if any(x != 0 for x in pad):
        draw_bounding_cube(img, local_draw_bbox, val=255)
    write_to_dir(full_raw_path, img, extension=extension)


    metadata = {
        'raw': {
            'pad': parameters['pad'],
            'bbox': bbox,
            'mip': parameters['mip'],
            'user': author,
            'timestamp': str(datetime.today()),
            'src_path': cv_path,
            'dst_path': os.path.join(local_prefix, output_path)
        }
    }

    with open(os.path.join(local_prefix, output_path, 'metadata.json'), 'w') as f:
        json.dump(metadata, f, indent=2)

    msg = '''
Cutout volume `{name}` created!
bucket path: `{path}`
Image layer: `{cv_path}`
Bounding box: [{bbox}]
Size: [{size}]
Mip level: {mip}
Padding: [{pad}]
'''.format(
        name=output_path,
        path=create_bucket_url(os.path.join(local_prefix, output_path)),
        cv_path=cv_path,
        bbox=", ".join(str(x) for x in bbox),
        size=", ".join(str(int(x)) for x in draw_bbox.size3()),
        mip=mip,
        pad=", ".join(str(x) for x in pad))
    reply(handle, msg)


if __name__ == '__main__':
    import sys
    parameters = {
        'mip': 1,
        'pad': [256,256,4],
        'prefix': '.'
    }
    parse_nglink(None,"https://neuromancer-seung-import.appspot.com/#!%7B%22layers%22:%5B%7B%22source%22:%22precomputed://gs://microns-seunglab/drosophila_v0/alignment/vector_fixer30_faster_v01/v4/mip1%22%2C%22type%22:%22image%22%2C%22name%22:%22img%22%7D%2C%7B%22source%22:%22precomputed://gs://neuroglancer/gtbot/Szi-chieh/7d25231da52f7d91%22%2C%22type%22:%22segmentation%22%2C%22name%22:%22seg%22%7D%2C%7B%22tool%22:%22annotatePoint%22%2C%22selectedAnnotation%22:%22fcaa0b9f64c7bd4db45fd7dadae162d10ad4382c%22%2C%22type%22:%22annotation%22%2C%22annotations%22:%5B%7B%22pointA%22:%5B132840.5%2C34792.5%2C3525%5D%2C%22pointB%22:%5B133197.5%2C35151.5%2C3511%5D%2C%22type%22:%22axis_aligned_bounding_box%22%2C%22id%22:%2234aa71a5bf0394e73dbc01fbbd25c8363914039e%22%2C%22description%22:%22test%22%7D%2C%7B%22pointA%22:%5B132256%2C34862%2C3527%5D%2C%22pointB%22:%5B132672%2C35314%2C3535%5D%2C%22type%22:%22axis_aligned_bounding_box%22%2C%22id%22:%22fcaa0b9f64c7bd4db45fd7dadae162d10ad4382c%22%2C%22description%22:%22test2%22%7D%2C%7B%22point%22:%5B132917%2C34912%2C3532%5D%2C%22type%22:%22point%22%2C%22id%22:%22b91b79b64236b5dc870e4f62b7754a794975b2f6%22%7D%2C%7B%22point%22:%5B132951%2C35090%2C3532%5D%2C%22type%22:%22point%22%2C%22id%22:%22887c8d8e1153b6c0e1c6a5ebb5a96b08869dcab2%22%7D%5D%2C%22annotationTags%22:%5B%5D%2C%22voxelSize%22:%5B4%2C4%2C40%5D%2C%22name%22:%22annotation%22%7D%5D%2C%22navigation%22:%7B%22pose%22:%7B%22position%22:%7B%22voxelSize%22:%5B4%2C4%2C40%5D%2C%22voxelCoordinates%22:%5B133060.875%2C35511.24609375%2C3527%5D%7D%7D%2C%22zoomFactor%22:13.280467690946193%7D%2C%22showSlices%22:false%2C%22selectedLayer%22:%7B%22layer%22:%22annotation%22%2C%22visible%22:true%7D%2C%22layout%22:%22xy%22%7D", parameters)

    #cloudvolume_to_dir(sys.argv[1], sys.argv[2], [132840, 34792, 3525, 133197, 35151, 3511], parameters)

