from helper import load_from_dir, reply, user_info, safe_string, load_from_omni_h5

from cloudvolume import CloudVolume
from cloudvolume.lib import Bbox, Vec
from taskqueue import LocalTaskQueue
import igneous.task_creation as tc
from time import sleep

from datetime import datetime
import numpy as np
import secrets
import urllib
import requests
import json
from collections import OrderedDict
from bot_info import oauth_token

def create_nglink(image_layer, seg_layers, center):
    ng_host = "https://neuromancer-seung-import.appspot.com"
    layers = OrderedDict()
    layers["img"] = {
        "source": "precomputed://"+image_layer,
        "type": "image"
    }

    for s in seg_layers:
        if "img" in s or "image" in s:
            layers[s] = {
                "source": "precomputed://"+seg_layers[s],
                "type": "image",
            }
        else:
            layers[s] = {
                "source": "precomputed://"+seg_layers[s],
                "type": "segmentation",
            }

    navigation = {
        "pose": {
            "position": {
                "voxelSize": [4, 4, 40],
                "voxelCoordinates": center
            }
        },
        "zoomFactor": 4
    }

    payload = OrderedDict([("layers", layers), ("navigation", navigation),("showSlices", False),("layout", "xy-3d")])


    url = "{host}/#!{payload}".format(
        host=ng_host,
        payload=urllib.parse.quote(json.dumps(payload))
    )

    try:
        headers = {'Authorization': 'Bearer {}'.format(oauth_token)}
        r = requests.post('https://globalv1.daf-apis.com/nglstate/post', headers=headers, data=json.dumps(payload))
    except:
        return "neuroglancer link: {}".format(url)

    if r.ok:
        link = r.text.strip()
        link = link[1:-1]
        ng_link = "https://neuromancer-seung-import.appspot.com/?json_url={}".format(link)
        return "neuroglancer link: {}".format(ng_link)
    else:
        return "neuroglancer link: {}".format(url)


def upload_dataset(handle, path, bucket, metadata):
    try:
        parameters = metadata['raw']
        pad = Vec(*parameters['pad'])
        image_layer = parameters['src_path']
        mip = parameters['mip']
    except KeyError:
        reply(handle, "I do not understand the metadata", broadcast=True)
        return False

    try:
        size = Vec(*parameters['size'])
        center = Vec(*parameters['center'])
        vol_start = center - size//2
        vol_stop = center + size//2 - Vec(1,1,0)
    except KeyError:
        if 'bbox' not in parameters:
            reply(handle, "Cannot contruct the bounding box", broadcast=True)
            return False
        bbox = parameters['bbox']
        vol_start = Vec(*bbox[0:3])
        vol_stop = Vec(*bbox[3:6])

    data = None
    if path.endswith('.h5'):
        data = load_from_omni_h5(path)
    else:
        extensions = ['tif', 'png']
        for e in extensions:
            data = load_from_dir(path, extension=e)
            if data:
                break

    ng_layers = {}
    for k in data:
        reply(handle, "Uploading layer {}".format(k))
        if "img" in k or "image" in k:
            img_layer = upload_img(handle, bucket, data[k], vol_start, vol_stop, metadata)
            ng_layers[k] = img_layer
        else:
            seg_layer = upload_seg(handle, bucket, data[k], vol_start, vol_stop, metadata)
            ng_layers[k] = seg_layer

    reply(handle, "done!", broadcast=True)

    center = [(vol_start[i] + vol_stop[i])/2 for i in range(3)]
    reply(handle, create_nglink(image_layer, ng_layers, center))

#FIXME: reuse the code from upload_seg
def upload_img(handle, bucket, data, vol_start, vol_stop, metadata):
    parameters = metadata['raw']
    pad = Vec(*parameters['pad'])
    image_layer = parameters['src_path']
    mip = parameters['mip']

    author = safe_string(user_info(handle, "display_name"))
    p = 16

    if author is None or author.strip() == "":
        author = "gtbot"

    img_layer = "gs://{}/{}/{}".format(bucket, author.replace(" ", "_"), secrets.token_hex(8))
    dst_bbox = Bbox(vol_start, vol_stop)
    mip0_bbox = Bbox(dst_bbox.minpt - pad, dst_bbox.maxpt + pad)
    data_type = 'uint8' if data.dtype == np.uint8 else 'float32'
    info = CloudVolume.create_new_info(
        num_channels = 1,
        layer_type   = 'image',
        data_type    = data_type,
        encoding     = 'raw',
        resolution   = (4,4,40),
        voxel_offset = dst_bbox.minpt,
        volume_size  = dst_bbox.size3(),
        chunk_size   = (64,64,8)
    )

    cv = CloudVolume(img_layer, mip=0, info=info)
    for i in range(mip):
        cv.add_scale([1<<(i+1),1<<(i+1),1])
    cv.commit_info()
    cv.provenance.processing.append({
        'owner': author,
        'timestamp': str(datetime.today()),
        'image_path': image_layer
    })
    cv.commit_provenance()

    cv = CloudVolume(img_layer, mip=mip, parallel=p, bounded=False, autocrop=True,
                     cdn_cache=False, fill_missing=True)

    print("original bbox")
    print(mip0_bbox)
    print(dst_bbox)
    src_bbox = cv.bbox_to_mip(mip0_bbox, 0, mip)
    dst_bbox = cv.bbox_to_mip(dst_bbox, 0, mip)
    crop_bbox = dst_bbox - src_bbox.minpt
    reply(handle, "loading the image...".format(img_layer))
    print(src_bbox)
    print(dst_bbox)
    print(data.shape)
    data = data[crop_bbox.to_slices()]
    print(crop_bbox)
    print(data.shape)
    print(dst_bbox)
    reply(handle, "uploading...".format(img_layer))
    cv[dst_bbox.to_slices()] = data
    reply(handle, "downsampling...")
    with LocalTaskQueue(parallel=p) as tq:
        tasks = tc.create_downsampling_tasks(img_layer, mip=mip, fill_missing=True, preserve_chunk_size=True)
        tq.insert_all(tasks)
        print("downsampled")
    return img_layer


def upload_seg(handle, bucket, data, vol_start, vol_stop, metadata):
    parameters = metadata['raw']
    pad = Vec(*parameters['pad'])
    image_layer = parameters['src_path']
    mip = parameters['mip']

    p = 16
    author = safe_string(user_info(handle, "display_name"))

    if author is None or author.strip() == "":
        author = "gtbot"

    seg_layer = "gs://{}/{}/{}".format(bucket, author.replace(" ", "_"), secrets.token_hex(8))
    dst_bbox = Bbox(vol_start, vol_stop)
    mip0_bbox = Bbox(dst_bbox.minpt - pad, dst_bbox.maxpt + pad)
    info = CloudVolume.create_new_info(
        num_channels = 1,
        layer_type   = 'segmentation',
        data_type    = 'uint32',
        encoding     = 'raw',
        resolution   = (4,4,40),
        voxel_offset = dst_bbox.minpt,
        volume_size  = dst_bbox.size3(),
        mesh         = 'mesh_mip_{}_err_0'.format(mip),
        chunk_size   = (64,64,8)
    )

    cv = CloudVolume(seg_layer, mip=0, info=info)
    for i in range(mip):
        cv.add_scale([1<<(i+1),1<<(i+1),1])
    cv.commit_info()
    cv.provenance.processing.append({
        'owner': author,
        'timestamp': str(datetime.today()),
        'image_path': image_layer
    })
    cv.commit_provenance()

    cv = CloudVolume(seg_layer, mip=mip, parallel=p, bounded=False, autocrop=True,
                     cdn_cache=False, fill_missing=True)

    print("original bbox")
    print(mip0_bbox)
    print(dst_bbox)
    src_bbox = cv.bbox_to_mip(mip0_bbox, 0, mip)
    dst_bbox = cv.bbox_to_mip(dst_bbox, 0, mip)
    crop_bbox = dst_bbox - src_bbox.minpt
    reply(handle, "loading the segments...".format(seg_layer))
    print(src_bbox)
    print(dst_bbox)
    print(data.shape)
    data = data[crop_bbox.to_slices()]
    print(crop_bbox)
    print(data.shape)
    print(dst_bbox)
    reply(handle, "uploading...".format(seg_layer))
    cv[dst_bbox.to_slices()] = data

    reply(handle, "meshing...")
    with LocalTaskQueue(parallel=p) as tq:
        tasks = tc.create_downsampling_tasks(seg_layer, mip=mip, fill_missing=True, preserve_chunk_size=True)
        tq.insert_all(tasks)
        print("downsampled")
        tasks = tc.create_meshing_tasks(seg_layer, mip=mip, simplification=False, shape=(320, 320, 40),
                              max_simplification_error=0)
        tq.insert_all(tasks)
        print("meshed")
        tasks = tc.create_mesh_manifest_tasks(seg_layer, magnitude=1)
        tq.insert_all(tasks)

    return seg_layer


if __name__ == '__main__':
    import sys

    with open(sys.argv[2]) as f:
        metadata = json.load(f)
        upload_dataset("", sys.argv[1], metadata)
