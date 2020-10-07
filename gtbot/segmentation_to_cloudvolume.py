#!/usr/bin/python
"""
T Macrina
190505

Ingest VAST segmentation into precomputed format
"""
from cloudvolume import CloudVolume
from cloudvolume.lib import Bbox, Vec
import numpy as np
from utilities import *
from os.path import join
from zmesh import Mesher
import argparse

def dir_to_image(seg_path, crop_bbox, extension='tif'):
    img = load_from_dir(seg_path, extension)
    return img[crop_bbox.to_slices()]

def write_unique_ids(path, img):
    unique_ids = np.unique(img)
    np.savetxt(path, unique_ids, '%i', delimiter='\n')

def image_to_cloudvolume(cv, img):
    cv[dst_bbox.to_slices()] = img
    cv = CloudVolume(src_path, mip=mip, fill_missing=True)
    pad = Vec(*pad)
    size = Vec(*size)
    center = Vec(*center)
    makedirs(dst_path, exist_ok=True)
    vol_start = center - size // 2
    vol_bbox = Bbox(vol_start - pad, vol_start + size + pad)
    draw_bbox = Bbox(vol_start, vol_start + size)
    vol_bbox = cv.bbox_to_mip(vol_bbox, 0, mip)
    draw_bbox = cv.bbox_to_mip(draw_bbox, 0, mip)
    img = cv[vol_bbox.to_slices()][:,:,:,0]
    local_draw_bbox = draw_bbox - vol_bbox.minpt
    draw_bounding_cube(img, local_draw_bbox, val=255)
    write_to_tif_dir(dst_path, img, extension=extension)

  img_cv  = 'gs://microns-seunglab/minnie_v0/minnie10/image'
  dst_cv  = 'gs://microns-seunglab/minnie_v0/minnie10/ground_truth/{vol}/draft'.format(vol=vol)
  provenance = {}
  provenance['owner'] = 'tmacrina'
  provenance['timestamp'] = str(datetime.today())
  provenance['image_path'] = img_cv 
  provenance['local_path'] = join('~', root, vol, ext)
  mip = 1
  pad = Vec(256, 256, 2)
  vol_start = Vec(259093, 159369, 20602)
  vol_size = Vec(512, 512, 36)
  src_bbox = Bbox(vol_start - pad, vol_start + vol_size + pad)
  dst_bbox = Bbox(vol_start, vol_start + vol_size)
  info = CloudVolume.create_new_info(1, 'segmentation', 'uint32', 'raw', 
                                     (4,4,40), dst_bbox.minpt, dst_bbox.size3(),
                                     mesh='mesh_mip_1_err_0', chunk_size=(64,64,32))
  cv = CloudVolume(dst_cv, mip=0, info=info, provenance=provenance)
  cv.add_scale([2,2,1])
  cv.commit_info()
  cv.commit_provenance()
  cv = CloudVolume(dst_cv, mip=mip, bounded=True, autocrop=True, 
                    non_aligned_writes=True, cdn_cache=False, fill_missing=True)
  src_bbox = cv.bbox_to_mip(src_bbox, 0, mip)
  dst_bbox = cv.bbox_to_mip(dst_bbox, 0, mip)
  crop_bbox = dst_bbox - src_bbox.minpt
  save_to_cv(local_dst, cv, dst_bbox, crop_bbox)
 
  p = 4 
  with LocalTaskQueue(parallel=p) as tq:
    tc.create_meshing_tasks(tq, dst_cv, mip=mip, shape=(320, 320, 40), 
                            max_simplification_error=0)
  with LocalTaskQueue(parallel=p) as tq:
    tc.create_mesh_manifest_tasks(tq, dst_cv)
  print("Meshing complete!")

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--seg_path', type=str, help='local path to segmentation')
  parser.add_argument('--provenance_path', type=str, 
                      help='local path to provenance')
  args = parser.parse_args()
  provenance = load_provenance(args.provenance_path)['processing']
  cv = get_cloudvolume(cv_path=cv_path, params
  dir_to_cloudvolume(seg_path=seg_path, **params)
