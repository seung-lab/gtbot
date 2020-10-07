#!/usr/bin/python
"""
T Macrina
180522

Ingest VAST segmentation into precomputed format
"""

from cloudvolume import CloudVolume
from cloudvolume.lib import Bbox, Vec
from PIL import Image
import numpy as np
import sys
import os
from os.path import join, expanduser
from os import makedirs
from taskqueue import LocalTaskQueue
import igneous.task_creation as tc
from datetime import datetime
from scipy import ndimage
import pandas as pd

def uint32_to_RGB(img):
  """Convert ndarray from uint32 to RGB image (24-bit) by PIL definition
  """
  img = img.reshape(img.shape + (1,)).view(np.uint8)
  img = img[:,:,::-1]
  return img[:,:,1:]

def load_image(src_path):
  """Open TIF image and convert to numpy ndarray of dtype
  
  Currently tested for only for uint8 -> uint8, uint32 or uint24 -> uint32
  
  Args:
  	src_path: full path of the image
  
  Returns:
  	An ndarray of dtype
  """
  img = np.array(Image.open(src_path))
  if len(img.shape) == 3:
    img = np.dstack((np.zeros(img.shape[:2]+(1,)), img))
    img = img[:,:, ::-1]
    img = img.astype(np.uint8).view(np.uint32)
    img = img.reshape(img.shape[:2])
  return img.astype(np.uint32)

def write_to_tif_dir(dir, img):
  """Split 3d ndimgay along z dim into 2d sections & save as tifs
  """
  for k in range(img.shape[0]):
    fn = join(dir, '{:03d}.tif'.format(k+1))
    print('Writing {0}'.format(fn))
    arr = uint32_to_RGB(img[k,:,:])
    arr = Image.fromarray(arr, mode='RGB')
    arr.save(fn)		

def compile_dirs(dir_list, dst_dir, pad=2):
  """Combine all directories in dir_list into one dst_dir, adjusting by overlap of pad
  """
  max_id = 0
  img_list = []
  for k, d in enumerate(dir_list):
    print('{0}: {1}'.format(k,d), end='')
    img = compile_image(d)
    z_mask = img == 0
    img += max_id
    img[z_mask] = 0
    max_id = np.max(img)
    sl = slice(pad, -pad) 
    if k == 0:
      sl = slice(0, sl.stop) 
    if k == len(dir_list)-1:
      sl = slice(sl.start, img.shape[2]) 
    img_list.append(img[sl,:,:])
  arr = np.concatenate(img_list, axis=0)
  return arr

def compile_image(src_dir):
  """Assume directory contains only the images to be stored
  """
  files = os.listdir(src_dir)
  files.sort()
  imgs = []
  for k, fn in enumerate(files):
    print('{0}: {1}'.format(k,fn))
    if fn.endswith('.tif'):
      imgs.append(load_image(join(src_dir, fn)))
  return np.asarray(imgs)

def save_to_cv(src_path, cv, dst_bbox, crop_bbox):
  img = compile_image(src_path).transpose(2,1,0)
  img = img[crop_bbox.to_slices()]
  cv[dst_bbox.to_slices()] = img
  mx = np.max(img)
  hist = ndimage.histogram(img,0,mx,mx)
  id_path = join(src_path, '../cell_segmentation_ids.csv')
  pd.Series(hist, dtype=int).to_csv(path_or_buf=id_path, index=True, 
                                    header=['size'], index_label='segment_id')

def main():
  home = expanduser('~')
  root = 'seungmount/Omni/TracerTasks/drosophila_v0/cremi/B/Stitched/cremib_stitched_export_3.0'
  # sub_dirs = [join(home, root, '{v}{l}'.format(v=vol, l=l), ext) for l in ['A', 'B']]
  local_dst = join(home, root)
  # makedirs(local_dst, exist_ok=True)
  # arr = compile_dirs(sub_dirs, local_dst, pad=2)
  # write_to_tif_dir(local_dst, arr)
  
  img_cv  = 'gs://neuroglancer/kisuk/CREMI/dodam/B/img'
  dst_cv  = 'gs://neuroglancer/kisuk/CREMI/dodam/B/ground_truth/draft03'
  provenance = {}
  provenance['owner'] = 'tmacrina'
  provenance['timestamp'] = str(datetime.today())
  provenance['image_path'] = img_cv 
  provenance['local_path'] = join('~', root)
  mip = 0
  pad = Vec(256, 256, 8)
  vol_start = Vec(0, 0, 0)
  vol_stop = Vec(2100, 1650, 155)
  src_bbox = Bbox(vol_start, vol_stop)
  dst_bbox = Bbox(vol_start + pad, vol_stop - pad)
  info = CloudVolume.create_new_info(1, 'segmentation', 'uint32', 'raw', 
                                     (4,4,40), dst_bbox.minpt, dst_bbox.size3(),
                                     mesh='mesh_mip_{}_err_0'.format(mip),
                                     chunk_size=(64,64,32))
  cv = CloudVolume(dst_cv, mip=0, info=info, provenance=provenance)
  # cv.add_scale([2,2,1])
  cv.commit_info()
  cv.commit_provenance()
  cv = CloudVolume(dst_cv, mip=mip, bounded=True, autocrop=True, 
                    non_aligned_writes=True, cdn_cache=False, fill_missing=True)
  src_bbox = cv.bbox_to_mip(src_bbox, 0, mip)
  dst_bbox = cv.bbox_to_mip(dst_bbox, 0, mip)
  crop_bbox = dst_bbox - src_bbox.minpt
  # save_to_cv(local_dst, cv, dst_bbox, crop_bbox)
 
  p = 4 
  with LocalTaskQueue(parallel=p) as tq:
    tc.create_meshing_tasks(tq, dst_cv, mip=mip, shape=(320, 320, 40), 
                            max_simplification_error=0)
  with LocalTaskQueue(parallel=p) as tq:
    tc.create_mesh_manifest_tasks(tq, dst_cv)
  print("Meshing complete!")

if __name__ == '__main__':
  main()
