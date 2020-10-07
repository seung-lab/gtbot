#!/usr/bin/python
"""
T Macrina
190515

Common functions for emt
"""
from cloudvolume import CloudVolume
from cloudvolume.lib import Bbox, Vec
from PIL import Image
import numpy as np
import sys
import json
import csv
import datetime
import os
from os import makedirs
from os.path import join, expanduser
from getpass import getuser

def draw_bounding_cube(img, bbox, val=255, thickness=1):
  minpt = bbox.minpt
  maxpt = bbox.maxpt
  z_slice = slice(minpt.z, maxpt.z)
  for t in range(-thickness, thickness):
    img[minpt.x+t, :, z_slice] = val
    img[maxpt.x+t,  :, z_slice] = val
    img[:, minpt.y+t, z_slice] = val
    img[:, maxpt.y+t,  z_slice] = val

def uint32_to_RGB(img):
  """Convert ndarray from uint32 to RGB image (24-bit) by PIL definition
  """
  img = img.reshape(img.shape + (1,)).view(np.uint8)
  img = img[:,:,::-1]
  return img[:,:,1:]

def load_image(img_path):
  """Open TIF image and convert to numpy ndarray of dtype

  Currently tested for only for uint8 -> uint8, uint32 or uint24 -> uint32

  Args:
    img_path: full path of the image

  Returns:
    An ndarray of dtype
  """
  img = np.array(Image.open(img_path))
  if len(img.shape) == 3:
    img = np.dstack((np.zeros(img.shape[:2]+(1,)), img))
    img = img[:,:, ::-1]
    img = img.astype(np.uint8).view(np.uint32)
    img = img.reshape(img.shape[:2])
  return img.astype(np.uint32)

def load_from_dir(src_dir, extension='tif'):
  """Assume directory contains only the images to be stored
  """
  files = os.listdir(src_dir)
  files.sort()
  imgs = []
  for fn in files:
    if fn.endswith(extension):
      imgs.append(load_image(join(src_dir, fn)))
  return np.asarray(imgs).transpose(2,1,0)

def write_to_dir(dst_dir, img, extension='tif'):
    """Split 3d ndimgay along z dim into 2d sections & save as tifs
    """
    for k in range(img.shape[2]):
      fn = join(dst_dir, '{0:03d}.{1}'.format(k+1, extension))
      print('Writing {0}'.format(fn))
      arr = Image.fromarray(img[:,:,k].T)
      arr.save(fn)

def get_provenance(**kwargs):
      params = {}
      params['owners'] = [getuser()]
      now = datetime.datetime.now(datetime.timezone.utc)
      params['timestamp'] = now.strftime('%Y-%m-%d %H:%M:%S %Z')
      params['script_path'] = os.path.abspath(__file__)
      params['processing'] = kwargs
      return params

def write_provenance(path, **kwargs):
    """Write out params from directory creation scripts
    """
    with open(path, 'w') as f:
      params = get_provenance(**kwargs)
      f.write(json.dumps(params, indent=4, sort_keys=True))
      f.close()

def load_provenance(path):
    """Load params from directory creation scripts
    """
    with open(path, 'r') as f:
        return json.load(f)

def get_bboxes(center, size, pad, **kwargs):
    pad = Vec(*pad)
    size = Vec(*size)
    center = Vec(*center)
    src_bbox = Bbox(vol_start - pad, vol_start + vol_size + pad)
    dst_bbox = Bbox(vol_start, vol_start + vol_size)
    return src_bbox, dst_bbox

def bboxes_to_mip(cv_path, mip, bboxes, **kwargs):
    cv = CloudVolume(cv_path, mip=mip)
    return [cv.bbox_to_mip(bbox, 0, mip) for bbox in bboxes]

def create_cloudvolume(path, mip, center, size, pad, **kwargs):
    provenance = create_provenance(**kwargs)
    info = CloudVolume.create_new_info(1, 'segmentation', 'uint32', 'raw', 
                                       (4,4,40), dst_bbox.minpt, dst_bbox.size3(),
                                       mesh='mesh_mip_1_err_0', chunk_size=(64,64,32))
    cv = CloudVolume(path, mip=mip, bounded=True, autocrop=True,
                     non_aligned_writes=True, cdn_cache=False,
                     fill_missing=True)
