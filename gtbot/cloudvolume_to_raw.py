#!/usr/bin/python
"""
T Macrina
190423

Create VAST directory from CloudVolume cutout
"""
from cloudvolume import CloudVolume
from cloudvolume.lib import Bbox, Vec
from utilities import *
from os import makedirs
from os.path import join
import argparse

def cloudvolume_to_dir(cv_path, raw_path, mip, center, size, pad,
                       extension='tif', **kwargs):
    """Save bbox from src_path to directory of tifs at dst_path

    Args:
      cv_path: CloudVolume path
      raw_path: local path to raw images
      mip: int for MIP of CloudVolume image
      center: int list at MIP0 for center of bbox to extract
      size: int list at MIP0 for size of volume to be annotated
      pad: int list at MIP0 for padding beyond size to be noted in the image
      extension: str for image file extension
    """
    cv = CloudVolume(cv_path, mip=mip, fill_missing=True)
    pad = Vec(*pad)
    size = Vec(*size)
    center = Vec(*center)
    makedirs(raw_path, exist_ok=True)
    vol_start = center - size // 2
    vol_bbox = Bbox(vol_start - pad, vol_start + size + pad)
    draw_bbox = Bbox(vol_start, vol_start + size)
    vol_bbox = cv.bbox_to_mip(vol_bbox, 0, mip)
    draw_bbox = cv.bbox_to_mip(draw_bbox, 0, mip)
    img = cv[vol_bbox.to_slices()][:,:,:,0]
    local_draw_bbox = draw_bbox - vol_bbox.minpt
    draw_bounding_cube(img, local_draw_bbox, val=255)
    write_to_tif_dir(raw_path, img, extension=extension)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--params_path', type=str, help='path to params',
                        default='')
    parser.add_argument('--cv_path', type=str, help='CloudVolume path',
                        default='')
    parser.add_argument('--local_path', type=str, help='Local path',
                        default='')
    parser.add_argument('--mip', type=int, help='MIP level of image',
                        default=0)
    parser.add_argument('--center', type=int, nargs=3,
                        help='center point (MIP0)',
                        default=[0,0,0])
    parser.add_argument('--size', type=int, nargs=3,
                        help='size in each dimension (MIP0)',
                        default=[512,512,64])
    parser.add_argument('--pad', type=int, nargs=3,
                        help='padding in each dimension (MIP0)',
                        default=[128,128,2])
    parser.add_argument('--extension', type=str, default='tif') 
    args = parser.parse_args()
    if args.params_path:
      with open(args.params_path, 'r') as f:
        reader = csv.reader(f, delimiter=',')
        for k, r in enumerate(reader):
          if k != 0:
            args.cv_path = r[0]
            args.root_path = r[1]
            args.local_path = join(r[1], 'raw')
            args.mip = int(r[2])
            args.center = map(int, r[3:6])
            args.size = map(int, r[6:9])
            args.pad = map(int, r[9:12])
            cloudvolume_to_dir(**vars(args))
            write_params(join(args.root_path, 'params.json'), **vars(args))
    else:
      args.raw_path = join(args.local_path, 'raw')
      cloudvolume_to_dir(**vars(args))
      write_params(join(args.local_path, 'params.json'), **vars(args))
