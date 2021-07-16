from PIL import Image
import numpy as np
import urllib
import requests
import json
from collections import OrderedDict
from slack_sdk import WebClient
import os
import re
import ntpath
from bot_info import slack_token, oauth_token, workspace_prefix


def safe_string(s):
    keepcharacters = (' ','.','_','-')
    return "".join(c if c.isalnum() or c in keepcharacters else "_" for c in s ).rstrip()


def reply(data, msg, broadcast=False):
    try:
        channel_id = data['channel']
        thread_ts = data['ts']
        user = data['user']
    except (KeyError, ValueError, IndexError):
        print(data)
        return

    webclient = WebClient(token=slack_token)
    webclient.chat_postMessage(
        channel=channel_id,
        thread_ts=thread_ts,
        unfurl_links=True,
        reply_broadcast=broadcast,
        text="<@{}>, {}".format(user, msg),
    )


def user_info(data, key):
    try:
        user = data['user']
    except (KeyError, ValueError, IndexError):
        print(data)
        return None
    webclient = WebClient(token=slack_token)
    rc = webclient.users_info(user=user)
    try:
        return rc["user"]["profile"][key]
    except KeyError:
        print("does not have key {} in the profile".format(key))
        return None


def extrac_bucket_path(path):
    m = re.search(r"""[/\w\s]*seungmount/([/\w\s\.-]+)""", path)
    if m is None:
        print("Not a unix path, try windows format")
        s = ntpath.splitdrive(path)
        if s[0] != "" and s[1] != "":
            return s[1][1:].replace("\\", "/")
        else:
            return None
    else:
        if m[1] != "":
            return m[1]
        else:
            return None


def create_bucket_url(local_path):
    bucket_path = extrac_bucket_path(local_path)
    if bucket_path is not None:
        return "bucket://"+bucket_path
    else:
        return local_path


def guess_path(path):
    my_bucket = workspace_prefix
    bucket_path = extrac_bucket_path(path)
    if bucket_path is not None:
        my_path = os.path.join(my_bucket, bucket_path)
        if os.path.exists(my_path):
            return my_path

    return None

def get_ng_payload(handle, url):
    try:
        components = urllib.parse.urlparse(url)
    except ValueError:
        reply(handle, "Invalid url: {}".format(url), broadcast=True)
        return None
    #print(urllib.parse.unquote(components.fragment))
    print(components.query)
    try:
        payload = ""
        if len(components.fragment) == 0:
            json_url = components.query.replace('json_url=','')
            if oauth_token:
                headers = {'Authorization': 'Bearer {}'.format(oauth_token)}
                r = requests.get(json_url, headers=headers)
            else:
                r = requests.get(json_url)
            payload = r.text
        else:
            payload = urllib.parse.unquote(components.fragment)[1:]
        data = json.loads(payload, object_pairs_hook=OrderedDict)
        return data
    except (ValueError, KeyError):
        reply(handle, "Cannot read json payload from the neuroglancer link: {}".format(url), broadcast=False)
        return None


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
  else:
    return img.astype(np.uint8)


def load_from_dir(src_dir, extension='tif'):
  """Assume directory contains only the images to be stored
  """
  from joblib import Parallel, delayed
  files = os.listdir(src_dir)
  files.sort()
  imgs = Parallel(n_jobs=-1)(delayed(load_image)(os.path.join(src_dir, fn)) for fn in files if fn.endswith(extension))
  #for fn in files:
  #  if fn.endswith(extension):
  #    imgs.append(load_image(os.path.join(src_dir, fn)))
  if imgs[0].dtype == np.uint8:
    return {'uploaded_image': np.asarray(imgs).transpose(2,1,0)}
  return {'seg': np.asarray(imgs).transpose(2,1,0)}


def load_from_omni_h5(fn):
    import h5py
    import numpy as np
    omni_types = {
        'working': 1,
        'valid': 2,
        'uncertain': 3
    }

    #omni_types = {
    #    'soma': 1,
    #    'axon': 2,
    #    'dendrite': 3,
    #    'glia': 5
    #}
    dirpath = os.path.split(fn)[0]
    with h5py.File(fn,"r") as f:
        data = np.squeeze(np.array(f['main'])).transpose(2,1,0)
        if len(data.shape) > 3:
            print("only support 3 dimensional dataset")
            return None
        if data.dtype == np.uint8 or data.dtype == np.float32:
            return {'raw_image': data}
    try:
        seg_type = np.loadtxt(os.path.join(dirpath, "segments.txt"), dtype=(int,int), delimiter=',', skiprows=2)
        #seg_type = np.loadtxt(os.path.join(dirpath, "segments.txt"), dtype=(int,int), delimiter=',')
    except IOError:
        return {'seg': data.astype(np.uint32)}

    seg_data = {'seg': data.astype(np.uint32)}
    #seg_data = dict()
    seg_type = dict(seg_type)
    for k in omni_types:
        print("process omni type: {}".format(k))
        seg = np.copy(data)
        fltr = np.vectorize(lambda x: True if x not in seg_type or seg_type[x] != omni_types[k] else False)
        mask = fltr(seg)
        seg[mask] = 0
        seg_data[k] = seg.astype(np.uint32)

    return seg_data


def draw_bounding_cube(img, bbox, val=255, thickness=1):
  minpt = bbox.minpt
  maxpt = bbox.maxpt
  z_slice = slice(minpt.z, maxpt.z)
  for t in range(0, thickness):
    img[minpt.x+t, :, z_slice] = val
    img[maxpt.x+t,  :, z_slice] = val
    img[:, minpt.y+t, z_slice] = val
    img[:, maxpt.y+t,  z_slice] = val


def write_to_dir(dst_dir, img, extension='tif'):
    """Split 3d ndimgay along z dim into 2d sections & save as tifs
    """
    for k in range(img.shape[2]):
      fn = os.path.join(dst_dir, '{0:03d}.{1}'.format(k+1, extension))
      print('Writing {0}'.format(fn))
      arr = Image.fromarray(img[:,:,k].T)
      arr.save(fn)


