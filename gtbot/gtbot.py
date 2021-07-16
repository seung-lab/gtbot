from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk import WebClient
import json
import string
import re
import os
import logging
import time
from helper import reply, guess_path
from uploader import upload_dataset
from downloader import parse_nglink
from bbox import convert_pt_to_bbox
from bot_info import slack_token, botid
from fortunate import Fortunate
import random
import wikiquote

import multiprocessing as mp

cmd_list = {
    # Should change upload to preview
    'upload': r"""upload[\s,:]*['"`<~]*([!#$&-;=?-\[\]_~:/\\\w\s]+)[>'"`]*""",
    'save': r"""save[\s,:]*['"`<~]*([!#$&-;=?-\[\]_~:/\\\w\s]+)[>'"`]*""",
    'download': r"""create[\s]*cutouts?[\s,:]*['"`<]*(([!#$&-;=?-\[\]{}"_a-z~]|%[0-9a-fA-F]{2})+)[>'"`]*""",
    'createbbox': r"""create[\s]*bbox(es)?[\s,:]*['"`<]*(([!#$&-;=?-\[\]{}"_a-z~]|%[0-9a-fA-F]{2})+)[>'"`]*"""
}

cutout_parameters = {
    'mip': 1,
    'pad': [256,256,4],
    'prefix': '.'
}

bbox_parameters = {
    'dim': [40920,40920,2048]
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
gen=Fortunate("fortune.dat")
app = App(token=os.environ["SLACK_BOT_TOKEN"])


def gen_quote():
    title = wikiquote.random_titles(max_titles=1)[0]
    return random.choice(wikiquote.quotes(title)) + "\n     --{}".format(title)


def relevant_msg(data):
    if 'subtype' in data: #Do not act to modified messages
        return False
    text = data['text']

    if text.startswith(botid):
        return True


def format_cmd(msg):
    return msg.replace(botid, '').strip().lstrip(string.punctuation).strip()


def load_metadata(path):
    json_names = ["metadata.json", "README.md", "raw/README.md"]
    parent = os.path.split(path)[0]
    for fn in json_names:
        full_fn = os.path.join(parent, fn)
        print("testing json file: ",full_fn)
        if os.path.exists(full_fn):
            try:
                with open(full_fn) as f:
                    metadata =json.load(f)
                    return metadata
            except ValueError:
                pass
        else:
            print("{} does not exist".format(full_fn))
    return None


def handle_upload(q):
    while True:
        logger.debug("check queue")
        if q.qsize() == 0:
            time.sleep(1)
            continue
        logger.debug("get message from queue")
        d = q.get()
        cmd = format_cmd(d['text'])

        bucket = "gtbot"
        m = re.match(cmd_list['upload'], cmd)
        if m is None:
            m = re.match(cmd_list['save'], cmd)
            if m is not None:
                bucket = "gtbot_perm"
            else:
                continue

        print(m[1])
        path = guess_path(m[1])
        if path is not None:
            reply(d, "Start uploading and meshing dataset: {}".format(m[1]))
        else:
            reply(d, "Cannot find the path: {}".format(m[1]), broadcast=True)
            continue
        metadata = load_metadata(path)
        if metadata is None:
            path = path+"/export"
            metadata = load_metadata(path)

        if metadata is None:
            reply(d, "Cannot load metadata, cannot upload", broadcast=True)
            continue
        print(json.dumps(metadata, indent=2))

        try:
            #fortune = gen()
            reply(d, "```{}```".format(gen_quote()), broadcast=True)
            #page = wikipedia.page(wikipedia.random())
            #reply(d, "{}".format(page.url), broadcast=True)
        except Exception:
            pass

        try:
            upload_dataset(d, path, bucket, metadata)
        except Exception as e:
            reply(d, "Some error I cannot handle: {}".format(str(e)))
            pass


def handle_download(q):
    while True:
        if q.qsize() == 0:
            time.sleep(1)
            continue
        d = q.get()
        cmd = format_cmd(d['text'])
        m = re.match(cmd_list['download'], cmd)
        try:
            fortune = gen()
            reply(d, "```{}```".format(fortune.strip()), broadcast=True)
            #page = wikipedia.page(wikipedia.random())
            #reply(d, "{}".format(page.url), broadcast=True)
        except Exception:
            pass
        try:
            reply(d, "Creating cutouts for ground truthing")
            parse_nglink(d, m[1], cutout_parameters)
        except Exception as e:
            reply(d, "Some error I cannot handle: {}".format(str(e)))
            pass

def process_bbox(payload):
    cmd = format_cmd(payload['text'])
    m = re.match(cmd_list['createbbox'], cmd)
    try:
        reply(payload, "Convert point annotations to bboxes")
        url = convert_pt_to_bbox(payload, m[2], bbox_parameters)
        reply(payload, url)
    except Exception as e:
        reply(payload, "Some error I cannot handle: {}".format(str(e)))
        pass


@app.event({"type": "message"})
def handle_cmd(body: dict):
    d = body['event']
    logger.debug(d)
    if relevant_msg(d):
        cmd = format_cmd(d['text'])
        logger.debug("try to match cmd")
        m = re.match(cmd_list['upload'], cmd)
        if m is not None:
            q_up.put(d)
            reply(d, "Add upload task, {} task(s) in the queue at this moment".format(q_up.qsize()))
            return

        m = re.match(cmd_list['save'], cmd)
        if m is not None:
            q_up.put(d)
            reply(d, "Add save task, {} task(s) in the queue at this moment".format(q_up.qsize()))
            return

        m = re.match(cmd_list['download'], cmd)
        if m is not None:
            q_down.put(d)
            reply(d, "Add download task, {} task(s) in the queue at this moment".format(q_down.qsize()))
            return

        m = re.match(cmd_list['createbbox'], cmd)
        if m is not None:
            process_bbox(d)
            return

        reply(d, "sorry, I do not understand the message")

def hello_world():
    client = WebClient(token=slack_token)

    client.chat_postMessage(
        channel='#seuron-alerts',
        text="gtbot rebooted at {}!".format(os.uname()[1]))


if __name__ == '__main__':
    q_up = mp.Queue()
    q_down = mp.Queue()
    p_up = mp.Process(target=handle_upload, args=(q_up,))
    p_down = mp.Process(target=handle_download, args=(q_down,))
    p_up.start()
    p_down.start()
    handler = SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"])
    handler.start()
    print("starting the bot")
    hello_world()
    p_up.join()
    p_down.join()
