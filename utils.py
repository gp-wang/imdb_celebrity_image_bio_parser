import requests
from collections import namedtuple
from PIL import Image
from io import BytesIO
import os
import shutil

import cl_request

ImdbRecord = namedtuple('ImdbRecord', ['id', 'name', 'rank'])

# combined record of ms1m and imdb info
CombinedRecord = namedtuple('CombinedRecord', ['ms1m_id', 'imdb_id', 'name', 'rank'])

# class for each single image
ImageRecord = namedtuple('ImageRecord', ['ms1m_id', 'name', 'url', 'fname', ])

ROOT_DIR = os.path.abspath("img_parse_new")


def chunk_it(seq, num):
     # max: for case where num > len(seq)
    avg = max(1, len(seq) / float(num))
    out = []
    last = 0.0

    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg

    return out

def download_one_image_record(image_record, is_proxied=False, timeout=None):
    assert type(image_record) is ImageRecord, "not an ImageRecord"
    trial = 0
    while trial < 3:
        try:
            if is_proxied:
                image_data = cl_request.proxied_get_https_url(image_record.url, timeout=timeout)
            else:
                image_data = requests.get(image_record.url, timeout=timeout)

            pil_image = Image.open(BytesIO(image_data.content))

            dir_path = os.path.join(ROOT_DIR, image_record.ms1m_id)

            if not os.path.isdir(dir_path):
                os.makedirs(dir_path)

            full_img_fpath = os.path.join(dir_path, image_record.fname)

            pil_image.save(full_img_fpath)
            
            break
            
        except Exception:
            print("failed to parse {} on trial #: {}".format(image_record, trial))
            continue
        else:
            print("success parse {} on trial #: {}".format(image_record, trial))
        finally:
            trial += 1

    # TODO
def downloads_images(image_record_list):
    pass
    


def recreate_dir_if_exist(dirpath):
    if os.path.isdir(dirpath):
        shutil.rmtree(dirpath)

    os.makedirs(dirpath)
