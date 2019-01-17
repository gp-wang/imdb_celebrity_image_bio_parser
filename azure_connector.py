# import matplotlib.pyplot as plt
import logging
from pdb import set_trace as bp

import requests
from threading import Thread

import os
import shutil
import math
# gw: local module
import cl_request
from utils import CombinedRecord, ImageRecord


SUBSCRIPTION_KEY = '20624d4ef17548ddaecdefbbcb558764'
assert SUBSCRIPTION_KEY, "azure subscription key not set!!"

# gw: mine is west us server
AZURE_IMAGE_SEARCH_API_URL = "https://westus2.api.cognitive.microsoft.com/bing/v7.0/images/search"

TPS_LIMIT = 250
PERIOD = 1 // TPS_LIMIT


# TIMEOUT = 10
TIMEOUT = 30

MAX_TRY = 5
SLEEP_TIME = 3

NOF_RECORD_PER_THREADS = 10



def query_azure_image_api_by_string(query_string, is_proxied=False):
    
    headers = {
        "Ocp-Apim-Subscription-Key" : SUBSCRIPTION_KEY,
        'BingAPIs-Market': 'en-US',
    }

    params  = {"q": query_string,
               # "license": "public",    # gw: overly restricting param
               "imageType": "photo",
               "count": 120,    # ms1m dataset has 100 img per person, here parse 120 to leave margin for later manual removal inappropriate photos
               "safeSearch": "Strict",
               "mkt": "en-US",
    }

    search_results = None
    trial = 0
    while trial < MAX_TRY:
        try:
            if is_proxied:
                response = cl_request.proxied_get_https_url(AZURE_IMAGE_SEARCH_API_URL, timeout=TIMEOUT, headers=headers, params=params)
            else:
                response = requests.get(AZURE_IMAGE_SEARCH_API_URL, headers=headers, params=params, timeout=TIMEOUT)

                # gw: ?? TODO: what is this stmt doing?
            response.raise_for_status()
            search_results = response.json()
            break
        except Exception:
            logging.warning("failed on {} trial for getting query of {}".format(trial, query_string))
            continue
        finally:
            trial += 1
    return search_results



def get_celeb_image_url_list(celeb_record):
    """
    Input a CombinedRecord with set ms1m_id and name
    Output a list of ImageRecord with set url, fpath (fpath to be saved, added value here is index increment)

    note: this method queries Azure Image Search API once per method call
    """ 
    if celeb_record:
        assert type(celeb_record) is CombinedRecord

    image_api_query_result_json = query_azure_image_api_by_string(celeb_record.name)

    if not image_api_query_result_json:
        return []
    
    if image_api_query_result_json["value"]:
        object_list = image_api_query_result_json["value"] 
    elif image_api_query_result_json["queryExpansions"]:
        object_list = image_api_query_result_json["queryExpansions"] 
    else:
        object_list = []

    full_img_urls = []
    for img in object_list:
        if "contentUrl" in img:
            full_img_urls.append(img["contentUrl"])
        elif "thumbnailUrl" in img:
            full_img_urls.append(img["thumbnailUrl"])
        elif "thumbnail" in img:
            if "thumbnailUrl" in img["thumbnail"]:
                full_img_urls.append(img["thumbnail"]["thumbnailUrl"])

    image_record_list = [
        ImageRecord(ms1m_id=celeb_record.ms1m_id,
                    name=celeb_record.name,
                    url=img_url,
                    fname="{}.png".format(idx)
        ) for idx, img_url in enumerate(full_img_urls)
    ]

    return image_record_list
    
    
    
    
# --- TPS facility

from threading import Thread, Lock, Event
import time
import random
from concurrent.futures import ThreadPoolExecutor


AZURE_IMAGE_SEARCH_API_TPS_LIMIT = 250
SAFE_TPS_LIMIT = math.floor(AZURE_IMAGE_SEARCH_API_TPS_LIMIT * 0.8)

NOF_TICKETS_PER_SECOND = SAFE_TPS_LIMIT
NOF_THREADS = 256

tickets = 0
ticketing_lock = Lock()
ticketing_event = Event()

def timer_do_work():
    time_zero = time.perf_counter()
    while True:
        global tickets, ticketing_lock
        try:
            ticketing_lock.acquire()
            tickets = NOF_TICKETS_PER_SECOND

            ticketing_event.set()
        finally:
            ticketing_lock.release()
            
        
        elapsed_time = time.perf_counter() - time_zero

        # logging.info("-------------current time: {:6.3f}---------------".format(elapsed_time))

        time.sleep(1)

def worker_do_work(work_item):
    global tickets, ticketing_lock

    while True:
        
        # wait_flag = False # no need own flag, because event has internal flag
        try:
            ticketing_lock.acquire()
            if tickets <= 0:
                # use a local flag to move the event.wait outside locking block. (don't block other threads from using this lock and do checking)
                ticketing_event.clear()
                # wait_flag = True # no need own flag, because event has internal flag

            else:
                # has available tickets, can proceed
                tickets -= 1
                break
        finally:
            ticketing_lock.release()
            
        #if wait_flag: # no need own flag, because event has internal flag
        ticketing_event.wait()
            
    # do actual work
    # logging.info("work item {} has started".format(work_item))
    # time.sleep(random.randint(0, 10))
    # commet it out if it makes you harder to count started work item between timer logging.infoouts
    # logging.info("done with work item {}".format(work_item))

    assert type(work_item) is CombinedRecord, "work item is not celeb record"
    celeb_record = work_item
    results = get_celeb_image_url_list(celeb_record)
    logging.info("done work item {} and got {} image urls".format(celeb_record.name, len(results)))
    return results
    

def ticketed_get_celeb_image_url_list(celeb_records):
    # timer thread responsible for releasing TPS tickets per seconds
    timer_thread = Thread(target=timer_do_work, args=(), daemon=True)
    # timer_thread = Thread(target=timer_do_work, args=())

    timer_thread.start()        # daemon thread no need join later

    results = []
    with ThreadPoolExecutor(max_workers=NOF_THREADS) as executor:
        results = list(executor.map(worker_do_work, celeb_records))
        
    return results
