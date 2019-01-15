# import matplotlib.pyplot as plt
from pdb import set_trace as bp

import requests
from threading import Thread

import os
import shutil

# gw: local module
import cl_request
from utils import CombinedRecord, ImageRecord


SUBSCRIPTION_KEY = '20624d4ef17548ddaecdefbbcb558764'
assert SUBSCRIPTION_KEY, "azure subscription key not set!!"

# gw: mine is west us server
AZURE_IMAGE_SEARCH_API_URL = "https://westus2.api.cognitive.microsoft.com/bing/v7.0/images/search"

TPS_LIMIT = 250
PERIOD = 1 // TPS_LIMIT


TIMEOUT = 10
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
    
    if is_proxied:
        response = cl_request.proxied_get_https_url(AZURE_IMAGE_SEARCH_API_URL, timeout=TIMEOUT, headers=headers, params=params)
    else:
        response = requests.get(AZURE_IMAGE_SEARCH_API_URL, headers=headers, params=params, timeout=TIMEOUT)

        # gw: ?? TODO: what is this stmt doing?
    response.raise_for_status()
    search_results = response.json()

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
    
    
    
    
