
# import matplotlib.pyplot as plt
from PIL import Image
from io import BytesIO
from pdb import set_trace as bp

import requests
from threading import Thread

import os
import shutil

import cl_request
TIMEOUT = 10
MAX_TRY = 5
SLEEP_TIME = 3

NOF_RECORD_PER_THREADS = 10
ROOT_DIR = os.path.abspath("img_multi_thread")


if os.path.isdir(ROOT_DIR):
    shutil.rmtree(ROOT_DIR)


def get_celeb_photo_by_name(msid, name):
    
    person_dir = os.path.join(ROOT_DIR, msid)

    if os.path.isdir(person_dir):
        shutil.rmtree(person_dir)

    os.makedirs(person_dir)
    
    SUBSCRIPTION_KEY = '20624d4ef17548ddaecdefbbcb558764'
    assert SUBSCRIPTION_KEY


    # gw: note: add the region before .api
    # https://portal.azure.com/?whr=live.com#@gaopengwanghotmail.onmicrosoft.com/resource/subscriptions/707a81ad-5dbe-445c-8528-8ebed679f9f1/resourceGroups/celescope/providers/Microsoft.CognitiveServices/accounts/celeb_parse/quickstart
    search_url = "https://westus2.api.cognitive.microsoft.com/bing/v7.0/images/search"

    headers = {
        "Ocp-Apim-Subscription-Key" : SUBSCRIPTION_KEY,
        'BingAPIs-Market': 'en-US',
        
    }

    params  = {"q": name,
               # "license": "public",    # gw: overly restricting param
               "imageType": "photo",
               "count": 120,    # ms1m dataset has 100 img per person, here parse 120 to leave margin for later manual removal inappropriate photos
               "safeSearch": "Strict",
               "mkt": "en-US",
    }
    # response = requests.get(search_url, headers=headers, params=params)

    response = cl_request.proxied_get_https_url(search_url, timeout=TIMEOUT, headers=headers, params=params)
    response.raise_for_status()
    search_results = response.json()
    

    # thumbnail_urls = [img["thumbnailUrl"] for img in search_results["value"][:16]]
    # gw: not all response have conten url,
    # thumbnails seems large enough

    #bp()
    if search_results["value"]:
        object_list = search_results["value"] 
    elif search_results["queryExpansions"]:
        object_list = search_results["queryExpansions"] 
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

    print("size of full_img_urls: {}".format(len(full_img_urls)))
    # if len(full_img_urls) == 0:
    #     full_img_urls = [img["thumbnailUrl"] for img in search_results["value"]]
    #     if len(full_img_urls) == 0:
    #         full_img_urls = [img["thumbnail"]["thumbnailUrl"] for img in search_results["value"]]
    # bp()
    # # display thumnails

    # f, axes = plt.subplots(4, 4)
    # for i in range(4):
    #     for j in range(4):
    #         image_data = requests.get(thumbnail_urls[i+4*j])
    #         image_data.raise_for_status()
    #         image = Image.open(BytesIO(image_data.content))        
    #         axes[i][j].imshow(image)
    #         axes[i][j].axis("off")
    # plt.show()


    def parse_img_urls(img_urls, offset):
        for idx,img_url in enumerate(img_urls):
            skip_flag = False
            nof_tries = 0
            if not skip_flag:
                for i in range(MAX_TRY):
                    try:
                        # image_data = requests.get(img_url)
                        image_data = cl_request.proxied_get_https_url(img_url, timeout=TIMEOUT)
                        image = Image.open(BytesIO(image_data.content))
                        image.save(os.path.join(person_dir, "{}.png".format(offset+idx, idx) ))
                        break   # break on first success
                    except requests.exceptions.Timeout:
                        print("Timeout detected and handled with retry")
                        if nof_tries < MAX_TRIES:
                            time.sleep(SLEEP_TIME)
                            continue
                    except Exception as inst:
                        print("failed to parse {}, idx: {}".format(name, idx))
                        print(type(inst))    # the exception instance
                        print(inst.args)     # arguments stored in .args
                        print(inst)
                        skip_flag = True
                        break   #  break on first unhandable exception
                    finally:
                        nof_tries += 1
        print("finished parsing msid {}, name {}, img at offset {}".format( msid, name, offset))

    threads = [Thread(target=parse_img_urls, args=(full_img_urls[x: x + NOF_RECORD_PER_THREADS], x)) for x in range(0, len(full_img_urls), NOF_RECORD_PER_THREADS) ]

    for t in threads:
        t.start()


    for t in threads:
        t.join()



