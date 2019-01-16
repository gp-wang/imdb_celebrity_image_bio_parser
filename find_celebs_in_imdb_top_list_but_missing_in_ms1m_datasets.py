# goal: find out out of the top 5k celeb (imdb rank), which are not included in our filtered ms1m dataset
#
# method: for entries(imdb_id) "results", which are not included by "links"


import math
import shutil
import time
from pdb import set_trace as bp
import os
from threading import Thread, Lock
import multiprocessing
import time

# local modules gw
import cl_request
import utils
from utils import ImdbRecord, CombinedRecord
import azure_connector

TIMEOUT = 10
MAX_TRY = 5



# Input files
IN_IMDB_TOP_CELEB_LIST_FPATH = "./results"
IN_MATCHED_PAIRS_OF_IMDB_TO_MS1M_FPATH = "./links"

imdb_results_in_famous_order = []

time_ground_zero = time.perf_counter()

with open(IN_IMDB_TOP_CELEB_LIST_FPATH, "r") as f:
     content = f.readlines()

     for idx,line in enumerate(content):
          # print(line)
          line = line.strip('()\n')
          line = line.split(',')
          # index, imdb_id, full_name
          imdb_id, imdb_name = line[1].strip('\' '), line[2].strip('\' ')
          imdb_results_in_famous_order.append(ImdbRecord(id=imdb_id, name=imdb_name, rank=idx))
          

print("done reading parser results")          

imdb_ids_from_matched_links = set()
with open(IN_MATCHED_PAIRS_OF_IMDB_TO_MS1M_FPATH, "r") as f:
     for line in f.readlines():
         _imdb_id, msid = line.split()
         imdb_id = _imdb_id.strip(", ")
         imdb_ids_from_matched_links.add(imdb_id)

print("done reading matched links")


missing_celeb_records = []
for idx, imdb_record in enumerate(imdb_results_in_famous_order):
    if not imdb_record.id in imdb_ids_from_matched_links:
         combined_record = CombinedRecord("m.{:08d}".format(idx), imdb_record.id, imdb_record.name, rank=imdb_record.rank)
         missing_celeb_records.append(combined_record)
         
OUT_CELEB_LIST_MISSING_FROM_IMAGE_DATASET_FPATH = "to_be_manually_added.txt"
with open(OUT_CELEB_LIST_MISSING_FROM_IMAGE_DATASET_FPATH, "w") as f:
    for idx,combined_record in enumerate(missing_celeb_records):
        print('{}\t{}\t{}\t"{}"'.format(combined_record.rank, combined_record.ms1m_id, combined_record.imdb_id, combined_record.name), file=f)

time_done_finding_celeb_list = time.perf_counter()        
print("done! finding missing celebs in {} secs".format(time_done_finding_celeb_list - time_ground_zero))


# --- calling azure image search API to get images urls --- (not downloading yet)
print("Start getting image urls ......")

AZURE_IMAGE_SEARCH_API_TPS_LIMIT = 250
SAFE_TPS_LIMIT = math.floor(AZURE_IMAGE_SEARCH_API_TPS_LIMIT * 0.8)

NOF_THREADS = 8

# pool_8 = multiprocessing.Pool(processes=8)

time_curr = time.perf_counter()
# list_of_celeb_image_record_list = pool_8.map(azure_connector.get_celeb_image_url_list, missing_celeb_records)

list_of_celeb_image_record_list = azure_connector.ticketed_get_celeb_image_url_list(missing_celeb_records)

# bp()
flattened_list_of_celeb_image_record_list = []
for sublist in list_of_celeb_image_record_list:
     if sublist:
          flattened_list_of_celeb_image_record_list += sublist

time_prev = time_curr
time_curr = time.perf_counter()
print("done getting image url list within {}".format(time_curr - time_prev))




# --- downloading actual images --
print("Start downloading images for missing celebs ......")

if os.path.isdir(utils.ROOT_DIR):
     shutil.rmtree(utils.ROOT_DIR)

os.makedirs(utils.ROOT_DIR)     
     
time_curr = time.perf_counter()

pool_32 = multiprocessing.Pool(processes=32)
pool_32.map(utils.download_one_image_record, flattened_list_of_celeb_image_record_list)
pool_32.close()
pool_32.join()
time_prev = time_curr
time_curr = time.perf_counter()
print("done downloading images list within {}".format(time_curr - time_prev))
