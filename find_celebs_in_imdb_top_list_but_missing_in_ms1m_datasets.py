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

# constants
TIMEOUT = 10
MAX_TRY = 5

AZURE_IMAGE_SEARCH_API_TPS_LIMIT = 250
SAFE_TPS_LIMIT = math.floor(AZURE_IMAGE_SEARCH_API_TPS_LIMIT * 0.8)
NOF_THREADS = 8

# Input files
IN_IMDB_TOP_CELEB_LIST_FPATH = "./results"
IN_MATCHED_PAIRS_OF_IMDB_TO_MS1M_FPATH = "./links"
OUT_CELEB_LIST_MISSING_FROM_IMAGE_DATASET_FPATH = "to_be_manually_added.txt"


def get_imdb_celebs_in_famous_order(imdb_celeb_list_fpath):
    """
     params:
     imdb_celeb_list_fpath (str): file path
     
     returns:
     [ImdbRecord]
     """
    imdb_results_in_famous_order = []
    # with open(IN_IMDB_TOP_CELEB_LIST_FPATH, "r") as f:
    with open(imdb_celeb_list_fpath, "r") as f:
        content = f.readlines()

        for idx, line in enumerate(content):
            # print(line)
            line = line.strip('()\n')
            line = line.split(',')
            # index, imdb_id, full_name
            imdb_id, imdb_name = line[1].strip('\' '), line[2].strip('\' ')
            imdb_results_in_famous_order.append(
                ImdbRecord(id=imdb_id, name=imdb_name, rank=idx))
    return imdb_results_in_famous_order


def get_missing_celebs(imdb_results_in_famous_order,
                       matched_pairs_imdb_to_ms1m_fpath):
    '''
     get missing celebs who are in imdb top list but not in matched pairs of ms1m-imdb
     param imdb_results_in_famous_order: a list of ImdbRecord ordered by imdb popularity rank
     param matched_pairs_imdb_to_ms1m_fpath: a file path, e.g. "./links"

     return:
     [ CombinedRecord]
     '''
    imdb_ids_from_matched_links = set()
    #with open(IN_MATCHED_PAIRS_OF_IMDB_TO_MS1M_FPATH, "r") as f:
    with open(matched_pairs_imdb_to_ms1m_fpath, "r") as f:
        for line in f.readlines():
            _imdb_id, msid = line.split()
            imdb_id = _imdb_id.strip(", ")
            imdb_ids_from_matched_links.add(imdb_id)

    print("done reading matched links")

    missing_celeb_records = []
    for idx, imdb_record in enumerate(imdb_results_in_famous_order):
        if not imdb_record.id in imdb_ids_from_matched_links:
            combined_record = CombinedRecord(
                "m.{:08d}".format(idx),
                imdb_record.id,
                imdb_record.name,
                rank=imdb_record.rank)
            missing_celeb_records.append(combined_record)

    return missing_celeb_records


def write_out_missing_celeb_list(missing_celeb_records, out_file_path):
    with open(out_file_path, "w") as f:
        # with open(OUT_CELEB_LIST_MISSING_FROM_IMAGE_DATASET_FPATH, "w") as f:
        for idx, combined_record in enumerate(missing_celeb_records):
            print(
                '{}\t{}\t{}\t"{}"'.format(
                    combined_record.rank, combined_record.ms1m_id,
                    combined_record.imdb_id, combined_record.name),
                file=f)


def get_flattened_img_url_list_for_downloading(missing_celeb_records):
    """

     params:
     missing_celeb_records ([CombinedRecord])

     returns:
     [str] : urls for img
     
     """

    list_of_celeb_image_record_list = azure_connector.ticketed_get_celeb_image_url_list(
        missing_celeb_records)

    # bp()
    flattened_list_of_celeb_image_record_list = []
    for sublist in list_of_celeb_image_record_list:
        if sublist:
            flattened_list_of_celeb_image_record_list += sublist

    return flattened_list_of_celeb_image_record_list


def recreate_dir_if_exist(dirpath):
    if os.path.isdir(dirpath):
        shutil.rmtree(dirpath)

    os.makedirs(dirpath)


def download_flattened_image_url_list(
        flattened_list_of_celeb_image_record_list, download_root_dir):

    recreate_dir_if_exist(download_root_dir)

    pool_32 = multiprocessing.Pool(processes=32)
    pool_32.map(utils.download_one_image_record,
                flattened_list_of_celeb_image_record_list)
    pool_32.close()
    pool_32.join()


# -----------
if __name__ == "__main__":
    time_curr = time.perf_counter()
    imdb_results_in_famous_order = get_imdb_celebs_in_famous_order(
        IN_IMDB_TOP_CELEB_LIST_FPATH)
    print("done reading parser results")

    missing_celeb_records = get_missing_celebs(
        imdb_results_in_famous_order, IN_MATCHED_PAIRS_OF_IMDB_TO_MS1M_FPATH)

    write_out_missing_celeb_list(
        missing_celeb_records, OUT_CELEB_LIST_MISSING_FROM_IMAGE_DATASET_FPATH)

    time_prev = time_curr
    time_curr = time.perf_counter()
    print("done! finding missing celebs in {} secs".format(time_curr - time_prev))


    # --- calling azure image search API to get images urls --- (not downloading yet)

    print("Start getting image urls ......")

    time_curr = time.perf_counter()

    flattened_list_of_celeb_image_record_list = get_flattened_img_url_list_for_downloading(
        missing_celeb_records)

    time_prev = time_curr
    time_curr = time.perf_counter()

    print(
        "done getting image url list within {}".format(time_curr - time_prev))

    # --- downloading actual images --
    print("Start downloading images for missing celebs ......")
    time_curr = time.perf_counter()

    download_flattened_image_url_list(
        flattened_list_of_celeb_image_record_list, utils.ROOT_DIR)

    time_prev = time_curr
    time_curr = time.perf_counter()
    print(
        "done downloading images list within {}".format(time_curr - time_prev))
