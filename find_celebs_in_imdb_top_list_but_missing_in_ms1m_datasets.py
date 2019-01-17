# prereq: sshfs mount and add openface to pythonpath
#         export PYTHONPATH=/mnt/sshfs/c4-openface:$PYTHONPATH
#         note: openface might be python2.7, you need to explore its usability under python3

# goal: find out out of the top 5k celeb (imdb rank), which are not included in our filtered ms1m dataset
#
# method: for entries(imdb_id) "results", which are not included by "links"
import traceback
from queue import Queue
import math
import shutil
import time
from pdb import set_trace as bp
import logging
import os
from threading import Thread, Lock
import multiprocessing
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor


# local modules gw
import cl_request
import utils
from utils import ImdbRecord, CombinedRecord
import azure_connector
import openface_align_utils



FORMAT = "[%(threadName)s, %(asctime)s, %(levelname)s] %(message)s"
logging.basicConfig(filename='logfile.log', level=logging.INFO, format=FORMAT)


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
            # logging.info(line)
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
    # with open(IN_MATCHED_PAIRS_OF_IMDB_TO_MS1M_FPATH, "r") as f:
    with open(matched_pairs_imdb_to_ms1m_fpath, "r") as f:
        for line in f.readlines():
            _imdb_id, msid = line.split()
            imdb_id = _imdb_id.strip(", ")
            imdb_ids_from_matched_links.add(imdb_id)

    logging.info("done reading matched links")

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
            logging.info(
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




class CelebImageService:
    def __init__(self, home_dir='.', nof_image_download_worker = 32, nof_image_align_worker = 8):
        self.home_dir = home_dir

        self.out_dl_root = os.path.join(self.home_dir, "dl_out")
        self.out_align_root = os.path.join(self.home_dir, "align_out")

        utils.recreate_dir_if_exist(self.out_dl_root)
        utils.recreate_dir_if_exist(self.out_align_root)
        
        self.img_dl_queue = multiprocessing.JoinableQueue()
        self.img_align_queue = multiprocessing.JoinableQueue()

        self.nof_image_download_worker = nof_image_download_worker
        self.nof_image_align_worker = nof_image_align_worker

    def download_images(self):
        while not self.img_dl_queue.empty():
            try:
                image_record = self.img_dl_queue.get()
                utils.download_one_image_record(image_record, dl_root_dir=self.out_dl_root)
                self.img_align_queue.put(image_record)
                logging.debug('downloaded 1 image for {}, {} remaining'.format(image_record.name, self.img_dl_queue.qsize()))
                self.img_dl_queue.task_done()
            except Queue.empty:
                logging.info('url Queue empty')
        return

    
    def align_and_resize_img_record(self, image_record):
        try: 
            openface_align_utils.align_image_record(image_record, input_root=self.out_dl_root, output_root=self.out_align_root)

        except Exception:
            logging.error("failed to align and resize {}".format(image_record))
            tb = traceback.format_exc()
        else:
            tb = None
        finally:
            if tb:
                print tb
            
            


    def align_images(self):
        while True:
            image_record = self.img_align_queue.get()
    
            if image_record:
                self.align_and_resize_img_record(image_record)
                self.img_align_queue.task_done()
                logging.info("alighed 1 mage for {}, {} remaining".format(image_record.name, self.img_align_queue.qsize()))
            else:
                self.img_align_queue.task_done()
                break
        return

    
        

    def start_processing(self):
        time_curr = time.perf_counter()
        imdb_results_in_famous_order = get_imdb_celebs_in_famous_order(
            IN_IMDB_TOP_CELEB_LIST_FPATH)
        logging.info("done reading parser results")

        missing_celeb_records = get_missing_celebs(
            imdb_results_in_famous_order, IN_MATCHED_PAIRS_OF_IMDB_TO_MS1M_FPATH)

        write_out_missing_celeb_list(
            missing_celeb_records, OUT_CELEB_LIST_MISSING_FROM_IMAGE_DATASET_FPATH)

        time_prev = time_curr
        time_curr = time.perf_counter()
        logging.info("done! finding missing celebs in {} secs".format(time_curr - time_prev))

        # --- calling azure image search API to get images urls --- (not downloading yet)
        logging.info("Start getting image urls ......")

        time_curr = time.perf_counter()

        flattened_list_of_celeb_image_record_list = get_flattened_img_url_list_for_downloading(
            missing_celeb_records)

        time_prev = time_curr
        time_curr = time.perf_counter()

        logging.info(
            "done getting image url list within {}".format(time_curr - time_prev))

        # --- downloading actual images --
        logging.info("Start downloading images for missing celebs ......")
        time_curr = time.perf_counter()

        for image_record in flattened_list_of_celeb_image_record_list:
            self.img_dl_queue.put(image_record)

        dl_worker_threads = [Thread(target=self.download_images) for _ in range(self.nof_image_download_worker)]

        for t in dl_worker_threads:
            t.start()

        # start image-aligning workers
        image_align_workers = [ multiprocessing.Process(target=self.align_images) for _ in range(self.nof_image_align_worker) ]

        for p in image_align_workers:
            p.start()

            
        
        # wait for all download jobs are exhausted in Queue
        self.img_dl_queue.join()

        # then put poison pills 
        for _ in range(self.nof_image_download_worker):
            self.img_dl_queue.put(None)


        # note: we still need manually join/stop image_align_workers even that poison pill will terminate them. Because we want main thread to wait here to declare entire process completion (end time counter)
        for p in image_align_workers:
            p.join()

            
        time_prev = time_curr
        time_curr = time.perf_counter()
        logging.info(
            "done downloading images list within {}".format(time_curr - time_prev))
        


if __name__ == "__main__":
    celeb_image_service = CelebImageService()

    celeb_image_service.start_processing()
