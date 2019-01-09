import time
from pdb import set_trace as bp
# goal: find out out of the top 5k celeb (imdb rank), which are not included in our filtered ms1m dataset
#
# method: for entries(imdb_id) "results", which are not included by "links"

import os
from collections import namedtuple
import azure_parse_multi as azure_parse
from threading import Thread
from multiprocessing import Process

import cl_request
TIMEOUT = 10
MAX_TRY = 5

ImdbRecord = namedtuple('ImdbRecord', ['id', 'name', 'rank'])

imdb_results_in_famous_order = []
with open("results", "r") as f:
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
with open("links", "r") as f:
     for line in f.readlines():
         _imdb_id, msid = line.split()
         imdb_id = _imdb_id.strip(", ")
         imdb_ids_from_matched_links.add(imdb_id)

print("done reading matched links")

to_be_added_in_famous_order = []

for imdb_record in imdb_results_in_famous_order:
    if not imdb_record.id in imdb_ids_from_matched_links:
        to_be_added_in_famous_order.append(imdb_record)

CombinedRecord = namedtuple('CombinedRecord', ['ms1m_id', 'imdb_id', 'name', 'rank'])

combined_records = []
with open("to_be_manually_added.txt", "w") as f:
    for idx,imdb_record in enumerate(to_be_added_in_famous_order):
        combined_record = CombinedRecord("m.{:08d}".format(idx), imdb_record.id, imdb_record.name, rank=imdb_record.rank)
        print('{}\t{}\t{}\t"{}"'.format(combined_record.rank, combined_record.ms1m_id, combined_record.imdb_id, combined_record.name), file=f)
        combined_records.append(combined_record)
        

print("done reverse filtering")




TPS_LIMIT = 250
PERIOD = 1 // TPS_LIMIT

def parse_celebs(celeb_records, offset):
    print("started celeb at offset {}".format( offset))
    start_time = time.perf_counter()
    for idx, celeb_record in enumerate(celeb_records):
        try:
             bp()
             azure_parse.get_celeb_photo_by_name(celeb_record.ms1m_id, celeb_record.name)
        except Exception as inst:
             print("failed to parse {}, idx: {}".format(celeb_record, idx))
             print(type(inst))    # the exception instance
             print(inst.args)     # arguments stored in .args
             print(inst)
             continue


    end_time = time.perf_counter()
    duration = end_time - start_time
    sec_to_cool_down = max(1, PERIOD - duration)  # at least wait for 1 sec 
    print("finished celeb at offset {} within {} sec".format( offset, duration))
    print("sleeping for {}".format(sec_to_cool_down))
    
    time.sleep(sec_to_cool_down)

def chunkIt(seq, num):
     # max: for case where num > len(seq)
    avg = max(1, len(seq) / float(num))
    out = []
    last = 0.0

    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg

    return out


TPS_LIMIT = 250

# NOF_THREADS = TPS_LIMIT - TPS_LIMIT * 0.2      # give a safe margin of 20%
NOF_THREADS = 8

celeb_records_sublists = chunkIt(combined_records[582:], NOF_THREADS)



threads = [Process(target=parse_celebs, args=(celeb_records_sublist,idx)) for idx,celeb_records_sublist in enumerate(celeb_records_sublists)]

for t in threads:
    t.start()


for t in threads:
    t.join()


         
          
