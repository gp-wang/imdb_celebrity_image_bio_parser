import time

# goal: find out out of the top 5k celeb (imdb rank), which are not included in our filtered ms1m dataset
#
# method: for entries(imdb_id) "results", which are not included by "links"

import os
from collections import namedtuple
import azure_parse
from threading import Thread
from multiprocessing import Process
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
            azure_parse.get_celeb_photo_by_name(celeb_record.ms1m_id, celeb_record.name)
        except Exception as inst:
            print("failed to parse {}, idx: {}".format(celeb_record, idx))
            print(type(inst))    # the exception instance
            print(inst.args)     # arguments stored in .args
            print(inst)
            continue


    end_time = time.perf_counter()
    duration = end_time - start_time
    sec_to_cool_down = max(1, PERIOD - duration)
    print("finished celeb at offset {} within {} sec".format( offset, duration))
    print("sleeping for {}".format(sec_to_cool_down))
    
    time.sleep(sec_to_cool_down)


NUM_RECORDS_PER_ = 8
threads = [Thread(target=parse_celebs, args=(combined_records[x: x + NUM_RECORDS_PER_THREAD], x)) for x in range(0, len(combined_records), NUM_RECORDS_PER_THREAD) ]

# for t in threads:
#     t.start()


# for t in threads:
#     t.join()

# use single thread    
# parse_celebs(combined_records[581:], 0)


         
          
