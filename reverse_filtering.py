# goal: find out out of the top 5k celeb (imdb rank), which are not included in our filtered ms1m dataset
#
# method: for entries(imdb_id) "results", which are not included by "links"

import os



dict_imdb_ids_from_parser = dict()
with open("results", "r") as f:
     content = f.readlines()

     for line in content:
          # print(line)
          line = line.strip('()\n')
          line = line.split(',')
          # index, imdb_id, full_name
          imdb_id, imdb_name = line[1].strip('\' '), line[2].strip('\' ')
          dict_imdb_ids_from_parser[imdb_id] = imdb_name

print("done reading parser results")          

          
imdb_ids_from_parser = set(dict_imdb_ids_from_parser.keys())


imdb_ids_from_matched_links = set()
with open("links", "r") as f:
     for line in f.readlines():
         _imdb_id, msid = line.split()
         imdb_id = _imdb_id.strip(", ")
         imdb_ids_from_matched_links.add(imdb_id)

print("done reading matched links")

to_be_added = imdb_ids_from_parser.difference(imdb_ids_from_matched_links)

with open("to_be_manually_added.txt", "w") as f:
    for imdb_id in to_be_added:
        print("{} {}".format(imdb_id, dict_imdb_ids_from_parser[imdb_id]), file=f)

print("done reverse filtering")
        
         
          
