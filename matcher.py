import util


with open("results", "r") as f:
     content = f.readlines()
     ctups = []
     for line in content:
          # print(line)
          line = line.strip('()\n')
          line = line.split(',')
          # index, imdb_id, full_name
          im_id, im_name = line[1].strip('\' '), line[2].strip('\' ')
          norm_im_name = util.normalize(im_name)
          im_gross_name = util.name_grosser(norm_im_name)
          im_num_token = len(norm_im_name.split(" "))

          ctups.append((im_id, im_name, norm_im_name, im_gross_name, im_num_token))

# for tup in ctups:
#      print(tup)

print(ctups[0])

ms_list = []     
with open("ms1m.list", "r") as f:
     content = f.readlines()
     for line in content:
          line = line[:-4]      # don't forget to count in the ending \n
          line = line.split("\t")
          
          #print(line)
          ms_id = line[0].strip()
          ms_name = line[1].strip(" \"")
          norm_ms_name = util.normalize(ms_name)
          gross_name = util.name_grosser(norm_ms_name)
          num_token = len(norm_ms_name.split(" "))
          
          ms_list.append((ms_id, ms_name, norm_ms_name, gross_name, num_token))

# for i in range(100)          :
#      print(ms_list[i])

# record visited name, for recording dup purpose
visited = {}
# index from normalized orig name to msid
index_orig_norm = {}
# index (only for length of 2 names)
index_2 = {}
# index 2 reversed, e.g. Jack Ma, Ma Jack
index_2_rev = {}
# index for name with multi seg
index_multi = {}


# !!!!**** what if rep name? two Jack Ma?
# ans: lets manualy sort them out 
for tup in ms_list:
     ms_id, ms_name, norm_ms_name, gross_name, num_token = tup
     if norm_ms_name in visited:
          if not visited[norm_ms_name]:
               visited[norm_ms_name] = set()

          visited[norm_ms_name].add(ms_id)
          continue

     visited[norm_ms_name]= None
     index_orig_norm[norm_ms_name] = ms_id

     if num_token == 2:
          index_2[norm_ms_name] = ms_id
          index_2_rev[util.flip(norm_ms_name)] = ms_id # TODO, flip

     elif num_token > 2:
          index_multi[tuple(sorted(gross_name.items()))] = ms_id

     
     
          
stat = dict(orig=0, two_parts=0, multi=0)
# linking imdb to ms1m
link = {}
for tup in ctups:
     im_id, im_name, norm_im_name, im_gross_name, num_token = tup
     if norm_im_name in index_orig_norm:
          
          link[im_id]  = index_orig_norm[norm_im_name]
          stat['orig'] = stat['orig'] + 1
     else:
          if num_token == 2:
               if norm_im_name in index_2:
                    link[im_id]  = index_2[norm_im_name]
                    stat['two_parts'] = stat['two_parts'] + 1
               elif norm_im_name in index_2_rev:
                    link[im_id]  = index_2_rev[norm_im_name]
                    stat['two_parts'] = stat['two_parts'] + 1
          elif num_token > 2:
               multi_key = tuple(sorted(im_gross_name.items()))
               if multi_key in index_multi:
                    link[im_id]  = index_multi[multi_key]
                    stat['multi'] = stat['multi'] + 1


with open("links", "w") as f:
     for k,v in link.items():
          print("{},\t{}".format(k,v), file=f)


print("done! stats:")
for k,v in stat.items():
     print("{}: {}".format(k, v))
