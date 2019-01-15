# checks the dir covers all people and image count is good



# -- readin the msid to name pairs for later useful




# -- below do the actual chking

from collections import namedtuple
import os

CHK_DIR = "img_first_443_people"

assert os.path.isdir(CHK_DIR), "chk_dir not exist"

PersonStat = namedtuple('PersonStat', ['name', 'image_count'])
tups = []

wanted_set = set(range(845))
actual_set = set()



for idx, class_dir in enumerate(os.listdir(CHK_DIR)):
    dir_path = os.path.join(CHK_DIR, class_dir)
    if not os.path.isdir(dir_path):
        continue

    person_idx = int(class_dir.split('.')[1])
    actual_set.add(person_idx)
    cnt = len(os.listdir(dir_path))
    tups.append(PersonStat(class_dir, cnt))


missing_set = wanted_set - actual_set
missing_list = sorted(missing_set)
    

tups = sorted(tups, key=lambda person_stat: person_stat.image_count)



for idx in missing_list:
    print("missing: {}".format(idx))

for tup in tups:
    print("{} has {} images".format(tup.name, tup.image_count))
    


