import requests
from bs4 import BeautifulSoup

num_to_get=5000
step = 249


def get_celebs(beg, step):
    page = requests.get('https://www.imdb.com/search/name?gender=male,female&count={}&start={}'.format(step, beg))
    soup = BeautifulSoup(page.text, 'html.parser')
    celeb_list_div=soup.select('div.lister-list')[0]
    celeb_list = celeb_list_div.select("div.lister-item.mode-detail > div.lister-item-content > h3.lister-item-header > a")
    celeb_tuple = [ (i + beg, anchor['href'][6:], anchor.text.strip(" \n"))  for i, anchor in enumerate(celeb_list)]

    return celeb_tuple


j = 1
results = []
while j < num_to_get:
    
    celebs_list = get_celebs(j, step)
    results = results + celebs_list
    print("parsed {} to {}".format(j, j + step))
    j = j + step

results = results[:num_to_get]    
print("done")

print("writing to file...")
with open("results", "w") as f:
    for celeb_tuple in results:
        print(celeb_tuple, file=f)
