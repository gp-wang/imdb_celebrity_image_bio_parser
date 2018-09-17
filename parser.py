import requests
from bs4 import BeautifulSoup
page = requests.get('https://www.imdb.com/search/name?gender=male,female&count=249&start=250')
soup = BeautifulSoup(page.text, 'html.parser')
celeb_list_div=soup.select('div.lister-list')[0]
celeb_list = celeb_list_div.select("div.lister-item.mode-detail > div.lister-item-content > h3.lister-item-header > a")
celib_tuple = [ (i, anchor['href'][6:], anchor.text.strip(" \n"))  for i, anchor in enumerate(celeb_list)]
for t in celib_tuple:
    print(t)

    
