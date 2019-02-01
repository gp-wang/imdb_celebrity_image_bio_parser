url_prefix = "https://www.imdb.com/name"

celebs = ['nm7939956']


import requests
from bs4 import BeautifulSoup
import bs4
from pdb import set_trace as bp
import traceback

def get_first_elment(soup, css_selector):
    """element getter with safe wrappers

    param attribute:
    the attribute of the element to return

    """
    elem = None
    try:
        elem = soup.select(css_selector)[0]
    except IndexError as ie:
        tb = traceback.format_exc()
        print("empty selection list: get_bio")
    except AttributeError as ae:
        tb = traceback.format_exc()
        print("no .text attr: get_bio")
    else:
        tb = ""
    finally:
        if tb:
            print(tb)
        
    return elem


def get_elem_text(element):

    text = ""
    
    try:
        if element:
            text = element.text
    except AttributeError as ae:
        tb = traceback.format_exc()
        print("error getting text")
    else:
        tb = ""
    finally:
        if tb:
            print(tb)

    return text

def get_elem_attr(element, attr_name):
    attr_text = ""

    try:
        if element and element:
            attr_text = element[attr_name]
    except AttributeError as ae:
        tb = traceback.format_exc()
        print("error getting attribute")
    else:
        tb = ""
    finally:
        if tb:
            print(tb)

    return attr_text
    
            

def get_bio(soup):
    elem =  get_first_elment(soup, css_selector='div#name-bio-text > div > div', attribute='text')
    return get_elem_text(elem)
    

def get_avartar_image_url(soup):
    elem = get_first_elment(soup, css_selector='div#name-overview-widget #img_primary img')
    bp()
    return get_elem_attr(elem, 'src')

def get_celeb(celeb_nmid):

    
    url = url_prefix + '/' + celeb_nmid
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    #bp()

    # print(get_bio(soup))
    print(get_avartar_image_url(soup))
    
    
    #return page.text


if __name__ == "__main__":
    print(get_celeb(celebs[0]))
    print()
