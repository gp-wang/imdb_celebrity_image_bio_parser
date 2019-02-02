


from datetime import datetime


import requests
from bs4 import BeautifulSoup
import bs4
from pdb import set_trace as bp
import traceback


from sqlalchemy import and_
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import DateTime
from sqlalchemy import Float
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import VARCHAR
from sqlalchemy import CHAR
from sqlalchemy import BINARY
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session


from model import ImdbCelebrity
import model


# Constants ---------------------------------------



# Requests: ---------------------------------------

url_prefix = "https://www.imdb.com/name"

celebs = ['nm7939956']

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
    elem =  get_first_elment(soup, css_selector='div#name-bio-text > div > div')
    return get_elem_text(elem)
    

def get_avartar_image_url(soup):
    elem = get_first_elment(soup, css_selector='div#name-overview-widget #img_primary img')
    # bp()
    return get_elem_attr(elem, 'src')

def get_celeb(nconst):

    url = url_prefix + '/' + nconst
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    #bp()

    # print(get_bio(soup))
    # print(get_avartar_image_url(soup))
    celeb_record = ImdbCelebrity(nconst,
                  bio=get_bio(soup),
                  avartar_url=get_avartar_image_url(soup)
                  )
    
    
    return celeb_record





if __name__ == "__main__":
    nconst = celebs[0]
    db_engine = create_engine("sqlite:///imdb_celebrity.db")
    model.Base.metadata.create_all(db_engine)


    session = Session(db_engine)
    session.merge(get_celeb(nconst))
    session.commit()
    session.close()

