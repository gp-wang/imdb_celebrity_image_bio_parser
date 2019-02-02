import concurrent.futures
from concurrent.futures import ThreadPoolExecutor


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
from threading import Lock

from cl_request import proxied_get_https_url

# Constants ---------------------------------------
DB_BATCH_SIZE=64
MAX_TRIAL_CNT = 16
TIME_OUT = 20

# Requests: ---------------------------------------

url_prefix = "https://www.imdb.com/name"

celebs = ['nm7939956']

total_cnt_lock = Lock()
total_cnt = 0

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

def populate_celeb_info(celeb_record):
    """
    populate bio, and avartar_url of the celeb
    
    param: celeb
    ImdbCelebrity
    
    """
    nconst = celeb_record.nconst

    url = url_prefix + '/' + nconst
    page = requests.get(url)
    # page = proxied_get_https_url(url, TIME_OUT)
    soup = BeautifulSoup(page.text, 'html.parser')

    # gw: actually this cnt is any parse trial, ignore "failure"
    parse_failure_cnt = celeb_record.parse_failure_cnt + 1
    #bp()

    # print(get_bio(soup))
    # print(get_avartar_image_url(soup))
    celeb_record.bio=get_bio(soup)
    celeb_record.avartar_url=get_avartar_image_url(soup)
    celeb_record.parse_failure_cnt = parse_failure_cnt

    
    # print("parsed %r" % celeb_record)
    # cnt = 0
    # global total_cnt_lock, total_cnt
    # try:
    #     total_cnt_lock.acquire()
    #     total_cnt += 1
    #     cnt = total_cnt
    # finally:
    #     total_cnt_lock.release()

    # print("parsed %r" % cnt)
    return celeb_record

# gw: db session is not thread safe
# def populate_and_save_celeb_info(celeb_record, db_session):
#     print("submitted one task: %r" % celeb_record)
#     db_session.merge(populate_celeb_info(celeb_record))
#     db_session.commit()
#     print("commited %r" % celeb_record)


DB_USER = 'imdb_celeb_parser'
DB_PASSWORD = 'imdb_celeb_parser'
DB_HOST = 'localhost'
DB_PORT = '3306'
DB_NAME = 'imdb_celeb_parser'

if __name__ == "__main__":
    
    # db_engine = create_engine("sqlite:///imdb_main.db")
    db_engine = create_engine("mysql+mysqldb://{user}:{pwd}@{host}:{port}/{dbname}".format(
        user=DB_USER,
        pwd=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME
    ))
    db_session = Session(db_engine)
    db_query = db_session.query(ImdbCelebrity)

    # TODO: filter by not parsed yet
    # all_top5k_celeb_records = db_query.all()
    
    all_top5k_celeb_records = db_query.filter(and_(ImdbCelebrity.avartar_url == None, ImdbCelebrity.bio == None, ImdbCelebrity.parse_failure_cnt < MAX_TRIAL_CNT)).all()

    print("starting... %r records needs to parse" % len(all_top5k_celeb_records))
    futures = []

    cnt_commited = 0
    with ThreadPoolExecutor(max_workers=8) as executor:

        # for celeb_record in all_top5k_celeb_records:
        #     executor.submit(populate_and_save_celeb_info, celeb_record, db_session)

        for celeb_record in all_top5k_celeb_records[:]:
            #future = executor.submit(populate_and_save_celeb_info, celeb_record, db_session)
            future = executor.submit(populate_celeb_info, celeb_record)
            futures.append(future)
            # populate_and_save_celeb_info(celeb_record, db_session)


        i = 0
        for completed_future in concurrent.futures.as_completed(futures):
            tb = ""
            try:
                #print("try 1")
                updated_celeb_record = completed_future.result()
                db_session.merge(updated_celeb_record)
                i += 1
            except Exception as e:
                tb = traceback.format_exc()
                
            else:
                tb = ""
            finally:
                if tb:
                    print(tb)

            if i == DB_BATCH_SIZE:
                db_session.commit()
                cnt_commited += i
                
                i = 0
                print("commited %r out of %r " % (cnt_commited, len(all_top5k_celeb_records)))

        # last batch
        db_session.commit()
        cnt_commited += i
        i = 0
        print("commited %r out of %r " % (cnt_commited, len(all_top5k_celeb_records)))
        
    # concurrent.futures.wait(futures)
    # updated_celeb_records = [future.result() for future in futures]

    # for celeb_record in updated_celeb_records:
    #     db_session.merge(celeb_record)
    # db_session.commit()
    
    db_session.close()
    print("done %r" % len(cnt_commited))

