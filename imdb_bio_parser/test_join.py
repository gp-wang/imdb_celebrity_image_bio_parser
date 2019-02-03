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

from collections import namedtuple


from model import ImdbCelebrity, ImdbMsid

from threading import Lock

from cl_request import proxied_get_https_url

# Constants ---------------------------------------
DB_BATCH_SIZE=64
MAX_TRIAL_CNT = 16
TIME_OUT = 20

# Requests: ---------------------------------------


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
    db_query = db_session.query(ImdbCelebrity, ImdbMsid)
    msid = 'm.01mqz0'
    imdb_celeb, msid = db_query.filter(ImdbMsid.msid == msid).filter(ImdbMsid.nconst == ImdbCelebrity.nconst).all()[0]

    print('imdb_celeb: {}'.format(imdb_celeb))
    
    db_session.close()


