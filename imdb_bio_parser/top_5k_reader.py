# read in data from imdb official dataset: 


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


DB_USER = 'imdb_celeb_parser'
DB_PASSWORD = 'imdb_celeb_parser'
DB_HOST = 'localhost'
DB_PORT = '3306'
DB_NAME = 'imdb_celeb_parser_top_5k'


TOP_5K_LIST = '../links'

# batch size for db saving
BATCH_SIZE = 256



if __name__ == "__main__":

    db_engine = create_engine("mysql+mysqldb://{user}:{pwd}@{host}:{port}/{dbname}".format(
        user=DB_USER,
        pwd=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME
    ))
    model.Base.metadata.create_all(db_engine)


    session = Session(db_engine)

    cnt = 0
    with open(TOP_5K_LIST, 'r') as f:

        # links file has no header
        # header_line = f.readline()


        line = f.readline()
        while line:
            i = 0
            celeb_batch = []
            while i < BATCH_SIZE and line:
                nconst, msid = line.split('\t')
                nconst = nconst.strip(',')
                #bp()
                new_celeb = ImdbCelebrity(nconst=nconst)
                
                celeb_batch.append(new_celeb)
                i += 1
                line = f.readline()

            session.add_all(celeb_batch)
            
            session.commit()
            cnt += len(celeb_batch)
            print("inserted {}".format(cnt))

    session.close()
    print("all done, %s" % cnt)

