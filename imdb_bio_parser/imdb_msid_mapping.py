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


from model import ImdbMsid
import model


# Constants ---------------------------------------


DB_USER = 'imdb_celeb_parser'
DB_PASSWORD = 'imdb_celeb_parser'
DB_HOST = 'localhost'
DB_PORT = '3306'
DB_NAME = 'imdb_celeb_parser'


IMDB_TO_MSID_MAPPING = '../sanitized_combined_msid_to_imdb_mapping_4k_02022019.txt'

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

    # create one table: https://stackoverflow.com/a/51780682/3701346
    model.Base.metadata.create_all(db_engine, tables=[ImdbMsid.__table__])


    session = Session(db_engine)

    cnt = 0
    with open(IMDB_TO_MSID_MAPPING, 'r') as f:

        # links file has no header
        # header_line = f.readline()


        line = f.readline()
        while line:
            i = 0
            celeb_batch = []
            while i < BATCH_SIZE and line:
                nconst, msid = line.strip('\n').split('\t')
                new_celeb = ImdbMsid(nconst, msid)
                
                celeb_batch.append(new_celeb)
                i += 1
                line = f.readline()

            session.add_all(celeb_batch)
            
            session.commit()
            cnt += len(celeb_batch)
            print("inserted {}".format(cnt))

    session.close()
    print("all done, %s" % cnt)

