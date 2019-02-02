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

IN_DB_NAME = 'imdb_celeb_parser_top_5k'
OUT_DB_NAME = 'imdb_celeb_parser'

# Requests: ---------------------------------------


DATASET = './name.basics.tsv'

# batch size for db saving
BATCH_SIZE = 1e3

if __name__ == "__main__":

    #in_db_engine = create_engine("sqlite:///imdb_top_5k.db")
    in_db_engine = create_engine("mysql+mysqldb://{user}:{pwd}@{host}:{port}/{dbname}".format(
        user=DB_USER,
        pwd=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        dbname=IN_DB_NAME
    ))

    # out_db_engine = create_engine("sqlite:///imdb_main.db")
    out_db_engine = create_engine("mysql+mysqldb://{user}:{pwd}@{host}:{port}/{dbname}".format(
        user=DB_USER,
        pwd=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        dbname=OUT_DB_NAME
    ))
    model.Base.metadata.create_all(out_db_engine)

    in_session = Session(in_db_engine)
    in_query = in_session.query(ImdbCelebrity)
    
    all_top5k_celebs = in_query.all()

    
    top5k_nconst_set = {celeb.nconst for celeb in all_top5k_celebs}
    in_session.close()
    
    out_session = Session(out_db_engine)


    cnt, skip_cnt = 0,0
    
    with open(DATASET, 'r') as f:
        
        header_line = f.readline()


        line = f.readline()
        while line:
            i = 0
            celeb_batch = []
            while i < BATCH_SIZE and line:
                nconst, primaryName, birthYear, deathYear, primaryProfession, knownForTitles = line.split('\t')
                #bp()

                if not nconst in top5k_nconst_set:
                    skip_cnt += 1
                    # continue
                else:
                    new_celeb = ImdbCelebrity(nconst=nconst)
                    if primaryName:
                        new_celeb.primaryName = primaryName

                    if birthYear and birthYear != '\\N':
                        new_celeb.birthYear = datetime(int(birthYear), 1, 1)

                    if deathYear and deathYear != '\\N':
                        new_celeb.deathYear = datetime(int(deathYear), 1, 1)

                    if primaryProfession:
                        new_celeb.primaryProfession = primaryProfession

                    if knownForTitles:
                        new_celeb.knownForTitles = knownForTitles

                    celeb_batch.append(new_celeb)
                    i += 1

                    
                    if i == len(top5k_nconst_set) - cnt:
                        print(" inner safe early stop now")
                        break

                print('skip:cnt %r : %r' % (skip_cnt, i))
                line = f.readline()
                

            for celeb in celeb_batch:
                out_session.merge(celeb)

            
            out_session.commit()
            cnt += len(celeb_batch)
            print("inserted %r" % cnt)

            if cnt == len(top5k_nconst_set):
                print("outer safe early stop now")
                break

    out_session.close()
    print("done %r" % cnt)

