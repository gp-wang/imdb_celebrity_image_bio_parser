
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


Base = declarative_base()

#  Database: ----------------------------------------

class ImdbCelebrity(Base):
    __tablename__ = "imdb_celebrity"

    nconst = Column(CHAR(9), primary_key=True)
    bio = Column(VARCHAR(200))
    avartar = Column(BINARY)
    avartar_url = Column(VARCHAR(200))

    def __init__(self, nconst, bio=None, avartar=None, avartar_url=None):
        self.nconst = nconst
        self.bio = bio
        self.avartar = avartar
        self.avartar_url = avartar_url


    def __repr__(self):
        #return "ImdbCelebrity(%r, %r)" % (self.nconst, self.bio)
        return "ImdbCelebrity(%r, %r, %r)" % (self.nconst, self.bio, self.avartar_url)


