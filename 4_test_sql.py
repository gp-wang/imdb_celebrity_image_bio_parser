"""Illustrate a many-to-many relationship between an
"Order" and a collection of "Item" objects, associating a purchase price
with each via an association object called "OrderItem"

The association object pattern is a form of many-to-many which
associates additional data with each association between parent/child.

The example illustrates an "order", referencing a collection
of "items", with a particular price paid associated with each "item".

"""

from datetime import datetime

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


class ImdbCelebrity(Base):
    __tablename__ = "imdb_celebrity"

    nconst = Column(CHAR(9), primary_key=True)
    bio = Column(VARCHAR(200))
    avartar = Column(BINARY)
    avartar_url = Column(VARCHAR(200))

    def __init__(self, nconst, bio, avartar):
        self.nconst = nconst
        self.bio = bio
        self.avartar = avartar


    def __repr__(self):
        return "ImdbCelebrity(%r, %r)" % (self.nconst, self.bio)
        #return "ImdbCelebrity(%r, %r, %r)" % (self.nconst, self.bio, self.avartar)  
        



if __name__ == "__main__":
    engine = create_engine("sqlite:///imdb_celebrity.db")
    Base.metadata.create_all(engine)

    session = Session(engine)

    # create catalog

    jack = ImdbCelebrity('nm1234568', 'a very famous celebrity', b'image data here')
    
    session.merge(jack)
    session.commit()

    # query the order, print items
    order = session.query(ImdbCelebrity).filter_by(nconst="nm1234568").one()
    print(order    )

    # # print customers who bought 'MySQL Crowbar' on sale
    # q = session.query(Order).join("order_items", "item")
    # q = q.filter(
    #     and_(Item.description == "MySQL Crowbar", Item.price > OrderItem.price)
    # )

    # print([order.customer_name for order in q])
    session.close()