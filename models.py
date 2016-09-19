# models.py
#
# SQLAlchemy classes for the ticket price data scraped from the
# National Rail website. For the full project, see
# https://github.com/mjwestcott/trains.

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import Integer, Float, String, Date, DateTime, Interval
from sqlalchemy import Column, ForeignKey
from sqlalchemy import create_engine
import datetime as dt

Base = declarative_base()

class Journey(Base):
    __tablename__ = 'journey'

    jid       = Column(Integer, primary_key=True)
    hash      = Column(String)
    departs   = Column(DateTime)
    arrives   = Column(DateTime)
    duration  = Column(Interval)
    src       = Column(String)
    src_name  = Column(String)
    dest      = Column(String)
    dest_name = Column(String)
    changes   = Column(Integer)

    def to_dict(self):
        """Return a dict suitable for JSON encoding."""
        return {
            "jid": self.jid,
            "hash": self.hash,
            "departs": str(self.departs),   # "%Y-%m-%d %H:%M:%S"
            "arrives": str(self.arrives),   # "%Y-%m-%d %H:%M:%S"
            "duration":
                {"seconds": self.duration.total_seconds()},
            "source":
                {"code": self.src,
                 "name": self.src_name},
            "destination":
                {"code": self.dest,
                 "name": self.dest_name},
            "changes": self.changes,
            "fares": [fare.to_dict_short() for fare in self.fares],
        }

class Fare(Base):
    __tablename__ = 'fare'

    fid       = Column(Integer,  primary_key=True)
    jid       = Column(Integer,  ForeignKey('journey.jid'))
    journey   = relationship(Journey, backref="fares")
    price     = Column(Float)
    com       = Column(String)
    com_name  = Column(String)
    type      = Column(String)
    flex      = Column(String)
    perm      = Column(String)
    timestamp = Column(DateTime)

    def to_dict(self):
        """Return a dict suitable for JSON encoding."""
        return {
            "fid": self.fid,
            "price": self.price,
            "type": self.type,
            "timestamp": str(self.timestamp),   # "%Y-%m-%d %H:%M:%S"
            "journey":
                {"jid": self.jid,
                 "departs": str(self.journey.departs),
                 "arrives": str(self.journey.arrives),
                 "source": self.journey.src,
                 "destination": self.journey.dest},
        }

    def to_dict_short(self):
        """Return a dict suitable for JSON encoding. Short version."""
        return {
            "fid": self.fid,
            "price": self.price,
            "type": self.type,
            "timestamp": str(self.timestamp),
        }

    @property
    def departdelta(self):
        return (self.journey.departs - self.timestamp).days

if __name__ == '__main__':
    # Create all tables in the engine. This is equivalent to "CREATE TABLE"
    # statements in raw SQL.
    engine = create_engine('postgresql://trains:trains@localhost/trains')
    Base.metadata.create_all(engine)
