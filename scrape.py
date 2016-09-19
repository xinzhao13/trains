#!/usr/bin/env python
import sys
import bs4
import time
import random
import hashlib
import requests
import datetime as dt
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from collections import namedtuple
from models import Journey, Fare


DATE_FMT = "%d%m%y"
DATETIMES = [dt.datetime.today() + dt.timedelta(days=i) for i in range(1, 91)]
DATES = [d.strftime(DATE_FMT) for d in DATETIMES]
TIMES = ["{:02}00".format(x) for x in range(25)]


BASE = "http://ojp.nationalrail.co.uk/service/timesandfares/"
TMPL = BASE+"{src}/{dest}/{date}/{time}/dep"
# e.g. TMPL.format(src="PAD", dest="SAU", date="010316", time="1200")


Request = namedtuple("Request", ["url", "date"])


def build_reqs(src, dest):
    """Make list of Request objects. e.g. build_req('PAD', 'SAU')"""
    return [Request(TMPL.format(src=src, dest=dest, date=d, time=t), d)
            for d in DATES           # Every day from today to 90 days ahead
            for t in TIMES[::3]]     # Every three hours (rudimentary paging)


def process(req, delay):
    resp = requests.get(req.url)
    # It's unclear what, if any, rate limiting the National Rail have.
    # TODO: investigate speeding this up, maybe using Tornado's async HTTP client.
    time.sleep(random.random() * delay)
    if resp.status_code == 200:
        soup = bs4.BeautifulSoup(resp.text, "html.parser")
        contains_mtx = lambda cssclass: cssclass is not None and "mtx" in cssclass
        data = list(filter(bool, (parse(tag, req.date)
                                  for tag in soup.find_all(class_=contains_mtx))))
        # Requests made e.g. for 01/01/2016 at 2100 may return journeys departing
        # 0100 on 02/01/2016. This is our opportunity to discover it.
        if data:
            # Note: we assume first_depart represents a departure datetime on
            # the day requested in the URL. This may be false: if there are no
            # trains on a particular day, the system may return those for the
            # next day. So far this has not happened.
            first_depart = data[0][0]["departs"]
        for journey, fare in data:
            if journey["departs"].hour < first_depart.hour:
                # Journey has spilled over into the next day.
                journey["departs"] += dt.timedelta(days=1)
                journey["arrives"] += dt.timedelta(days=1)
                journey["duration"] = journey["arrives"] - journey["departs"]
                journey["hash"] = makehash(journey)
            yield journey, fare


def parse(tag, date):
    """Take a tag whose class contains "mtx" and return dicts representing a
    journey and fare, or False in case of error (such as html not as expected)."""
    # Search for the data.
    fb = tag.findChild(class_="fare-breakdown")
    jb = tag.findChild(class_="journey-breakdown")
    if fb is None or jb is None:
        return False
    j = jb.findChild("input")["value"].split("|")
    f = fb.findChild("input")["value"].split("|")

    # Parse the data.
    date = date          # "010316" == 1st of March 2016 (from our request URL)
    src = j[1]           # "PAD"
    src_name = j[0]      # "London Paddington"
    dest = j[4]          # "SAU"
    dest_name = j[3]     # "St Austell"
    departs = j[2]       # "12:00"
    arrives = j[5]       # "16:49"
    changes = j[8]       # "2"
    tickettype = f[3]    # "Super Off-Peak Single" or "Advance (Standard Class)"
    price = f[5]         # "60.00"
    com = f[10]          # "GWA"
    com_name = f[11]     # "Great Western Railway"
    flexibility = f[16]  # "FLEXIBLE" or "RESTRICTED"
    permission = f[15]   # "ANY PERMITTED" or "GREAT WESTN ONLY"

    year, month, day = int(2000+int(date[4:])), int(date[2:4]), int(date[:2])
    dep_hour, dep_minute = int(departs[:2]), int(departs[3:])
    arr_hour, arr_minute = int(arrives[:2]), int(arrives[3:])

    departs = dt.datetime(year, month, day, dep_hour, dep_minute)
    arrives = dt.datetime(year, month, day, arr_hour, arr_minute)

    # Make sure day of arrival is correct.
    if arr_hour < dep_hour:
        arrives += dt.timedelta(days=1)

    journey = {
        "src": src,
        "src_name": src_name,
        "dest": dest,
        "dest_name": dest_name,
        "departs": departs,
        "arrives": arrives,
        "duration": arrives - departs,
        "changes": int(changes),
    }

    # Add a hash field to uniquely identify this journey.
    journey["hash"] = makehash(journey)

    fare = {
        "price": float(price),
        "com": com,
        "com_name": com_name,
        "type": tickettype,
        "flex": flexibility,
        "perm": permission,
        "timestamp": dt.datetime.now(),
    }

    # dicts to be transformed into Journey and Fare sqlalchemy objects.
    return journey, fare


def makehash(journey):
    """Take a journey dict, and return a hash of the fields."""
    keys = ["src", "dest", "changes", "departs", "arrives"]
    as_str = "".join(str(journey[key]) for key in keys)
    return hashlib.sha1(as_str.encode("utf-8")).hexdigest()


def scrape(src, dest, delay=2):
    engine = create_engine('postgresql://trains:trains@localhost/trains')
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    # Add all discovered Journeys and Fares to the database.
    for req in build_reqs(src, dest):
        print("\nprocessing", req)
        for journey, fare in process(req, delay):
            print("scraped {}".format(journey['departs']), end="")
            existing = (session.query(Journey)
                               .filter(Journey.hash == journey['hash'])
                               .one_or_none())
            if not existing:
                # First time we've seen this journey.
                j = Journey(**journey)
                f = Fare(**fare, journey=j)
                session.add(j)
                session.add(f)
                session.commit()
                print("; added {} (new journey)".format(f.price))
            else:
                # Only add this fare if no fares added to this journey in 23h.
                fares = (session.query(Fare)
                                .join(Fare.journey)
                                .filter(Journey.jid == existing.jid)
                                .all())
                now = dt.datetime.now()
                mostrecent = min(fares, key=lambda x: abs(x.timestamp - now))
                if abs(mostrecent.timestamp - now) > dt.timedelta(hours=23):
                    f = Fare(**fare, journey=existing)
                    session.add(f)
                    session.commit()
                    print("; added {}".format(f.price))
                else:
                    print("; seen at: {}; {} ago".format(
                        mostrecent.timestamp, abs(mostrecent.timestamp - now)))
    session.close()

if __name__ == "__main__":
    # Example usage: python scrape.py PAD SAU
    scrape(*sys.argv[1:])
