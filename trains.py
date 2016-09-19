#!/usr/bin/env python

# trains.py
#
# This file implements an HTTP JSON API for ticket price data scraped
# from the National Rail website. It was made for educational purposes.
# You can view the project at https://github.com/mjwestcott/trains

import tornado.httpserver
import tornado.escape
import tornado.ioloop
import tornado.web

from sqlalchemy.orm import sessionmaker, joinedload
from sqlalchemy import create_engine
from sqlalchemy import asc, desc, cast, Date
from models import Fare, Journey
from dateutil.parser import parse


class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r'/', HelloHandler),
            (r'/v0/journeys(?:/(\d+))?', JourneysHandler),
            (r'/v0/fares(?:/(\d+))?', FaresHandler),
            (r"/static/(.*)", tornado.web.StaticFileHandler, {"path": "./static"}),
        ]
        super().__init__(handlers)

        # One global DB session across all handlers.
        engine = create_engine("postgresql://trains:trains@localhost/trains")
        Session = sessionmaker(bind=engine)
        self.db = Session()


class HelloHandler(tornado.web.RequestHandler):
    def get(self):
        self.render("template/index.html")


class BaseJSONHandler(tornado.web.RequestHandler):
    def initialize(self):
        self.db = self.application.db
        self.set_header('Content-Type', 'application/json')

        # Arguments used to filter Journeys.
        names = ['src', 'dest', 'changes']
        values = [self.get_argument(name, default=None) for name in names]
        self.journeyfilters = {k: v for (k, v) in zip(names, values) if v}

        # Arguments used to filter Fares.
        names = ['type', 'flex', 'perm', 'com', 'jid']
        values = [self.get_argument(name, default=None) for name in names]
        self.farefilters = {k: v for (k, v) in zip(names, values) if v}

        # Other arguments.
        self.args = {
            'limit': self.get_argument('limit', default=None),
            'date': self.get_argument('date', default=None),
            'format': self.get_argument('format', default=None),
        }

    def encode_fares(self, fares):
        return tornado.escape.json_encode([f.to_dict() for f in fares])

    def encode_journeys(self, journeys):
        return tornado.escape.json_encode([j.to_dict() for j in journeys])


class JourneysHandler(BaseJSONHandler):
    def get(self, jid=None):
        if jid:
            self.journeyfilters['jid'] = jid

        # Base query
        query = (self.db.query(Journey)
                        .filter_by(**self.journeyfilters)
                        .order_by(asc(Journey.departs)))

        # Filter by date if requested.
        if self.args['date']:
            date = parse(self.args['date']).date()
            query = query.filter(cast(Journey.departs, Date) == date)

        # Limit if requested.
        if self.args['limit']:
            query = query.limit(int(self.args['limit']))

        self.write(self.encode_journeys(query.all()))


class FaresHandler(BaseJSONHandler):
    def get(self, fid=None):
        if fid:
            self.farefilters['fid'] = fid

        # Base query
        query = (self.db.query(Fare)
                        .filter_by(**self.farefilters)
                        .join(Fare.journey)
                        .options(joinedload(Fare.journey))
                        .filter_by(**self.journeyfilters)
                        .order_by(asc(Journey.departs)))

        # Filter by date if requested.
        if self.args['date']:
            date = parse(self.args['date']).date()
            query = query.filter(cast(Journey.departs, Date) == date)

        # Limit if requested.
        if self.args['limit']:
            query = query.limit(int(self.args['limit']))

        self.write(self.encode_journeys(query.all()))


def main():
    http_server = tornado.httpserver.HTTPServer(Application(), xheaders=True)
    http_server.listen(8001)
    tornado.ioloop.IOLoop.current().start()

if __name__ == '__main__':
    main()
