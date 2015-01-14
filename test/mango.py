
import json
import time
import unittest
import uuid

import requests

import user_docs


def random_db_name():
    return "mango_test_" + uuid.uuid4().hex


class Database(object):
    def __init__(self, host, port, dbname, auth=None):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.sess = requests.session()
        if auth is not None:
            self.sess.auth = auth
        self.sess.headers["Content-Type"] = "application/json"

    @property
    def url(self):
        return "http://{}:{}/{}".format(self.host, self.port, self.dbname)

    def path(self, parts):
        if isinstance(parts, (str, unicode)):
            parts = [parts]
        return "/".join([self.url] + parts)

    def create(self, q=1, n=3):
        r = self.sess.get(self.url)
        if r.status_code == 404:
            r = self.sess.put(self.url, params={"q":q, "n": n})
            r.raise_for_status()

    def delete(self):
        r = self.sess.delete(self.url)

    def recreate(self):
        self.delete()
        time.sleep(1)
        self.create()
        time.sleep(1)

    def save_doc(self, doc):
        self.save_docs([doc])

    def save_docs(self, docs, **kwargs):
        body = json.dumps({"docs": docs})
        r = self.sess.post(self.path("_bulk_docs"), data=body, params=kwargs)
        r.raise_for_status()
        for doc, result in zip(docs, r.json()):
            doc["_id"] = result["id"]
            doc["_rev"] = result["rev"]

    def open_doc(self, docid):
        r = self.sess.get(self.path(docid))
        r.raise_for_status()
        return r.json()

    def ddoc_info(self, ddocid):
        r = self.sess.get(self.path([ddocid, "_info"]))
        r.raise_for_status()
        return r.json()

    def create_index(self, fields, idx_type="json", name=None, ddoc=None):
        body = {
            "index": {
                "fields": fields
            },
            "type": idx_type,
            "w": 3
        }
        if name is not None:
            body["name"] = name
        if ddoc is not None:
            body["ddoc"] = ddoc
        body = json.dumps(body)
        r = self.sess.post(self.path("_index"), data=body)
        r.raise_for_status()
        return r.json()["result"] == "created"

    def list_indexes(self):
        r = self.sess.get(self.path("_index"))
        r.raise_for_status()
        return r.json()["indexes"]

    def delete_index(self, ddocid, name, idx_type="json"):
        path = ["_index", ddocid, idx_type, name]
        r = self.sess.delete(self.path(path), params={"w":"3"})
        r.raise_for_status()

    def find(self, selector, limit=25, skip=0, sort=None, fields=None,
                r=1, conflicts=False):
        body = {
            "selector": selector,
            "limit": limit,
            "skip": skip,
            "r": r,
            "conflicts": conflicts
        }
        if sort is not None:
            body["sort"] = sort
        if fields is not None:
            body["fields"] = fields
        body = json.dumps(body)
        r = self.sess.post(self.path("_find"), data=body)
        r.raise_for_status()
        return r.json()["docs"]

    def find_one(self, *args, **kwargs):
        results = self.find(*args, **kwargs)
        if len(results) > 1:
            raise RuntimeError("Multiple results for Database.find_one")
        if len(results):
            return results[0]
        else:
            return None


class DbPerClass(unittest.TestCase):

    @classmethod
    def setUpClass(klass):
        klass.db = Database("127.0.0.1", "5984", random_db_name())
        klass.db.create(q=1, n=3)

    def setUp(self):
        self.db = self.__class__.db


class UserDocsTests(DbPerClass):

    @classmethod
    def setUpClass(klass):
        super(UserDocsTests, klass).setUpClass()
        user_docs.setup(klass.db)


class UserDocsTextTests(DbPerClass):

    DEFAULT_FIELD = None
    FIELDS = None

    @classmethod
    def setUpClass(klass):
        super(UserDocsTextTests, klass).setUpClass()
        user_docs.setup(
                klass.db,
                index_type="text",
                default_field=klass.DEFAULT_FIELD,
                fields=klass.FIELDS
            )


class FriendDocsTextTests(DbPerClass):

    @classmethod
    def setUpClass(klass):
        super(FriendDocsTextTests, klass).setUpClass()
        friend_docs.setup(klass.db, index_type="text")
