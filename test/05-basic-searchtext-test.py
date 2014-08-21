import random
import time

import mango
import user_docs


def mkdb():
    return mango.Database("127.0.0.1", "5984", "mango_test")


def setup():
    user_docs.create_db_and_indexes()


def test_create_text_idx_01():
    db = mkdb()
    fields = ["foo", "bar"]
    analyzer = "standard"
    ret = db.create_text_index(fields, analyzer, name="text_idx_01")
    assert ret is True
    for idx in db.list_indexes():
        if idx["name"] != "text_idx_01":
            continue
        assert idx["def"]["fields"] == ["foo", "bar"]
        assert idx["type"] == "text"
        return
    raise AssertionError("index not created")
    

def test_create_text_no_sub_fields():
    db = mkdb()
    fields = [
        {
            "age": {
                "store": "true",
                "index": "false",
                "facet": "true"
            }
        }
    ]
    analyzer = "standard"
    ret = db.create_text_index(fields, analyzer, name="text_idx_02")
    assert ret is True
    for idx in db.list_indexes():
        if idx["name"] != "text_idx_02":
            continue
        assert idx["def"]["fields"] == [
        {
            "age": {
                "store": "true",
                "index": "false",
                "facet": "true"
            }
        }
    ]
        assert idx["type"] == "text"
        return
    raise AssertionError("index not created")


def test_create_text_sub_fields():
    db = mkdb()
    fields = [{
            "hometown": {
                "doc_fields" : ["location.state","location.city","location.address"],
                "store": "true",
                "index": "false",
                "facet": "true"
            }
        }]
    analyzer = "standard"
    ret = db.create_text_index(fields, analyzer, name="text_idx_03")
    assert ret is True
    for idx in db.list_indexes():
        if idx["name"] != "text_idx_03":
            continue
        assert idx["def"]["fields"] == [{
            "hometown": {
                "doc_fields" : ["location.state","location.city","location.address"],
                "store": "true",
                "index": "false",
                "facet": "true"
            }
        }]
        assert idx["type"] == "text"
        return
    raise AssertionError("index not created")


def test_create_text_with_analyzer():
    db = mkdb()
    fields = [{
            "names": {
                "doc_fields" : ["names.first","names.last"],
                "store": "true",
                "index": "false",
                "facet": "true"
            }
        }]
    analyzer =  [{ 
        "name": "perfield",
        "default": "english",
        "fields": {
            "names": "spanish"
        }
    }]
    ret = db.create_text_index(fields, analyzer, name="text_idx_04")
    assert ret is True
    for idx in db.list_indexes():
        if idx["name"] != "text_idx_04":
            continue
        assert idx["type"] == "text"
        assert idx["analyzer"] == [{ 
        "name": "perfield",
        "default": "english",
        "fields": {
            "names": "spanish"
            }
        }]
        assert idx["def"]["fields"] == [{
            "names": {
                "doc_fields" : ["names.first","names.last"],
                "store": "true",
                "index": "false",
                "facet": "true"
            }
        }]
        return
    raise AssertionError("index not created")