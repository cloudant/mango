import random
import time

import mango
import user_docs


def mkdb():
    return mango.Database("127.0.0.1", "5984", "mango_test")


def setup():
    user_docs.create_db_and_indexes()

def test_create_text_simple():
    db = mkdb()
    fields = ["age","manager"]
    analyzer = "standard"
    ret = db.create_text_index(fields, analyzer,{},name="test_create_text_simple")
    assert ret is True
    for idx in db.list_indexes():
        if idx["name"] != "test_create_text_simple":
            continue
        assert idx["def"]["fields"] == [{
            "default": {
            "facet":False,
            "index":True,
            "doc_fields":["manager","age"],
            "store":False
            }
        },"age","manager"]
        assert idx["type"] == "text"
        return
    raise AssertionError("index not created")

def test_create_text_mixed():
    db = mkdb()
    fields = [{
            "age":{
                "store": True,
                "index": True,
                "facet": True
            }
        }, "manager"]
    analyzer = "standard"
    ret = db.create_text_index(fields, analyzer,{},name="test_create_text_mixed")
    assert ret is True
    for idx in db.list_indexes():
        if idx["name"] != "test_create_text_mixed":
            continue
        assert idx["def"]["fields"] == [{
            "default": {
            "facet":False,
            "index":True,
            "doc_fields":["manager","age"],
            "store":False
            }
        },{
            "age":{
                "store": True,
                "index": True,
                "facet": True
            }
        }, "manager"]
        assert idx["type"] == "text"
        return
    raise AssertionError("index not created")
    

def test_create_text_boolean():
    db = mkdb()
    fields = [
        {
            "manager": {
                "store": True,
                "index": False,
                "facet": True
            }
        }
    ]
    analyzer = "standard"
    ret = db.create_text_index(fields, analyzer,{}, name="text_idx_03")
    assert ret is True
    for idx in db.list_indexes():
        if idx["name"] != "text_idx_03":
            continue
        assert idx["def"]["fields"] == [{
            "default": {
            "facet":False,
            "index":True,
            "doc_fields":["manager"],
            "store":False,
            }
        },
        {
            "manager": {
                "store": True,
                "index": False,
                "facet": True
            }
        }
    ]
        assert idx["type"] == "text"
        return
    raise AssertionError("index not created")


def test_create_text_bool_number_fields():
    db = mkdb()
    fields = [
        { 
            "manager": {
                "store": True,
                "index": True,
                "facet": False
            }
        },
        {
            "age": {
                "store": True,
                "index": True,
                "facet": True
            }
        }
    ]
    analyzer = "standard"
    ret = db.create_text_index(fields, analyzer,{}, name="text_idx_bool_num")
    assert ret is True
    for idx in db.list_indexes():
        if idx["name"] != "text_idx_bool_num":
            continue
        assert idx["def"]["fields"] == [{
            "default": {
            "facet":False,
            "index":True,
            "doc_fields":["age","manager"],
            "store":False
            }
        },
        { 
            "manager": {
                "store": True,
                "index": True,
                "facet": False
            }
        },
        {
            "age": {
                "store": True,
                "index": True,
                "facet": True
            }
        }
    ]
        assert idx["type"] == "text"
        return
    raise AssertionError("index not created")

def test_create_text_array():
    db = mkdb()
    fields = [
        {
            "favorites": {
                "store": True,
                "index": True,
                "facet": True
            }
        }
    ]
    analyzer = "standard"
    ret = db.create_text_index(fields, analyzer,{}, name="text_idx_array_01")
    assert ret is True
    for idx in db.list_indexes():
        if idx["name"] != "text_idx_array_01":
            continue
        assert idx["def"]["fields"] == [{
            "default": {
            "facet":False,
            "index":True,
            "doc_fields":["favorites"],
            "store":False
            }
        },
        {
            "favorites": {
                "store": True,
                "index": True,
                "facet": True
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
                "store": True,
                "index": True,
                "facet": True
            }
        }, {"names": {
                "doc_fields" : ["name.first","name.last"],
                "store": True,
                "index": True,
                "facet": True
            }}]
    analyzer = "standard"
    ret = db.create_text_index(fields, analyzer,{}, name="text_sub_fields")
    assert ret is True
    for idx in db.list_indexes():
        if idx["name"] != "text_sub_fields":
            continue
        assert idx["def"]["fields"] ==  [{
            "default": {
                "doc_fields":["location.state","location.city","location.address","name.first","name.last"],
                "store":False,
                "facet":False,
                "index":True
            }
        },
            {"hometown": {
                "doc_fields" : ["location.state","location.city","location.address"],
                "store": True,
                "index": True,
                "facet": True
            }
        }, {"names": {
                "doc_fields" : ["name.first","name.last"],
                "store": True,
                "index": True,
                "facet": True
            }}]
        assert idx["type"] == "text"
        return
    raise AssertionError("index not created")


def test_create_text_with_analyzer():
    db = mkdb()
    fields = [{
        "names": {
            "doc_fields" : ["name.first","name.last"],
            "store": True,
            "index": False,
            "facet": True
            }
        }]
    analyzer =  [{
        "names": "perfield",
        "default": "english",
        "fields": {
            "names": "spanish"
        }
    }]
    ret = db.create_text_index(fields, analyzer,{}, name="text_idx_analyzer")
    assert ret is True
    for idx in db.list_indexes():
        if idx["name"] != "text_idx_analyzer":
            continue
        assert idx["type"] == "text"
        assert idx["def"]["analyzer"] == [{ 
        "names": "perfield",
        "default": "english",
        "fields": {
            "names": "spanish"
            }
        }]
        assert idx["def"]["fields"] == [{
        "default": {
            "doc_fields":["name.first","name.last"],
            "store":False,
            "facet":False,
            "index":True
            }
        },
        {
            "names": {
                "doc_fields" : ["name.first","name.last"],
                "store": True,
                "index": False,
                "facet": True
            }
        }]
        return
    raise AssertionError("index not created")


def test_create_text_with_selector_analyzer():
    db = mkdb()
    fields = [{
            "names": {
                "doc_fields" : ["name.first","name.last"],
                "store": True,
                "index": False,
                "facet": True
            }
        }]
    analyzer =  [{ 
        "names": "perfield",
        "default": "english",
        "fields": {
            "names": "spanish"
        }
    }]
    selector = {"age": {"$lt": 35}}

    ret = db.create_text_index(fields, analyzer, selector,name="text_with_selector_analyzer(")
    assert ret is True
    for idx in db.list_indexes():
        if idx["name"] != "text_with_selector_analyzer(":
            continue
        assert idx["type"] == "text"
        assert idx["def"]["analyzer"] == [{ 
        "names": "perfield",
        "default": "english",
        "fields": {
            "names": "spanish"
            }
        }]
        assert idx["def"]["fields"] == [{
        "default": {
            "doc_fields":["name.first","name.last"],
            "store":False,
            "facet":False,
            "index":True
            }
        },
        {
            "names": {
                "doc_fields" : ["name.first","name.last"],
                "store": True,
                "index": False,
                "facet": True
            }
        }]
        assert idx["def"]["selector"] == {"age": {"$lt": 35}}
        return
    raise AssertionError("index not created")