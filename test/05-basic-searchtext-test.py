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
    ret = db.create_text_index(fields, analyzer,{}, name="text_boolean")
    assert ret is True
    for idx in db.list_indexes():
        if idx["name"] != "text_boolean":
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


def test_create_text_mixed():
    db = mkdb()
    fields = [{
            "age":{
                "store": True,
                "index": True,
                "facet": True
            }
        }, "manager","twitter"]
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
            "doc_fields":["twitter","manager","age"],
            "store":False
            }
        },{
            "age":{
                "store": True,
                "index": True,
                "facet": True
            }
        }, "manager","twitter"]
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

def test_search_simple_number():
    db = mkdb()
    fields = ["age","manager"]
    analyzer = "standard"
    docs = db.find({"$text":"age:61"},fields=fields)
    assert len(docs) == 4
    assert docs[3]["doc"]["age"] == 61

def test_search_num_AND_boolean():
    db = mkdb()
    fields = ["age","manager"]
    analyzer = "standard"
    docs = db.find({"$text":"age:61 AND manager:false"},fields=fields)
    assert len(docs) == 4
    assert docs[3]["doc"]["manager"] == False
    assert docs[3]["doc"]["age"] == 61


def test_search_simple_boolean():
    db = mkdb()
    fields = ["manager"]
    analyzer = "standard"
    docs = db.find({"$text":"manager:false"},fields=fields)
    print len(docs)
    assert len(docs) == 7
    assert docs[6]["doc"]["manager"] == False

def test_search_num_boolean_string():
    db = mkdb()
    fields = ["age","manager","twitter"]
    analyzer = "standard"
    docs = db.find({"$text":"age:45 manager:false twitter:@stephaniekirkland"},fields=fields)
    assert len(docs) == 7
    assert docs[3]["doc"]["manager"] == False


def test_search_index_array_to_string():
    db = mkdb()
    fields = ["favorites"]
    analyzer = "standard"
    docs = db.find({"$text":"favorites:Lisp"}, fields=fields)
    print len(docs)
    assert len(docs) == 12
    assert docs[3]["doc"]["favorites"] == ["Lisp","Lisp"]

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

def test_search_sub_field_01():
    db = mkdb()
    fields = ["hometown","names"]
    analyzer = "standard"
    docs = db.find({"$text":"hometown:\"Woodhull Street\""}, fields=fields)
    assert len(docs) == 4
    assert docs[3]["doc"]["location"] == {"city":"Corinne","state":"Georgia","address":{"street":"Woodhull Street","number":6845}}

def test_search_sub_field_02():
    db = mkdb()
    fields = ["names","hometown"]
    analyzer = "standard"
    docs = db.find({"$text":"hometown:\"Avenue\" names:B*"}, fields=fields)
    assert len(docs) == 9
    assert docs[3]["doc"]["name"]=={"last":"Kirkland","first":"Stephanie"}

#This test depends on the indexes created, may fail if indexes created above change
def test_default_search():
    db = mkdb()
    analyzer = "standard"
    docs = db.find({"$text":"@whitleyharvey false AND \"78\""})
    assert len(docs) == 4
    assert docs[3]["doc"]["twitter"]=="@whitleyharvey"
    assert docs[3]["doc"]["age"]==78
    assert docs[3]["doc"]["manager"]==False

def test_search_pagination():
    db = mkdb()
    fields = ["favorites"]
    analyzer = "standard"
    docs = db.find({"$text":"favorites:Lisp"}, fields=fields,limit=1)
    assert len(docs) == 4
    assert docs[3]["doc"]["user_id"] == 5
    bookmark = docs[2]["bookmark"]
    docs = db.find({"$text":"favorites:Lisp", 
        "$options": {"$bookmark" : bookmark}
        }, fields=fields,limit=1)
    assert len(docs) == 4
    assert docs[3]["doc"]["user_id"] == 14

def test_search_counts():
    db = mkdb()
    fields = ["favorites"]
    analyzer = "standard"
    docs = db.find({"$text":"favorites:Lisp", 
        "$options": {"$counts" : ["favorites"]}
        }, fields=fields,limit=1)
    assert len(docs) == 4
    assert docs[0]["counts"] != {}
    assert docs[3]["doc"]["user_id"] == 5

def test_search_ranges():
    db = mkdb()
    fields = ["age"]
    analyzer = "standard"
    docs = db.find({"$text":"*:*", 
        "$options": {"$ranges" : {"age":{"young":"[0 TO 30]","expensive":"{30 TO Infinity}"}}}
        }, fields=fields)
    assert docs[1]["ranges"] == {"age":{"expensive":14.0,"young":1.0}}

def test_search_sort_and_store():
    db = mkdb()
    fields = ["age"]
    analyzer = "standard"
    docs = db.find({"$text":"*:*"}, fields=fields,sort=["age"])
    assert docs[3]["fields"] == {"age":31.0}
    assert docs[3]["doc"]["age"] == 31


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
        }, "age"]
    analyzer =  [{ 
        "names": "perfield",
        "default": "english",
        "fields": {
            "names": "spanish"
        }
    }]
    selector = {"age": {"$lt": 10}}

    ret = db.create_text_index(fields, analyzer, selector,name="text_with_selector_analyzer")
    assert ret is True
    for idx in db.list_indexes():
        if idx["name"] != "text_with_selector_analyzer":
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
            "doc_fields":["age","name.first","name.last"],
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
        },"age"]
        assert idx["def"]["selector"] == {"age": {"$lt": 10}}
        return
    raise AssertionError("index not created")

# This should return 0 documents because  our index named text_with_selector_analyzer has a selector filter
# that filters age < 10. No documents contain age < 10.
def test_search_selector():
    db = mkdb()
    fields = ["age","names"]
    analyzer = "standard"
    docs = db.find({"$text":"age:22"},fields=fields)
    assert len(docs) == 3
