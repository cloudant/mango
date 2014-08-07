import mango
import user_docs

class SortTests(mango.UserDocsTextTests):

    def test_number_sort(self):
        q = {"age": {"$gt": 0}}
        docs = self.db.find(q, sort=["age:number"])
        assert len(docs) == 15
        assert docs[0]["age"] == 22

    def test_string_sort(self):
        q = {"email": {"$gt": None}}
        docs = self.db.find(q, sort=["email:string"])
        assert len(docs) == 15
        assert docs[0]["email"] == "abbottwatson@talkola.com"

    def test_notype_sort(self):
        q = {"email": {"$gt": None}}
        try:
            self.db.find(q, sort=["email"])
        except Exception, e:
            assert e.response.status_code == 400
        else:
            raise AssertionError("Should have thrown error for sort")

    def test_array_sort(self):
        q = {"favorites": {"$exists": True}}
        docs = self.db.find(q, sort=["favorites.[]:string"])
        assert len(docs) == 15
        assert docs[0]["user_id"] == 8

    def test_multi_sort(self):
        q = {"name": {"$exists": True}}
        docs = self.db.find(q, sort=["name.last:string", "age:number"])
        assert len(docs) == 15
        assert docs[0]["name"] == {"last":"Ewing","first":"Shelly"}
        assert docs[1]["age"] == 22

    def test_guess_type_sort(self):
        q = {"$or": [{"age":{"$gt": 0}}, {"email": {"$gt": None}}]}
        docs = self.db.find(q, sort=["age"])
        assert len(docs) == 15
        assert docs[0]["age"] == 22

    def test_guess_dup_type_sort(self):
        q = {"$and": [{"age":{"$gt": 0}}, {"email": {"$gt": None}},
            {"age":{"$lte": 100}}]}
        docs = self.db.find(q, sort=["age"])
        assert len(docs) == 15
        assert docs[0]["age"] == 22

    def test_ambiguous_type_sort(self):
        q = {"$or": [{"age":{"$gt": 0}}, {"email": {"$gt": None}},
            {"age": "34"}]}
        try:
            self.db.find(q, sort=["age"])
        except Exception, e:
            assert e.response.status_code == 400
        else:
            raise AssertionError("Should have thrown error for sort")

    def test_guess_multi_sort(self):
        q = {"$or": [{"age":{"$gt": 0}}, {"email": {"$gt": None}},
            {"name.last": "Harvey"}]}
        docs = self.db.find(q, sort=["name.last", "age"])
        assert len(docs) == 15
        assert docs[0]["name"] == {"last":"Ewing","first":"Shelly"}
        assert docs[1]["age"] == 22

    def test_guess_mix_sort(self):
        q = {"$or": [{"age":{"$gt": 0}}, {"email": {"$gt": None}},
            {"name.last": "Harvey"}]}
        docs = self.db.find(q, sort=["name.last:string", "age"])
        assert len(docs) == 15
        assert docs[0]["name"] == {"last":"Ewing","first":"Shelly"}
        assert docs[1]["age"] == 22
