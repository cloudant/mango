
import mango


class OperatorTests(mango.UserDocsTests):

    def test_all(self):
        docs = self.db.find({
                "manager": True,
                "favorites": {"$all": ["Lisp", "Python"]}
            })
        print docs
        assert len(docs) == 4
        assert docs[0]["user_id"] == 2
        assert docs[1]["user_id"] == 12
        assert docs[2]["user_id"] == 9
        assert docs[3]["user_id"] == 14

    def test_all_non_array(self):
        docs = self.db.find({
                "manager": True,
                "location": {"$all": ["Ohai"]}
            })
        assert len(docs) == 0

    def test_elem_match(self):
        emdocs = [
            {
                "user_id": "a",
                "bang": [{
                    "foo": 1,
                    "bar": 2
                }]
            },
            {
                "user_id": "b",
                "bang": [{
                    "foo": 2,
                    "bam": True
                }]
            }
        ]
        self.db.save_docs(emdocs, w=3)
        docs = self.db.find({
            "_id": {"$gt": None},
            "bang": {"$elemMatch": {
                "foo": {"$gte": 1},
                "bam": True
            }}
        })
        print docs
        assert len(docs) == 1
        assert docs[0]["user_id"] == "b"

    def test_in_operator_array(self):
        docs = self.db.find({
                "manager": True,
                "favorites": {"$in": ["Ruby", "Python"]}
            })
        assert len(docs) == 7
        assert docs[0]["user_id"] == 2
        assert docs[1]["user_id"] == 12

    def test_regex(self):
        docs = self.db.find({
                "age": {"$gt": 40},
                "location.state": {"$regex": "(?i)new.*"}
            })
        assert len(docs) == 2
        assert docs[0]["user_id"] == 2
        assert docs[1]["user_id"] == 10
