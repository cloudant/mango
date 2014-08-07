
import mango



class CustomFieldsTest(mango.UserDocsTextTests):

    FIELDS = [
        {"name": "favorites.[]", "type": "string"},
        {"name": "manager", "type": "boolean"},
        {"name": "age", "type": "number"},
        # These two are to test the default analyzer for
        # each field.
        {"name": "location.state", "type": "string"},
        {
            "name": "location.address.street",
            "type": "string"
        }
    ]

    def test_basic(self):
        docs = self.db.find({"age": 22})
        assert len(docs) == 1
        assert docs[0]["user_id"] == 9

    def test_multi_field(self):
        docs = self.db.find({"age": 22, "manager": True})
        assert len(docs) == 1
        assert docs[0]["user_id"] == 9

        docs = self.db.find({"age": 22, "manager": False})
        assert len(docs) == 0

    def test_missing(self):
        self.db.find({"location.state": "Nevada"})

    def test_missing_type(self):
        # Raises an exception
        try:
            self.db.find({"age": "foo"})
            raise Exception("Should have thrown an HTTPError")
        except:
            return

    def test_field_analyzer_is_keyword(self):
        docs = self.db.find({"location.state": "New"})
        assert len(docs) == 0

        docs = self.db.find({"location.state": "New Hampshire"})
        assert len(docs) == 1
        assert docs[0]["user_id"] == 10
