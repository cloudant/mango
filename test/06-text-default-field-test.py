
import mango



class NoDefaultFieldTest(mango.UserDocsTextTests):

    DEFAULT_FIELD = False

    def test_basic(self):
        docs = self.db.find({"$text": "Ramona"})
        # Or should this throw an error?
        assert len(docs) == 0

    def test_other_fields_exist(self):
        docs = self.db.find({"age": 22})
        assert len(docs) == 1
        assert docs[0]["user_id"] == 9


class NoDefaultFieldWithAnalyzer(mango.UserDocsTextTests):

    DEFAULT_FIELD = {
        "enabled": False,
        "analyzer": "keyword"
    }

    def test_basic(self):
        docs = self.db.find({"$text": "Ramona"})
        assert len(docs) == 0

    def test_other_fields_exist(self):
        docs = self.db.find({"age": 22})
        assert len(docs) == 1
        assert docs[0]["user_id"] == 9


class DefaultFieldWithCustomAnalyzer(mango.UserDocsTextTests):

    DEFAULT_FIELD = {
        "enabled": True,
        "analyzer": "keyword"
    }

    def test_basic(self):
        docs = self.db.find({"$text": "Ramona"})
        assert len(docs) == 1
        assert docs[0]["user_id"] == 9

    def test_not_analyzed(self):
        docs = self.db.find({"$text": "Lott Place"})
        assert len(docs) == 1
        assert docs[0]["user_id"] == 9

        docs = self.db.find({"$text": "Lott"})
        assert len(docs) == 0

        docs = self.db.find({"$text": "Place"})
        assert len(docs) == 0
