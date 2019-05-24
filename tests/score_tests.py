from app import app
from models import create_tables, database, Bite
import unittest
import os
import json
from cStringIO import StringIO

DATA_DIR = 'local_uploads'


class ScoreTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        create_tables()

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        # creates a test client
        self.app = app.test_client()
        # propagate the exceptions to the test client
        self.app.testing = True
        app.testing = True
        self.maxDiff = None # to show to the full diff

    def tearDown(self):
        pass

    def test_home(self):
        result = self.app.get('/')
        self.assertEqual(result.status_code, 200)

    def test_score(self):
        data_dir = os.path.join(DATA_DIR, 'data.json')
        data_file_exists = os.path.isfile(data_dir)

        if data_file_exists:
            os.remove(data_dir)

        players = ["Ben Pipes", "Anouer Taouerghi", "Anna Matienko"]
        content = "\t".join(players)
        data = {'table': 'players', 'column': 0, 'slice': 0, 'total': 1, 'addr': ''}
        data['file_slice'] = (StringIO(content), "players.csv")
        result = self.app.post('/score', data=data, content_type='multipart/form-data')
        self.assertEqual(result.status_code, 200)
        print("data dir: "+data_dir)
        data_file_exists = os.path.isfile(data_dir)
        self.assertTrue(data_file_exists)
        annotated_cells = {"data":
                               [{'Anouer Taouerghi': {u'http://dbpedia.org/resource/Anouer_Taouerghi': [
                                   u'http://dbpedia.org/ontology/Person',
                                   u'http://dbpedia.org/ontology/Agent',
                                   u'http://dbpedia.org/ontology/Athlete',
                                   u'http://dbpedia.org/ontology/VolleyballPlayer']}},
                                {'Anna Matienko': {u'http://dbpedia.org/resource/Anna_Matienko': [
                                    u'http://dbpedia.org/ontology/Person',
                                    u'http://dbpedia.org/ontology/Athlete']}}, {
                                    'Ben Pipes': {u'http://dbpedia.org/resource/Ben_Pipes': [
                                        u'http://dbpedia.org/ontology/Person',
                                        u'http://dbpedia.org/ontology/Agent',
                                        u'http://dbpedia.org/ontology/Athlete',
                                        u'http://dbpedia.org/ontology/VolleyballPlayer']}}]
                           }
        f = open(data_dir)
        computed_data = json.loads(f.read())
        # self.assertDictEqual(computed_data, annotated_cells)
        self.assertListEqual(annotated_cells["data"].keys(), computed_data["data"].keys())