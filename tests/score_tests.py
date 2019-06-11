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
        table_name = 'players'
        players = ["Ben Pipes", "Anouer Taouerghi", "Anna Matienko"]
        content = "\t".join(players)
        data = {'table': table_name, 'column': 0, 'slice': 0, 'total': 1, 'addr': ''}
        data['file_slice'] = (StringIO(content), "players.csv")
        Bite.delete().execute()  # delete all instances
        print("after deletiong: "+str(len(Bite.select())))
        result = self.app.post('/score', data=data, content_type='multipart/form-data')
        database.connect(reuse_if_open=True)
        self.assertEqual(result.status_code, 200)
        bites = Bite.select().where(Bite.table==table_name, Bite.column==0)
        self.assertEqual(len(bites), 1)
        data_dir = os.path.join(DATA_DIR, bites[0].fname)
        print("data_dir: "+data_dir)
        data_file_exists = os.path.isfile(data_dir)
        self.assertTrue(data_file_exists, msg="The data file is not found")
        # annotated_cells = {"data":
        #                        [{'Anouer Taouerghi': {u'http://dbpedia.org/resource/Anouer_Taouerghi': [
        #                            u'http://dbpedia.org/ontology/Person',
        #                            u'http://dbpedia.org/ontology/Agent',
        #                            u'http://dbpedia.org/ontology/Athlete',
        #                            u'http://dbpedia.org/ontology/VolleyballPlayer']}},
        #                         {'Anna Matienko': {u'http://dbpedia.org/resource/Anna_Matienko': [
        #                             u'http://dbpedia.org/ontology/Person',
        #                             u'http://dbpedia.org/ontology/Athlete']}}, {
        #                             'Ben Pipes': {u'http://dbpedia.org/resource/Ben_Pipes': [
        #                                 u'http://dbpedia.org/ontology/Person',
        #                                 u'http://dbpedia.org/ontology/Agent',
        #                                 u'http://dbpedia.org/ontology/Athlete',
        #                                 u'http://dbpedia.org/ontology/VolleyballPlayer']}}]
        #                    }
        # f = open(data_dir)
        # computed_data = json.loads(f.read())
        # self.assertListEqual(sorted(annotated_cells["data"]), sorted(computed_data["data"]))
        # k1 = annotated_cells["data"][0].keys()[0]
        # k2 = annotated_cells["data"][0][k1].keys()[0]
        # del annotated_cells["data"][0][k1][k2][0]
        # self.assertTrue(sorted(annotated_cells["data"]) != sorted(computed_data["data"]))
        annotated_cells = {"data":
                               {
                               'Anouer Taouerghi': {u'http://dbpedia.org/resource/Anouer_Taouerghi': [
                                   u'http://dbpedia.org/ontology/Person',
                                   u'http://dbpedia.org/ontology/Agent',
                                   u'http://dbpedia.org/ontology/Athlete',
                                   u'http://dbpedia.org/ontology/VolleyballPlayer']},
                                'Anna Matienko': {u'http://dbpedia.org/resource/Anna_Matienko': [
                                    u'http://dbpedia.org/ontology/Person',
                                    u'http://dbpedia.org/ontology/Athlete']},
                                    'Ben Pipes': {u'http://dbpedia.org/resource/Ben_Pipes': [
                                        u'http://dbpedia.org/ontology/Person',
                                        u'http://dbpedia.org/ontology/Agent',
                                        u'http://dbpedia.org/ontology/Athlete',
                                        u'http://dbpedia.org/ontology/VolleyballPlayer']}
                               }
                           }
        f = open(data_dir)
        computed_data = json.load(f)
        self.assertDictEqual(annotated_cells, computed_data)
        # computed_data = json.loads(f.read())
        # self.assertListEqual(sorted(annotated_cells["data"]), sorted(computed_data["data"]))
        # k1 = annotated_cells["data"][0].keys()[0]
        # k2 = annotated_cells["data"][0][k1].keys()[0]
        # del annotated_cells["data"][0][k1][k2][0]
        # self.assertTrue(sorted(annotated_cells["data"]) != sorted(computed_data["data"]))

    def test_fetch(self):
        result = self.app.get('/fetch')
        self.assertEqual(result.status_code, 200)

    def test_hello(self):
        result = self.app.get('/')
        self.assertEqual(result.status_code, 200)
