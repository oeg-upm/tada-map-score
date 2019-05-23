from app import app
import unittest
import os
from cStringIO import StringIO


DATA_DIR = 'local_data'


class ScoreTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        # creates a test client
        self.app = app.test_client()
        # propagate the exceptions to the test client
        self.app.testing = True

    def tearDown(self):
        pass

    def test_home_status_code(self):
        # sends HTTP GET request to the application
        # on the specified path
        result = self.app.get('/')

        # assert the status code of the response
        self.assertEqual(result.status_code, 200)

    def test_score(self):
        players = ["Ben Pipes", "Anouer Taouerghi", "Anna Matienko"]
        content = "\t".join(players)
        data = {'table': 'players', 'column': 0, 'slice': 0, 'total': 1, 'addr': ''}
        data['file_slice'] = (StringIO(content), "players.csv")
        result = self.app.post('/score', data=data, content_type='multipart/form-data')
        self.assertEqual(result.status_code, 200)



