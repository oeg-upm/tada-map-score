from __future__ import print_function
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# added this to import app.py
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from score_tests import ScoreTest
import unittest

score_cases = unittest.TestLoader().loadTestsFromTestCase(ScoreTest)
suite = unittest.TestSuite([score_cases])
result = unittest.TextTestRunner().run(suite)
sys.exit(not result.wasSuccessful())
