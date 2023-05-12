
import pytest
import datetime
import json

from pysrc.env import Env

def test_username():
    username = Env.username()
    assert(username != None)
