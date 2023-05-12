
import pytest
import datetime
import json

from pysrc.fs import FS

def test_read():
    s = FS.read('templates/bicep_params_sample.jinga2')
    assert('https://schema.management.azure.com/schemas/2019-04-01/deploymentParameters.json#' in s)
