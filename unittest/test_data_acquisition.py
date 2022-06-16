'''
Test Data Acquition:
'''

import sys
import os

#to test the files get downloaded to local file system
def test_local_download():
    assert os.path.exists("c:/sb/strfacts/indata")

#to test the files get downloaded to azure blob
def test_azure_download():
    assert os.path.exists("/dbfs/mnt/blob1/indata")
