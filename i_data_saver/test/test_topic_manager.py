# -*- coding: utf-8 -*-

import sys
import os
SCRIPT_PATH = os.path.dirname(__file__)
sys.path.append(os.path.join(SCRIPT_PATH, '..'))
sys.path.append(os.path.join(SCRIPT_PATH, '../..'))
import pprint

import topic_manager

tpm = topic_manager.TopicManager()
pprint.pprint(tpm.topic_dict)
