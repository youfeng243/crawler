#!/usr/bin/env python
# -*- coding:utf-8 -*-

from flask import Blueprint

topic_api = Blueprint('topic_api', __name__)

from . import topic
