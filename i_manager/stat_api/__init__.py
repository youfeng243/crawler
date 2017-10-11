#!/usr/bin/env python
#-*- coding:utf-8 -*-

from flask import Blueprint
stat_api = Blueprint('stat_api', __name__)

from . import stat
