#!/usr/bin/env python
#-*- coding:utf-8 -*-

from flask import Blueprint
datasave_api = Blueprint('datasave_api', __name__)

from . import datasave