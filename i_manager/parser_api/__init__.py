#!/usr/bin/env python
#-*- coding:utf-8 -*-

from flask import Blueprint
parser_api = Blueprint('parser_api', __name__)

from . import parser
