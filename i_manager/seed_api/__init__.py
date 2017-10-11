#!/usr/bin/env python
#-*- coding:utf-8 -*-

from flask import Blueprint
seed_api = Blueprint('seed_api', __name__)

from . import seed