#!/usr/bin/env python
#-*- coding:utf-8 -*-

from flask import Blueprint
download_api = Blueprint('download_api', __name__)

from . import proxy