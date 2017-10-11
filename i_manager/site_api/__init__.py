#!/usr/bin/env python
#-*- coding:utf-8 -*-

from flask import Blueprint
site_api = Blueprint('site_api', __name__)

from . import site