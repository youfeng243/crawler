#!/usr/bin/env python
#-*- coding:utf-8 -*-

from flask import Blueprint
entity_api = Blueprint('entity_api', __name__)

from . import entity