#!/usr/bin/env python
# -*- coding:utf-8 -*-

from flask import Blueprint
schedule_api = Blueprint('schedule_api', __name__)

from . import schedule
