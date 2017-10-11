#!/usr/bin/env python
# -*- coding:utf-8 -*-

class NoParserConfigException(Exception):
    def __init__(self, value = "NoParseConfig"):
        self.message = value

    def __str__(self):
        return self.message

class ParserErrorException(Exception):
    def __init__(self, value = "ParserError"):
        self.message = value
    def __str__(self):
        return self.message

class PageDecodeException(Exception):
    def __init__(self, value="PageDecodeError"):
        self.message = value
    def __str__(self):
        return self.message

class NoBodyException(Exception):
    def __init__(self, value="NoBodyToParser"):
        self.message = value
    def __str__(self):
        return self.message

class ParseNothingException(Exception):
    def __init__(self, value="ParseNothing"):
        self.message = value
    def __str__(self):
        return self.message

class NoNeedParseException(Exception):
    def __init__(self, value="NoNeedParse"):
        self.message = value
    def __str__(self):
        return self.message

class RequireException(Exception):
    def __init__(self, value="RequireError"):
        self.message = value
    def __str__(self):
        return self.message

class NoFoundPluginException(Exception):
    def __init__(self, value="NoFoundPluginException"):
        self.message = value
    def __str__(self):
        return self.message