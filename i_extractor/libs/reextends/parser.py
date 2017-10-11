# -*- coding:utf-8 -*-
#author: gikieng
from __future__ import print_function, absolute_import, division, generators, nested_scopes

import logging
import os.path
import re

import ply.yacc

from .lexer import ReExtendsLexer

logger = logging.getLogger(__name__)


def parse(string):
    return ReExtends().parse(string)



class ReExtends(object):
    '''
    An LALR-parser for RE_FUNCTION_PATH
    '''
    FunctionNamespace = {}
    tokens = ReExtendsLexer.tokens
    def __init__(self, string, debug=False, lexer_class=None):
        if self.__doc__ == None:
            raise Exception(
                'Docstrings have been removed! By design of PLY, jsonpath-rw requires docstrings. You must not use PYTHONOPTIMIZE=2 or python -OO.')
        self.string = string
        if not isinstance(self.string, unicode):
            self.string = self.string.decode('utf-8')
        self.debug = debug
        self.lexer_class = lexer_class or ReExtendsLexer  # Crufty but works around statefulness in PLY

    def parse(self, rule, lexer=None):
        lexer = lexer or self.lexer_class()
        rule = rule.decode('utf-8')
        try:
            return self.parse_token_stream(lexer.tokenize(rule))
        except Exception as e:
            return re.findall(rule, self.string)

    def parse_token_stream(self, token_iterator, start_symbol='start'):

        output_directory = os.path.dirname(__file__)
        try:
            module_name = os.path.splitext(os.path.split(__file__)[1])[0]
        except:
            module_name = __name__

        parsing_table_module = '_'.join([module_name, start_symbol, 'parsetab'])

        # And we regenerate the parse table every time; it doesn't actually take that long!
        new_parser = ply.yacc.yacc(module=self,
                                   debug=self.debug,
                                   tabmodule=parsing_table_module,
                                   outputdir=output_directory,
                                   write_tables=0,
                                   start=start_symbol,
                                   errorlog=logger)

        return new_parser.parse(lexer=IteratorToTokenStream(token_iterator))

    # ===================== PLY Parser specification =====================

    precedence = [
        ('left', ','),
    ]

    def p_error(self, t):
        raise Exception('Parse error at %s:%s near token %s (%s)' % (t.lineno, t.col, t.value, t.type))

    def p_start(self, p):
        """start : expression"""
        p[0] = p[1]

    def p_expression(self, p):
        """expression : function"""
        p[0] = p[1]


    def p_function(self, p):
        """function :   ID '(' multiparam ')'"""

        if p[1] == 're':
            p[0] = re.findall(p[3][0], self.string)
        else:
            match_flag = False
            for k, f in self.FunctionNamespace.items():
                if p[1] == k:
                    p[0] = f(self.string, *p[3])
                    match_flag = True
                    break
            if not match_flag:
                raise NameError('not register function')

    def p_multiparam(self, p):
        """multiparam : STRING
                    |   function
                    |   NUMBER
                    """
        p[0] = [p[1]]

    def p_multiparam_ex(self, p):
        """multiparam : multiparam ','  STRING
                    |   multiparam ',' function
                    |   multiparam ',' NUMBER
                        """
        if p[1] is not None:
            p[0] = p[1][:]+[p[3]]
        else:
            p[0] = [p[3]]


class IteratorToTokenStream(object):
    def __init__(self, iterator):
        self.iterator = iterator

    def token(self):
        try:
            return next(self.iterator)
        except StopIteration:
            return None

def concat(context, *strings):
    x = []
    if isinstance(strings, tuple):
        for l in strings:
            if isinstance(l, list) and l:
                x.append(l[0])
            else:
                x.append(l)
    else:
        x.append(strings)
    ret = [''.join(map(str, x))]
    return ret

ns = ReExtends.FunctionNamespace
ns['concat'] = concat

if __name__ == '__main__':
    logging.basicConfig()
    string = """hello="test hello(你好fef.?)./" """
    rule = r"""concat(1,2)"""

    parser = ReExtends(string).parse(rule)
    print (parser)
