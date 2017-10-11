#!/usr/bin/env
#-*- coding:utf-8 -*-
#author:gikieng
from parser import ReExtends
ns = ReExtends.FunctionNamespace

def _collect_string_content(x):
    return str(x)

def __cartesian_product(strings, depth, multi_strings, result_set, max_depth):
    if depth == max_depth:
        result_set.add(''.join(strings))
        return
    for x in multi_strings[depth]:
        value = _collect_string_content(x)
        strings.append(value)
        __cartesian_product(strings, depth+1, multi_strings, result_set, max_depth)
        strings.pop()

def list_concat(context, *args):
    multi_strings = [x if isinstance(x, list) else [x] for x in args]
    result_set = set()
    __cartesian_product([], 0, multi_strings, result_set, max_depth=len(multi_strings))
    return list(result_set)

def list_zip_concat(context, *args):
    not_meet_length = 999999
    min_length = not_meet_length
    str_pos = []
    args = list(args)
    for i in range(len(args)):
        if isinstance(args[i], list):
            min_length = min(min_length, len(args[i]))
        else:
            str_pos.append(i)
    result_set = set()
    if min_length == not_meet_length:
        result_set.add("".join(args))
        return result_set
    for i in str_pos:
        args[i] = [args[i]] * min_length
    multi_zip = zip(*args)
    for strings in multi_zip:
        result_set.add("".join(map(_collect_string_content, strings)))
    return list(result_set)


def list_substring(context, strings, start, end = None):
    start = int(start) - 1
    if end:
        end = int(end)
    if not isinstance(strings, list):
        strings = [strings]
    strings = [_collect_string_content(x) for x in strings]
    res = []
    for s in strings:
        try:
            res.append(s[start:end])
        except Exception as e:
            pass
    return res

def list_substring_after(context, strings, pattern):
    if not isinstance(strings, list):
        strings = [strings]
    strings = [_collect_string_content(x) for x in strings]
    res = []
    for s in strings:
        try:
            res.append(''.join(s.split(pattern)[-1:]))
        except:
            pass
    return res

def list_substring_before(context, strings, pattern):
    if not isinstance(strings, list):
        strings = [strings]
    strings = [_collect_string_content(x) for x in strings]
    res = []
    for s in strings:
        try:
            res.append(''.join(s.split(pattern)[:1]))
        except:
            pass
    return res


def format_string(context, pattern, *args):
    strings = [_collect_string_content(x) for x in args]
    return [pattern.format(*strings)]

def replace(context, string, old, new):
    return [string[0].replace(old, new)]

def list_replace(context, strings, old, new):
    if not isinstance(strings, list):
        strings = [strings]
    strings = [_collect_string_content(x) for x in strings]
    res = []
    for s in strings:
        try:
            res.append(s.replace(old, new))
        except:
            pass
    return res


def join_string(context, strings, sp="\t"):
    return [sp.join(map(_collect_string_content, strings))]

def slice(context, ls, start, end = None):
    start = int(start) - 1
    if end:
        end = int(end)
    return ls[start:end]

def number(context, string):
    return float(string)

ns['list-concat'] = list_concat
ns['list-zip-concat'] = list_zip_concat
ns['list-substring'] = list_substring
ns['list-substring-after'] = list_substring_after
ns['list-substring-before'] =  list_substring_before
ns['format-string'] = format_string
ns['replace'] = replace
ns['list-replace'] = list_replace
ns['number'] = number
ns['slice'] = slice
ns['join-string'] = join_string

if __name__ == "__main__":
    format_string("", '{}{}{}', *['hello', 'wolrd'])