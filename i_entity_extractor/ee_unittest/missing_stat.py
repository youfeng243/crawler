#!/bin/env python
# coding=utf8

import argparse
import sys

import sys
reload(sys)
sys.setdefaultencoding('utf-8')


sys.argv.extend('-i xizang.txt'.split())

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', help='input', dest='input', default=None)
    # parser.add_argument('-o', '--output', help='output', dest='output', default=None)
    args = parser.parse_args()



    with open(args.input) as ifile:#, open(args.output) as ofile:

        seen_company = set([])
        passed_company = set([])
        company_missing_attr = {}

        missing_map_total = {}
        last_company = None
        current_company = None
        last_line = None
        count = 0
        for line in ifile:
            line = line.decode('utf-8')
            count +=1
            if 'Missing attrs :' in line:
                current_company = line.split(' Missing attrs : ')[0].strip()
            else:
                current_company = line.split(' Passed')[0].strip()
                passed_company.add(current_company)
            seen_company.add(current_company)
            if count == 1262:
                a=1
            if last_company != current_company:
                if last_line is not None:
                    last_line_parts = last_line.split(u' Missing attrs : ')
                    if len(last_line_parts) == 2:
                        missing_attrs = last_line_parts[1].strip().split(', ')
                        print last_company + " missing " + ", ".join(missing_attrs)
                        company_missing_attr[last_company] = missing_attrs
                        for attr in missing_attrs:
                            missing_map_total.setdefault(attr, 0)
                            missing_map_total[attr] += 1

            last_company = current_company
            last_line = line

        missing_sorted = sorted(missing_map_total.items(), cmp=lambda a, b: cmp(a[1], b[1]), reverse=True)

        not_passed_company = sorted(list(seen_company - passed_company))
        print "\n\n## RESULT ##\n\n"
        for company in not_passed_company:
            missing = company_missing_attr[company]
            print "%s missing %s" % (company, ", ".join(missing))
        print "%d companies not passed in %d companies" % (len(not_passed_company), len(seen_company))
        for tuple in missing_sorted:
            print tuple



