#!/usr/bin/env python2
# -*- coding: utf-8 -*-

from __future__ import print_function
import json
import sys

def parse_portfolio_file(portfolio_file) :
    with open(portfolio_file, 'r') as rf:
        try:
            portfolio = json.load(rf)
        except Exception as e:
            print('Error: Failed on parsing portfolio json file "' + portfolio_file + '". Exception: ' + str(e))
            sys.exit(1)

    if not isinstance(portfolio, list):
        print('Error: Unexpected root level JSON type. Expected: list/array, Got: ' + str(type(portfolio)))
        sys.exit(1)

    portfolio_set = set()
    for entry in portfolio:
        if not isinstance(entry, dict):
            print('Warning: Unexpected portfolio entry JSON type. Expected: dict, Got: ' + str(type(entry)))
            continue
        if ('id' not in entry) or ('name' not in entry):
            print('Warning: Skip enties which do not contain "id" or "name".')
            continue
        #print('portfolio: ' + str(type(entry['name'])))
        portfolio_set.add(entry['name'])

    return portfolio_set

def parse_stocknames_csv(stocknames_file) :
    names_set = set()
    with open(stocknames_file, 'r') as rf:
        try:
            for line in rf:
                names_set.add(line.strip(' \r\t\n').decode('utf-8'))
        except Exception as e:
            print('Error: Failed on parsing stock names csv file "' + stocknames_file + '". Exception: ' + str(e))

    return names_set

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage: pfdiff portfolio.json stock_names.csv')
        os.exit(1)

    portfolio_set = parse_portfolio_file(sys.argv[1])
    stocknames_set = parse_stocknames_csv(sys.argv[2])

    to_remove = portfolio_set.difference(stocknames_set)
    to_add = stocknames_set.difference(portfolio_set)

    if (len(to_remove) == 0) and (len(to_add) == 0):
        sys.exit(0)

    if len(to_remove) > 0:
        print('========== To remove:')
        for e in to_remove:
            print(e)

    if len(to_add) > 0:
        print('========== To add:')
        for e in to_add:
            print(e)

