#!/usr/bin/env python2
# -*- coding: utf-8 -*-

from __future__ import print_function
import json
import getopt
import os
import os.path
import sys

def parse_stocknames(names_file) :
    with open(names_file, 'r') as rf:
        try:
            names = json.load(rf)
        except Exception as e:
            print('Error: Failed on parsing stock names json file "' + names_file + '". Exception: ' + str(e))
            sys.exit(1)

    if not isinstance(names, list):
        print('Error: Unexpected root level JSON type. Expected: list/array, Got: ' + str(type(names)))
        sys.exit(1)

    names_set = set()
    for entry in names:
        if not isinstance(entry, dict):
            print('Warning: Unexpected stock names entry JSON type. Expected: dict, Got: ' + str(type(entry)))
            continue
        if ('id' not in entry) or ('name' not in entry):
            print('Warning: Skip enties which do not contain "id" or "name".')
            continue
        names_set.add(entry['name'])

    return names_set

def parse_stocknames_csv(stocknames_file) :
    names_set = set()
    with open(stocknames_file, 'r') as rf:
        try:
            for line in rf:
                names_set.add(line.strip(' \r\t\n').decode('utf-8'))
        except Exception as e:
            print('Error: Failed on parsing stock names csv file "' + stocknames_file + '". Exception: ' + str(e))

    return names_set

def show_usage():
    print('Usage: %s -c portfolio.json ... stock_names.json' % sys.argv[0])
    print('    Collect stocker ID/Name pairs from portfolio JSON files and writes the merged results in stock_names.json')
    print('')
    print('Usage: %s [-y] [-n stocknames.json] portfolio.json stock_names.csv' % sys.argv[0])
    print('    Patch the specific portfolio JSON file using the latest ETF member stock names in CSV file.')
    print('    It generates a diff summary and ask for approval before applying the changes. Use -y to apply without asking.')
    print('    It reads stock name/ID mapping from local file "stocknames.json". Use -n to override.')


if __name__ == '__main__':

    # Parse command line arguments
    try:
        pairs, remaining = getopt.getopt(sys.argv[1:], 'cy')
    except getopt.GetoptError as e:
        print('Error: ' + str(e), file=sys.stderr)
        show_usage()
        sys.exit(1)

    g_collectnames = False
    g_stocknamesfile = 'stocknames.json'
    g_yes = False
    for p in pairs:
        if p[0] == '-c':
            g_collectnames = True
        elif p[0] == '-y':
            g_yes = True
        elif p[0] == '-n':
            if os.path.isfile(p[1]):
                g_stocknamesfile = p[1]
            else:
                print('Error: "' + p[1] + '" is not a readable file.')
                sys.exit(1)

    if len(remaining) < 2:
        print('Error insufficient parameters.')
        show_usage()
        sys.exit(1)

    # working in the collecting mode ...
    if g_collectnames:
        knownNames = {}

        # read the existing pairs kept in file
        collectedFileName = remaining[len(remaining)-1]
        try:
            with open(collectedFileName, 'r') as rf:
                names = json.load(rf, encoding='utf8')
                for pair in names:
                    if pair["name"] not in knownNames:
                        knownNames[pair["name"]] = pair["id"]
        except:
            pass

        # merge new pairs found in portfolio files
        for i in range(len(remaining)-1):
            try:
                with open(remaining[i], 'r') as rf:
                    names = json.load(rf)
                    for pair in names:
                        if pair["name"] not in knownNames:
                            knownNames[pair["name"]] = pair["id"]
            except:
                print('Failed on reading from file: "' + reamining[i] + '" Aborted.')
                sys.exit(1)

        # write the merged result to file
        namesArray=[]
        for name in knownNames:
            namesArray.append({'name':name,'id':knownNames[name]})
        with open(collectedFileName, 'w') as wf:
            wf.write(json.dumps(namesArray, wf, indent=2, ensure_ascii=False).encode('utf8'))

        sys.exit(0)

    # Working in the patching mode ...
    name2id = {}
    with open(g_stocknamesfile, 'r') as rf:
        names =json.load(rf, encoding='utf8')
        for pair in names:
            name2id[pair["name"]] = pair["id"]

    stocknames_set = parse_stocknames_csv(remaining[1])
    # Check whether that is any unknown name
    valid = True
    for sn in stocknames_set:
        if sn not in name2id:
            print('Unknown stock name: %s, in csv file %s' % (sn, remaining[1]))
            valid = False
    if valid is not True:
        print('Error: Encountered one or more unknown stock names. Patch the %s to include those names first.' % g_stocknamesfile)
        sys.exit(1)

    # Show differences before patching
    if g_yes is not True:
        portfolio_set = parse_stocknames(remaining[0])

        to_remove = portfolio_set.difference(stocknames_set)
        to_add = stocknames_set.difference(portfolio_set)

        if (len(to_remove) == 0) and (len(to_add) == 0):
            print('No update')
            sys.exit(0)

        if len(to_remove) > 0:
            print('Difference ========== To remove:')
        for e in to_remove:
            idstr = ' (unknown)'
            if e in name2id:
                idstr = ' (%d)' % name2id[e]
            print(e + idstr)

        if len(to_add) > 0:
            print('Difference ========== To add:')
            for e in to_add:
                idstr = ' (unknown)'
                if e in name2id:
                    idstr = ' (%d)' % name2id[e]
                print(e + idstr)

        if g_yes is not True:
            approval = raw_input('Proceed ? (y/N)')
            if (approval != 'y') and (approval != 'Y'):
                sys.exit(0)

    # Perform patching (it actually re-genrate a new copy)
    array2write = []
    for sn in stocknames_set:
        array2write.append({'name':sn,'id':name2id[sn]})
    with open(remaining[0], 'w') as wf:
        wf.write(json.dumps(array2write, wf, indent=2, ensure_ascii=False).encode('utf8'))

