# Contributor: Chirag Uday Kamath (cukamath@umass.edu)

import csv
import re
import itertools
import random

with open('../data/dblp/final_view.csv', 'r', newline='', encoding='utf-8') as f:
    reader = csv.reader(f)
    data = [(row) for row in reader]

# print(data[:2])
cleaned = []
for row in data:
    # print(row[4])
    l = r = 0
    # print(row[4])
    if '-' in row[4] and ',' in row[4]:
        split = row[4][:row[4].index(',')].split('-')
    elif '-' in row[4]:
        split = row[4].split('-')
        if ':' in split[0]:
            split[0] = split[0][split[0].index(':') + 1:]
        if ':' in split[1]:
            split[1] = split[1][split[1].index(':') + 1:]
        if '.' in split[0]:
            split[0] = split[0][split[0].index('.') + 1:]
        if '.' in split[1]:
            split[1] = split[1][split[1].index('.') + 1:]

    elif ',' in row[4]:
        split = row[4].split(',')
    if re.search('[a-zA-Z]', split[0]) or re.search('[a-zA-Z]', split[1]) or split[0].isalpha() or split[0].isalpha() or \
            split[0] == '' or split[0] == ' ' or split[1] == '' or split[1] == ' ':
        continue
    if len(split) > 1:
        # print(split)
        l = int(split[0].strip())
        r = int(split[1].strip())
    else:
        l = 0
        r = int(random.randint(0, 10))
        # print(r)
    p = []
    p.append(abs(r - l + 1))
    # page_count = r-l
    q = []
    school = row[1]
    if row[1] == '' or row[1] == ' ':
        school = "Not from School"
    q.append(school)
    cleaned.append(list(itertools.chain(row[0:1], q, row[2:4], p, row[5:])))
print(len(cleaned))
# print(p)

with open("../data/dblp/cleaned_final_view.data", "w", newline="", encoding='utf-8') as f:
    writer = csv.writer(f, delimiter='#')
    writer.writerows(cleaned)
