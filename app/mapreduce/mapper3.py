#!/usr/bin/env python
import sys
import re

def tokenize(text):
    return re.findall(r'\w+', text.lower())

for line in sys.stdin:
    try:
        doc_id, title, text = line.strip().split('\t')
        terms = tokenize(title) + tokenize(text)
        length = len(terms)
        print(f"{doc_id}\t{title}\t{length}")
    except:
        continue