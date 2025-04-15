#!/usr/bin/env python
import sys
import re

def tokenize(text):
    return re.findall(r'\w+', text.lower())

for line in sys.stdin:
    try:
        doc_id, title, text = line.strip().split('\t')
        terms = tokenize(title) + tokenize(text)
        for term in terms:
            print(f"{doc_id}\t{term}\t1")  # Emit each term occurrence
    except:
        continue