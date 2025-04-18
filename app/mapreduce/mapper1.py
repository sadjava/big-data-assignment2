import sys
import re

def tokenize(text):
    return re.findall(r'\w+', text.lower())

for line in sys.stdin:
    try:
        doc_id, title, text = line.strip().split('\t')
        terms = set(tokenize(title) + tokenize(text))  # Get unique terms per document
        for term in terms:
            print(f"{term}\t1")  # Emit each term once per document
    except:
        continue