#!/usr/bin/env python
import sys
sys.path.insert(0, 'cassandra.zip')
from cassandra.cluster import Cluster


cluster = Cluster(['cassandra-server'])
session = cluster.connect('document_analysis')

update_stmt = session.prepare("""
    UPDATE term_frequencies 
    SET frequency = frequency + ? 
    WHERE document_id = ? AND term = ?
""")

current_key = None
current_count = 0

for line in sys.stdin:
    doc_id, term, count = line.strip().split('\t')
    count = int(count)
    key = (doc_id, term)
    
    if key == current_key:
        current_count += count
    else:
        if current_key:
            session.execute(update_stmt, (current_count, int(current_key[0]), current_key[1]))
        current_key = key
        current_count = count

if current_key:
    session.execute(update_stmt, (current_count, int(current_key[0]), current_key[1]))
