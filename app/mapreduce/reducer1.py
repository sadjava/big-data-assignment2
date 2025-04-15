#!/usr/bin/env python
import sys
sys.path.insert(0, 'cassandra.zip')
from cassandra.cluster import Cluster


cluster = Cluster(['cassandra-server'])
session = cluster.connect('document_analysis')

update_stmt = session.prepare("""
    UPDATE document_frequencies 
    SET count = count + ? 
    WHERE term = ?
""")

current_term = None
current_count = 0

for line in sys.stdin:
    term, count = line.strip().split('\t')
    count = int(count)
    
    if term == current_term:
        current_count += count
    else:
        if current_term:
            session.execute(update_stmt, (current_count, current_term))
        current_term = term
        current_count = count

if current_term:
    session.execute(update_stmt, (current_count, current_term))
