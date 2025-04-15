#!/usr/bin/env python
import sys
sys.path.insert(0, 'cassandra.zip')
from cassandra.cluster import Cluster


cluster = Cluster(['cassandra-server'])
session = cluster.connect('document_analysis')

update_stmt = session.prepare("""
    UPDATE document_lengths 
    SET length = ?, title = ? 
    WHERE document_id = ?
""")

for line in sys.stdin:
    doc_id, title, length = line.strip().split('\t')
    session.execute(update_stmt, (int(length), title, int(doc_id)))
