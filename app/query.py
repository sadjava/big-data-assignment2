import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
import math

# BM25 parameters
K1 = 1.2
B = 0.75

def get_cassandra_connection():
    cluster = Cluster(['cassandra-server'])  # Adjust to your Cassandra cluster
    return cluster.connect('document_analysis')

def calculate_bm25(query_terms, document_freqs, total_docs, avg_doc_length):
    # Initialize Spark
    conf = SparkConf().setAppName("BM25 Ranker")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    
    # Get Cassandra connection - only used on driver, not in workers
    session = get_cassandra_connection()
    
    # Get all documents with their lengths
    rows = session.execute("SELECT document_id, length, title FROM document_lengths")
    documents = [(row.document_id, row.length, row.title) for row in rows]
    
    # Get all term frequencies for relevant documents and terms
    term_freqs_all = {}
    for doc_id, _, _ in documents:
        term_rows = session.execute(
            "SELECT term, frequency FROM term_frequencies WHERE document_id = %s",
            [doc_id]
        )
        term_freqs = {row.term: row.frequency for row in term_rows}
        term_freqs_filtered = {k: v for k, v in term_freqs.items() if k in query_terms}
        if term_freqs_filtered:
            term_freqs_all[doc_id] = term_freqs_filtered
    
    # Broadcast variables to all workers
    query_terms_bc = sc.broadcast(query_terms)
    document_freqs_bc = sc.broadcast(document_freqs)
    total_docs_bc = sc.broadcast(total_docs)
    avg_doc_length_bc = sc.broadcast(avg_doc_length)
    term_freqs_all_bc = sc.broadcast(term_freqs_all)
    
    # Create RDD of documents
    docs_rdd = sc.parallelize(documents)
    
    # For each document, calculate BM25 score
    def calculate_doc_score(doc):
        doc_id, doc_length, title = doc
        score = 0.0
        
        # Get term frequencies for this document from broadcast variable
        term_freqs = term_freqs_all_bc.value.get(doc_id, {})
        
        # Calculate BM25 for each query term
        for term in query_terms_bc.value:
            if term in term_freqs:
                tf = term_freqs[term]
                df = document_freqs_bc.value.get(term, 0)
                if df == 0:
                    continue
                
                # IDF calculation
                idf = math.log(total_docs_bc.value / df)
                
                # TF normalization
                norm_tf = (tf * (K1 + 1)) / (
                    tf + K1 * (1 - B + B * (doc_length / avg_doc_length_bc.value)))
                
                score += idf * norm_tf
        
        return (doc_id, title, score)
    
    # Calculate scores for all documents
    scored_docs = docs_rdd.map(calculate_doc_score)
    
    # Get top 10 documents
    top_docs = scored_docs.takeOrdered(10, key=lambda x: -x[2])
    
    sc.stop()
    return top_docs

def main():
    if len(sys.argv) < 2:
        print("Usage: query.py '<query>'")
        sys.exit(1)
    
    query = sys.argv[1].lower()
    query_terms = query.split()
    
    # Get Cassandra connection
    session = get_cassandra_connection()
    
    # Get document frequencies for query terms
    document_freqs = {}
    for term in query_terms:
        row = session.execute(
            "SELECT count FROM document_frequencies WHERE term = %s",
            [term]
        ).one()
        if row:
            document_freqs[term] = row.count
    
    # Get total number of documents and average length
    rows = session.execute("SELECT COUNT(*) as count, AVG(length) as avg FROM document_lengths")
    row = rows.one()
    total_docs = row.count
    avg_doc_length = row.avg if row.avg else 1.0
    
    # Calculate BM25 scores
    top_docs = calculate_bm25(query_terms, document_freqs, total_docs, avg_doc_length)
    
    # Print results
    print("Top 10 relevant documents:")
    for i, (doc_id, title, score) in enumerate(top_docs, 1):
        print(f"{i}. Document ID: {doc_id}, Title: {title}, BM25 Score: {score:.4f}")

if __name__ == "__main__":
    main()