from cassandra.cluster import Cluster


def create_tables(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS document_analysis 
        WITH replication = {
            'class': 'SimpleStrategy', 
            'replication_factor': 1
    };""")
    session.execute("USE document_analysis;")
    session.execute("""
        CREATE TABLE IF NOT EXISTS document_frequencies (
        term text PRIMARY KEY,
        count counter
    );""")
    session.execute("""
        CREATE TABLE IF NOT EXISTS term_frequencies (
        document_id bigint,
        term text,
        frequency counter,
        PRIMARY KEY (document_id, term)
    );""") 
    session.execute("""
        CREATE TABLE IF NOT EXISTS document_lengths (
        document_id bigint PRIMARY KEY,
        length int,
        title text
    );""")

if __name__ == '__main__':
    # Connects to the cassandra server
    cluster = Cluster(['cassandra-server'])

    session = cluster.connect()
    create_tables(session)

    # displays the available keyspaces
    rows = session.execute('DESC keyspaces')
    for row in rows:
        print(row)