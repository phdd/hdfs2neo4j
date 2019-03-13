#!/usr/bin/env python 

import argparse

from datetime import datetime
from neomodel import config
from runner import HdfsToNeo4j

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Import HDFS Directory to Neo4j.')

    parser.add_argument('--neo4j-url', type=str,
        dest='neo4j_url', default='bolt://neo4j:neo4j@localhost:7687',
        help="Bolt Scheme URL (default is 'bolt://neo4j:neo4j@localhost:7687')")

    parser.add_argument('--timestamp', type=str,
        dest='timestamp', default=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        help='Date and time for this version (default is now)')

    parser.add_argument('name', type=str,
        help='Symbolic import name (all nodes will be it\'s children)')

    parser.add_argument('directory', type=str,
        help='HDFS Directory to import')

    args = parser.parse_args()

    config.DATABASE_URL = args.neo4j_url
    HdfsToNeo4j(args.name, args.directory, args.timestamp).update()
