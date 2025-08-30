#!/usr/bin/env python3
import argparse, orjson, os
from neo4j import GraphDatabase

def load_lines(path):
    with open(path, "rb") as f:
        for line in f:
            if line.strip():
                yield orjson.loads(line)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", required=True)
    ap.add_argument("--edges", required=True)
    args = ap.parse_args()
    uri=os.getenv("NEO4J_URI","bolt://neo4j:7687"); user=os.getenv("NEO4J_USER","neo4j"); pwd=os.getenv("NEO4J_PASS","neo4j")
    driver = GraphDatabase.driver(uri, auth=(user,pwd))
    with driver.session() as s:
        for stmt in open("/cypher/constraints.cypher").read().split(";"):
            stmt=stmt.strip()
            if stmt: s.run(stmt)
        tx = s.begin_transaction()
        i=0
        for n in load_lines(args.nodes):
            tx.run("""MERGE (x:Node {id:$id})
                      ON CREATE SET x.name=$name, x.type=$type, x.repo=$repo
                      ON MATCH  SET x.name=coalesce(x.name,$name), x.type=coalesce(x.type,$type), x.repo=coalesce(x.repo,$repo)
                   """, **n)
            i+=1
            if i%1000==0: tx.commit(); tx=s.begin_transaction()
        tx.commit()
        tx = s.begin_transaction()
        i=0
        for e in load_lines(args.edges):
            tx.run("""MATCH (a:Node {id:$src}), (b:Node {id:$dst})
                      MERGE (a)-[r:FLOWS_TO {op:$op}]->(b)
                      ON CREATE SET r.first_ts=$ts, r.weight=$weight
                      ON MATCH  SET r.last_ts=$ts
                   """, **e)
            i+=1
            if i%2000==0: tx.commit(); tx=s.begin_transaction()
        tx.commit()
    print("Neo4j load complete.")

if __name__ == "__main__":
    main()
