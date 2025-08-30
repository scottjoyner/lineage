#!/usr/bin/env bash
set -euo pipefail
echo "[tg_bootstrap] Waiting for TigerGraph..."
for i in {1..60}; do
  if curl -fsS http://localhost:9000/echo >/dev/null; then break; fi
  sleep 3
done
docker compose exec -T tigergraph bash -lc '  gadmin start all &&   until gsql -version >/dev/null 2>&1; do sleep 3; done;   gsql -g LineageGraph - <<"GSQL"
USE GLOBAL
DROP GRAPH LineageGraph IF EXISTS
CREATE GRAPH LineageGraph(Node, FLOWS_TO)
USE GRAPH LineageGraph
CREATE VERTEX Node (PRIMARY_ID id STRING, name STRING, type STRING, repo STRING) WITH primary_id_as_attribute="true";
CREATE DIRECTED EDGE FLOWS_TO (FROM Node, TO Node, op STRING, ts DATETIME, weight FLOAT);
CREATE QUERY getSinks(SET<STRING> srcIds) SYNTAX v2 {
  SetAccum<STRING> @@frontier, @@next, @@visited, @@sinks;
  @@frontier = srcIds;
  WHILE size(@@frontier) > 0 DO
    SELECT t FROM Node:s -(FLOWS_TO:e)-> Node:t
      WHERE s.id IN @@frontier
      ACCUM @@visited += s.id, @@next += t.id;
    @@frontier = @@next;
    @@next.clear();
  END;
  SELECT x FROM Node:x
    WHERE x.id IN @@visited
    AND ( SELECT COUNT(y) FROM Node:x -(FLOWS_TO:e)-> Node:y ) == 0
    ACCUM @@sinks += x.id;
  PRINT @@sinks AS sinks;
}
INSTALL QUERY getSinks
GSQL'
echo "[tg_bootstrap] Done."
