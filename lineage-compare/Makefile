.PHONY: normalize up-neo4j up-tg compare down clean
normalize:
	bash scripts/normalize.sh
up-neo4j:
	docker compose up -d neo4j && docker compose up --build neo4j-loader
up-tg:
	docker compose up -d tigergraph && bash scripts/tg_bootstrap.sh && docker compose up --build tg-loader
compare:
	docker compose up --build compare
down:
	docker compose down
clean:
	rm -rf data/normalized/* data/output/* data/neo4j
