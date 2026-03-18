# Weather Data Pipeline

Pipeline de dados utilizando Apache Airflow e PostgreSQL.

## Arquitetura

API → RAW → TRANSFORM → TRUSTED

## Tecnologias

- Airflow
- PostgreSQL
- Docker

## Funcionalidades

- Ingestão de dados da API de clima
- Armazenamento em camada RAW
- Transformação e limpeza de dados
- UPSERT na camada TRUSTED
- Orquestração com Airflow

## Como rodar

```bash
docker compose up -d
