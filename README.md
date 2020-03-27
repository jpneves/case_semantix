# Desafio Semantix

Desafio proposto pelo time da Semantix para vaga de Engenheiro de Dados

## Authors:
João Paulo Neves da Costa


## Requirements:

Projeto criado com pipenv, python 3.7.4, spark 2.4.5

Execute os seguintes comandos, que contém apenas a instalação do pacote pyspark.
```bash
pipenv install
pipenv run python case_spark.py
```
Se no seu ambiente já possui pyspark, então uma execução python simples já basta:

```bash
python case_spark.py
```


## Purpose:

Ler arquivos de log de web servers;
Responder as seguintes perguntas utilizando Spark:

1. Número de hosts únicos.
2. O total de erros 404.
3. Os 5 URLs que mais causaram erro 404.
4. Quantidade de erros 404 por dia.
5. O total de bytes retornados.

