import logging
import re
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from module.file_manager import FileManager

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                    level=logging.INFO,
                    datefmt="%d/%m/%y %H:%M:%S")


def get_files():
    "Realiza o download dos arquivos listados"
    dir = 'data/'
    ftp_files = ['ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz',
                 'ftp://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz']

    for f in ftp_files:
        file = FileManager(f, dir)
        file.download_file()

    return


def init_spark():
    """Abre a conexão com o spark"""
    spark = SparkSession.builder.appName("Csse Semantix").getOrCreate()
    sc = spark.sparkContext

    return spark, sc


def parse_log_file(row, pattern):
    """Procura por padrão em cada linha"""
    match = re.match(pattern, row)
    if match is None:
        return (row, 'not match')

    return (match.groups(), 'match')


def get_parse_result(rdd):
    """Separa os registros que tiveram padrão encontrado daqueles que não tem padrão conhecido"""
    rdd_match = rdd.filter(lambda r: r[1] == 'match') \
        .map(lambda r: r[0])
    rdd_fail = rdd.filter(lambda r: r[1] == 'not match') \
        .map(lambda r: r[0])

    logging.info('Quantidade de registros com match: {}'.format(rdd_match.count()))
    logging.info('Quantidade de registros sem match: {}'.format(rdd_fail.count()))

    return rdd_match, rdd_fail


def rdd_to_df(rdd, schema):
    """Gera spark dataframe a partir de um rdd e schema definido"""

    return rdd.toDF(schema=schema)


def get_answers(df):
    """Imprime o resultado das perguntas do Case"""

    logging.info('Número de hosts únicos:')
    df.agg(f.countDistinct("host")).show()

    logging.info('O total de erros 404:')
    df_404 = df.filter(df['response'] == '404').cache()
    print(df_404.count())

    logging.info('Os 5 URLs que mais causaram erro 404:')
    df_404.groupBy('url') \
        .agg(f.count(f.lit(1)).alias('qtde')) \
        .orderBy('qtde', ascending=False) \
        .show(5, False)

    logging.info('Quantidade de erros 404 por dia:')
    df_404.groupBy('date') \
        .agg(f.count(f.lit(1)).alias('qtde')) \
        .orderBy('date') \
        .show(100, False)

    logging.info('O total de bytes retornados:')
    df.agg(f.sum('bytes')).show()

    return logging.info('Respostas concluídas!!!')


def main():
    spark, sc = init_spark()

    get_files()

    pattern = '^(\S+) - - \[(\w+\/\w+\/\w+):(\S+)\s([+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)'
    rdd_log = sc.textFile('data/*.gz') \
        .map(lambda x: parse_log_file(x, pattern))
    rdd_match, rdd_fail = get_parse_result(rdd_log)

    schema = StructType([
        StructField('host', StringType(), True),
        StructField('date', StringType(), True),
        StructField('time', StringType(), True),
        StructField('timezone', StringType(), True),
        StructField('method', StringType(), True),
        StructField('url', StringType(), True),
        StructField('protocol', StringType(), True),
        StructField('response', StringType(), True),
        StructField('bytes', StringType(), True)
    ])

    df_log = rdd_to_df(rdd_match, schema)

    df_log = df_log.withColumn('date', f.from_unixtime(f.unix_timestamp(df_log['date'], 'dd/MMM/yyyy'))) \
        .withColumn('bytes', df_log['bytes'].cast(IntegerType()))

    get_answers(df_log)


if __name__ == '__main__':
    main()
