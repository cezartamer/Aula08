from etl import pipeline_calcular_kpi_de_vendas_consolidado

pasta = 'data'
formato_saida = ["parquet"]

pipeline_calcular_kpi_de_vendas_consolidado(pasta, formato_saida)