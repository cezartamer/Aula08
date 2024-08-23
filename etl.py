import pandas as pd
import os
import glob

# Função de extract que le e consolida os json
def extrair_dados(pasta: str) -> pd.DataFrame:
    arquivos_json = glob.glob(os.path.join(pasta,"*.json"))
    df_list = [pd.read_json(arquivo) for arquivo in arquivos_json]
    df_total = pd.concat(df_list, ignore_index=True)
    return df_total

# Função que transforma

def calcular_kpi_de_total_de_vendas(df_total:pd.DataFrame) -> pd.DataFrame:
    df_total["Total"] = df_total["Quantidade"] * df_total["Venda"]
    return df_total

# Load

def carregar_dados(df:pd.DataFrame, nome:str, formato_saida:list):
    for format in formato_saida:
        if format in 'csv':
            df.to_csv(nome +".csv", index=False)
        if format in 'parquet':
            df.to_parquet(nome + ".parquet")

def pipeline_calcular_kpi_de_vendas_consolidado(path: str, formato_saida: list):
    data_frame = extrair_dados(path)
    data_frame_calculado = calcular_kpi_de_total_de_vendas(data_frame)
    carregar_dados(data_frame_calculado, "dado_salvo", formato_saida)


if __name__ == "__main__":
    path = 'data'
    data_frame = extrair_dados(path)
    data_frame_calculado = calcular_kpi_de_total_de_vendas(data_frame)
    formato_saida: list = ["csv", "parquet"]
    carregar_dados(data_frame_calculado, "dado_salvo", formato_saida)
