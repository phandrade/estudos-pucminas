import pandas as pd
import functools as ft

from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

default_args={
    'owner':'Paulo',
    'dependes_on_past':False,
    'start_date':datetime(2002,9,29)
}

@dag(default_args=default_args,schedule_interval='@once',catchup=False, tags=["Paulo","PUC","dag2"])
def trabalho2_dag2():

    # 1. Ler a tabela única de indicadores feitos na Dag1 (/tmp/tabela_unica.csv)
    # 3. Printar a tabela nos logs
    @task
    def ingestao():
        NOME_DO_ARQUIVO = "/tmp/tabela_unica.csv"
        df = pd.read_csv(NOME_DO_ARQUIVO, sep=';')
        df.to_csv(NOME_DO_ARQUIVO, index=False, header=True, sep=";")
        print(df)
        return NOME_DO_ARQUIVO

    # 2. Produzir médias para cada indicador considerando o total
    # 3. Printar a tabela nos logs
    @task
    def ind_media_passageiros(nome_do_arquivo):
        NOME_DO_ARQUIVO = "/tmp/tabela_media_total.csv"
        df = pd.read_csv(nome_do_arquivo, sep=';')
        res = df.agg({
            "total_passageiros":"mean",
            "preco_medio":"mean",
            "total_sibsp_parch":"mean"
        }).reset_index()  
        res.rename(columns = {'total_passageiros':'media_passageiros'}, inplace = True)
        res.rename(columns = {'preco_medio':'media_preco'}, inplace = True)
        res.rename(columns = {'total_sibsp_parch':'media_sibsp_parch'}, inplace = True)  
        print(res)
        res.to_csv(NOME_DO_ARQUIVO, index=False, sep=';')
        return NOME_DO_ARQUIVO


    # 4. Escrever o resultado em um arquivo csv local no container (/tmp/resultados.csv)
    @task
    def resultados(path1):
        PATH_SAIDA = "/tmp/resultados.csv"
        df_final = pd.read_csv(path1, sep=';')
        print(df_final)
        df_final.to_csv(PATH_SAIDA, index=False, sep=';')
        return PATH_SAIDA


    fim = DummyOperator(task_id="fim")

    ing = ingestao()
    ind = ind_media_passageiros(ing)
    p = resultados(ind)
    
    ind >> p >> fim

execucao = trabalho2_dag2()
    