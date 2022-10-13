import pandas as pd
import functools as ft

from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

URL = "https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv"

default_args={
    'owner':'Paulo',
    'dependes_on_past':False,
    'start_date':datetime(2002,9,29)
}

@dag(default_args=default_args,schedule_interval='@once',catchup=False, tags=["Paulo","PUC","dag1"])
def trabalho2_dag1():

    @task
    def ingestao():
        NOME_DO_ARQUIVO = "/tmp/titanic.csv"
        df = pd.read_csv(URL, sep=';')
        df.to_csv(NOME_DO_ARQUIVO, index=False, header=True, sep=";")
        return NOME_DO_ARQUIVO

    #1. Quantidade de passageiros por sexo e classe (produzir e escrever)
    @task
    def ind_total_passageiros(nome_do_arquivo):
        NOME_TABELA = "/tmp/passageito_por_sexo_class.csv"
        df = pd.read_csv(nome_do_arquivo, sep=';')
        res = df.groupby(['Sex','Pclass']).agg({
            "PassengerId":"count"
        }).reset_index()
        res.rename(columns = {'PassengerId':'total_passageiros'}, inplace = True)
        print(res)
        res.to_csv(NOME_TABELA, index=False, sep=';')
        return NOME_TABELA

    #2. Preço médio da tarifa pago por sexo e classe (produzir e escrever)
    @task
    def ind_preco_medio_sexo_classe(nome_do_arquivo):
        PATH_SAIDA = "/tmp/preco_medio_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=';')
        res = df.groupby(['Sex','Pclass']).agg({
            "Fare":"mean"
        }).reset_index()    
        res.rename(columns = {'Fare':'preco_medio'}, inplace = True)
        res['preco_medio'] = res['preco_medio'].round(decimals = 2)
        print(res)
        res.to_csv(PATH_SAIDA, index=False, sep=';')
        return PATH_SAIDA

    # 3. Quantidade total de SibSp + Parch (tudo junto) por sexo e classe (produzir e escrever)
    @task
    def ind_total_sibsp_parch_por_sexo_classe(nome_do_arquivo):
        PATH_SAIDA = "/tmp/familia_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=';')
        soma = df['SibSp']+df['Parch']
        df['total']=soma
        res = df.groupby(['Sex','Pclass']
        ).agg({
            "total":"sum"
        }).reset_index()
        res.rename(columns = {'total':'total_sibsp_parch'}, inplace = True)
        print(res)
        res.to_csv(PATH_SAIDA, index=False, sep=';')
        return PATH_SAIDA

    # 3. Juntar todos os indicadores criados em um único dataset (produzir o dataset e escrever) /tmp/tabela_unica.csv
    @task
    def merge(path1, path2, path3):
        raw_passageiros = path1
        raw_tarifa = path2
        raw_familiares = path3
        
        NOME_TABELA = "/tmp/tabela_unica.csv"

        passageiros = pd.read_csv(raw_passageiros, sep=";")
        tarifa = pd.read_csv(raw_tarifa, sep=";")
        familiares = pd.read_csv(raw_familiares, sep=";")

        df_resultado = (
            passageiros
                .merge(tarifa, how="inner", on=['Sex','Pclass'])
                .merge(familiares, how="inner", on=['Sex','Pclass'])
        ).reset_index()

        print("\n"+df_resultado.to_string())
        df_resultado.rename(columns = {'Sex':'sexo'}, inplace = True)
        df_resultado.rename(columns = {'Pclass':'classe'}, inplace = True)
        
        df_resultado.to_csv(NOME_TABELA, index=False, sep=";")

        return NOME_TABELA
        
    
    fim = DummyOperator(task_id="fim")

    ing = ingestao()
    ind_tp = ind_total_passageiros(ing)
    ind_mp = ind_preco_medio_sexo_classe(ing)
    ind_ts = ind_total_sibsp_parch_por_sexo_classe(ing)
    me = merge(ind_tp, ind_mp, ind_ts)

    trigger_dag2 = TriggerDagRunOperator(
            task_id='trigger_trabalho2_dag2',
            trigger_dag_id='trabalho2_dag2'
        )

    [ind_tp,ind_mp,ind_ts] >> me >> trigger_dag2 >> fim

execucao = trabalho2_dag1()