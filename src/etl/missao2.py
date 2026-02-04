import pandas as pd
import numpy as np
import os
import re

# --- CONFIGURAÇÕES ---
ARQUIVO_DESPESAS = "consolidado_despesas.csv"
ARQUIVO_CADASTRO = "./downloads_ans/Relatorio_cadop.csv"
ARQUIVO_SAIDA = "despesas_agregadas.csv"
ARQUIVO_COMPLETO_SQL = "dados_completos_para_sql.csv"

def padronizar_registro_ans(valor):
    try:
        limpo = str(int(float(valor)))
        return limpo.zfill(6)
    except:
        return str(valor)

def executar_missao_2():
    print("--- INICIANDO MISSÃO 2: CORREÇÃO (V4) ---")
    
    if not os.path.exists(ARQUIVO_DESPESAS) or not os.path.exists(ARQUIVO_CADASTRO):
        print("❌ Erro: Arquivos não encontrados.")
        return

    # 1. CARREGAR DESPESAS
    print("Carregando despesas...")
    df_despesas = pd.read_csv(ARQUIVO_DESPESAS, sep=';', encoding='utf-8')
    df_despesas = df_despesas.drop(columns=['CNPJ', 'RazaoSocial'], errors='ignore')
    df_despesas['RegistroANS'] = df_despesas['RegistroANS'].apply(padronizar_registro_ans)

    # 2. CARREGAR CADASTRO
    print("Carregando cadastro...")
    try:
        df_cadastro = pd.read_csv(ARQUIVO_CADASTRO, sep=';', encoding='utf-8')
    except:
        df_cadastro = pd.read_csv(ARQUIVO_CADASTRO, sep=';', encoding='latin1')

    df_cadastro['RegistroANS'] = df_cadastro['REGISTRO_OPERADORA'].apply(padronizar_registro_ans)
    
    # --- AQUI ESTAVA O ERRO ---
    # Adicionei 'Modalidade' na lista abaixo
    colunas_necessarias = ['RegistroANS', 'CNPJ', 'Razao_Social', 'Modalidade', 'UF']
    df_cadastro_limpo = df_cadastro[colunas_necessarias].copy()
    
    print(f"Despesas: {len(df_despesas)} linhas | Cadastro: {len(df_cadastro_limpo)} operadoras")

    # 3. O JOIN
    print("Cruzando tabelas...")
    df_completo = pd.merge(df_despesas, df_cadastro_limpo, on='RegistroANS', how='left')
    
    df_completo = df_completo.rename(columns={'Razao_Social': 'RazaoSocial'})
    
    # Preenche vazios
    df_completo['RazaoSocial'] = df_completo['RazaoSocial'].fillna("OPERADORA NÃO IDENTIFICADA")
    df_completo['UF'] = df_completo['UF'].fillna("INDEFINIDO")
    df_completo['Modalidade'] = df_completo['Modalidade'].fillna("DESCONHECIDA") # Preenche modalidade vazia
    
    # 5. AGREGAÇÃO E SALVAMENTO
    print("Gerando arquivos corrigidos...")
    
    # Arquivo Agregado
    df_agregado = df_completo.groupby(['RazaoSocial', 'UF'])['Valor'].agg(
        Total_Despesas='sum', Media_Trimestral='mean', Desvio_Padrao='std'
    ).reset_index().sort_values(by='Total_Despesas', ascending=False)
    
    df_agregado['Desvio_Padrao'] = df_agregado['Desvio_Padrao'].fillna(0)
    df_agregado.to_csv(ARQUIVO_SAIDA, index=False, sep=';', encoding='utf-8')
    
    # Arquivo SQL (Agora com Modalidade!)
    df_completo.to_csv(ARQUIVO_COMPLETO_SQL, index=False, sep=';', encoding='utf-8')
    
    print(f"\n✅ SUCESSO! '{ARQUIVO_COMPLETO_SQL}' atualizado com a coluna Modalidade.")

if __name__ == "__main__":
    executar_missao_2()