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
    """
    Transforma o registro em string de 6 dígitos (padrão ANS).
    Ex: 41465 -> "041465"
    """
    try:
        # Remove .0 se vier como float e converte para texto
        limpo = str(int(float(valor)))
        return limpo.zfill(6) # Adiciona zeros à esquerda
    except:
        return str(valor)

def executar_missao_2():
    print("--- INICIANDO MISSÃO 2: TRANSFORMAÇÃO E VALIDAÇÃO (V3) ---")
    
    if not os.path.exists(ARQUIVO_DESPESAS) or not os.path.exists(ARQUIVO_CADASTRO):
        print("❌ Erro: Arquivos não encontrados.")
        return

    # 1. CARREGAR DESPESAS
    print("Carregando despesas...")
    df_despesas = pd.read_csv(ARQUIVO_DESPESAS, sep=';', encoding='utf-8')
    
    # CORREÇÃO CRÍTICA: Removemos as colunas placeholders para evitar conflito no merge
    # Jogamos fora o CNPJ 'A_DEFINIR' para dar lugar ao CNPJ real
    df_despesas = df_despesas.drop(columns=['CNPJ', 'RazaoSocial'], errors='ignore')
    
    # Padroniza a chave
    df_despesas['RegistroANS'] = df_despesas['RegistroANS'].apply(padronizar_registro_ans)

    # 2. CARREGAR CADASTRO
    print("Carregando cadastro...")
    try:
        # Tenta UTF-8 primeiro
        df_cadastro = pd.read_csv(ARQUIVO_CADASTRO, sep=';', encoding='utf-8')
    except:
        # Se falhar, vai de Latin1
        df_cadastro = pd.read_csv(ARQUIVO_CADASTRO, sep=';', encoding='latin1')

    # Padroniza a chave no cadastro também (Nome da coluna no CSV é REGISTRO_OPERADORA)
    df_cadastro['RegistroANS'] = df_cadastro['REGISTRO_OPERADORA'].apply(padronizar_registro_ans)
    
    # Seleciona apenas o necessário
    df_cadastro_limpo = df_cadastro[['RegistroANS', 'CNPJ', 'Razao_Social', 'UF']].copy()
    
    print(f"Despesas: {len(df_despesas)} linhas | Cadastro: {len(df_cadastro_limpo)} operadoras")

    # 3. O JOIN (ENRIQUECIMENTO)
    print("Cruzando tabelas...")
    # Agora não haverá conflito de colunas
    df_completo = pd.merge(df_despesas, df_cadastro_limpo, on='RegistroANS', how='left')
    
    # Renomeia Razao_Social (do cadastro) para RazaoSocial (pedido no teste)
    df_completo = df_completo.rename(columns={'Razao_Social': 'RazaoSocial'})
    
    # Preenche vazios
    df_completo['RazaoSocial'] = df_completo['RazaoSocial'].fillna("OPERADORA NÃO IDENTIFICADA")
    df_completo['UF'] = df_completo['UF'].fillna("INDEFINIDO")
    
    # 4. DIAGNÓSTICO
    sem_match = df_completo[df_completo['CNPJ'].isna()]
    qtd_sem_match = len(sem_match)
    
    if qtd_sem_match > 0:
        print(f"⚠️ Atenção: {qtd_sem_match} registros não encontraram correspondência (Possível operadora inativa).")
    else:
        print("✅ Sucesso Absoluto! Todos os registros foram identificados.")
    
    # 5. AGREGAÇÃO
    print("Calculando estatísticas...")
    
    df_agregado = df_completo.groupby(['RazaoSocial', 'UF'])['Valor'].agg(
        Total_Despesas='sum',
        Media_Trimestral='mean',
        Desvio_Padrao='std'
    ).reset_index()
    
    df_agregado['Desvio_Padrao'] = df_agregado['Desvio_Padrao'].fillna(0)
    df_agregado = df_agregado.sort_values(by='Total_Despesas', ascending=False)
    
    print("\n--- TOP 5 MAIORES DESPESAS ---")
    pd.options.display.float_format = '{:.2f}'.format
    print(df_agregado.head(5))
    
    # 6. SALVAR
    # Arquivo 1: Agregado (Pedido no PDF Teste 2.3)
    df_agregado.to_csv(ARQUIVO_SAIDA, index=False, sep=';', encoding='utf-8')
    
    # Arquivo 2: Completo (Vamos usar na Missão 3 - Banco de Dados)
    df_completo.to_csv(ARQUIVO_COMPLETO_SQL, index=False, sep=';', encoding='utf-8')
    
    # Arquivo 3: Zip (Pedido no PDF)
    import zipfile
    with zipfile.ZipFile(f"Teste_Rafael_Damaso.zip", 'w', zipfile.ZIP_DEFLATED) as z:
        z.write(ARQUIVO_SAIDA)
    
    print(f"\n✅ SUCESSO! Arquivos '{ARQUIVO_SAIDA}' e '{ARQUIVO_COMPLETO_SQL}' gerados.")

if __name__ == "__main__":
    executar_missao_2()