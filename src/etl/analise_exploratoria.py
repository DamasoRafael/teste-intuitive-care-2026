import pandas as pd
import os

# Caminho do arquivo que você baixou
caminho_arquivo = "./downloads_ans/3T2023.csv"

# Verificação de segurança
if not os.path.exists(caminho_arquivo):
    print("ERRO: Arquivo não encontrado! Verifique se o nome na pasta é exatamente 3T2023.csv")
else:
    print("Lendo o arquivo... (Isso pode demorar alguns segundos)")
    
    try:
        # Padrão Governo Brasileiro: Separador ';' e encoding 'latin1' (ou 'cp1252')
        # on_bad_lines='skip' ignora linhas quebradas para não travar o script
        df = pd.read_csv(caminho_arquivo, sep=';', encoding='latin1', on_bad_lines='skip')
        
        print("\n--- SUCESSO! O ARQUIVO FOI LIDO ---")
        
        print(f"\n1. Quantidade de linhas e colunas: {df.shape}")
        
        print("\n2. Nomes das colunas encontradas:")
        for col in df.columns:
            print(f"   - {col}")
            
        print("\n3. Primeiras 5 linhas de dados:")
        print(df.head())
        
        # Tenta achar pistas sobre "Despesas"
        # Geralmente a coluna de descrição se chama 'DS_CONTA' ou 'DESCRICAO'
        coluna_descricao = None
        for col in df.columns:
            if "DESC" in col.upper() or "CONTA" in col.upper():
                coluna_descricao = col
                break
        
        if coluna_descricao:
            print(f"\n4. Exemplo de descrições na coluna '{coluna_descricao}':")
            print(df[coluna_descricao].sample(5).values) # Pega 5 aleatórios
            
    except Exception as e:
        print(f"Erro ao ler o CSV: {e}")