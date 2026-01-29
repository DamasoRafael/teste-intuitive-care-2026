import pandas as pd
import os

def processar_arquivo(caminho_entrada):
    """
    Lê o arquivo bruto, filtra despesas e padroniza as colunas.
    """
    print(f"Processando: {caminho_entrada}")
    
    try:
        # 1. LEITURA COM CORREÇÕES
        # encoding='utf-8': Corrigimos o problema dos caracteres estranhos
        # sep=';': O separador é ponto e vírgula
        # decimal=',': Avisamos ao Pandas que a vírgula é o separador decimal (vital!)
        df = pd.read_csv(caminho_entrada, sep=';', encoding='utf-8', decimal=',')
        
        # 2. FILTRAGEM (O PDF pede "Despesas com Eventos/Sinistros")
        # Vamos buscar linhas onde a DESCRIÇÃO contém 'EVENTOS' ou 'SINISTROS'
        # case=False ignora maiúsculas/minúsculas
        termo_busca = 'EVENTOS|SINISTROS' 
        df_filtrado = df[df['DESCRICAO'].str.contains(termo_busca, case=False, na=False)].copy()
        
        print(f"Linhas originais: {len(df)} -> Linhas de Despesa: {len(df_filtrado)}")
        
        # 3. CRIAÇÃO DE COLUNAS (Ano e Trimestre)
        # Converte a coluna DATA para o formato de data do Python
        df_filtrado['DATA'] = pd.to_datetime(df_filtrado['DATA'])
        
        df_filtrado['Ano'] = df_filtrado['DATA'].dt.year
        # Lógica matemática para achar o trimestre: (Mês - 1) // 3 + 1
        df_filtrado['Trimestre'] = (df_filtrado['DATA'].dt.month - 1) // 3 + 1
        df_filtrado['Trimestre'] = df_filtrado['Trimestre'].astype(str) + 'T' # Formata como '3T'
        
        # 4. RENOMEAÇÃO E SELEÇÃO
        # O PDF pede: CNPJ, RazaoSocial, Trimestre, Ano, Valor
        # Trade-off: Não temos CNPJ nem RazaoSocial ainda, usamos REG_ANS e DESCRICAO
        df_final = df_filtrado.rename(columns={
            'REG_ANS': 'RegistroANS',
            'VL_SALDO_FINAL': 'Valor'
        })
        
        # Adicionamos colunas vazias para CNPJ e RazaoSocial (serão preenchidas na Missão 2)
        df_final['CNPJ'] = 'A_DEFINIR' 
        df_final['RazaoSocial'] = 'A_DEFINIR'
        
        # Seleciona apenas as colunas que importam
        colunas_desejadas = ['RegistroANS', 'CNPJ', 'RazaoSocial', 'Trimestre', 'Ano', 'Valor', 'DESCRICAO']
        return df_final[colunas_desejadas]
        
    except Exception as e:
        print(f"Erro ao processar arquivo: {e}")
        return None

if __name__ == "__main__":
    # Teste unitário
    caminho = "./downloads_ans/3T2023.csv"
    if os.path.exists(caminho):
        df_resultado = processar_arquivo(caminho)
        
        if df_resultado is not None:
            print("\n--- Amostra do Resultado Final ---")
            print(df_resultado.head())
            
            # Salva um teste para você conferir
            df_resultado.to_csv("teste_consolidado.csv", index=False, sep=';', encoding='utf-8')
            print("\nArquivo 'teste_consolidado.csv' gerado na raiz para conferência.")
    else:
        print("Arquivo de entrada não encontrado. Rode o main.py primeiro.")