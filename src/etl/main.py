import os # "diretório". Uso para manipular caminhos de arquivos e pastas
import pandas as pd # "planilhas". Uso para manipular dados tabulares
import sys # "sistema". Uso para manipular o path de importação
import requests # "navegador" do código. Acessa sites. Uso para bater na porta do site e pedir o arquivo
import zipfile # "tesouras". Ao inves de salvar arquivo .zip no disco, abre diretamente na ram
import io # "tesouras". Ao inves de salvar arquivo .zip no disco, abre diretamente na ram
import urllib3 # <--- Mudança aqui: Importação direta e limpa

# Adiciona o diretório atual ao path para conseguir importar o outro arquivo
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from transformacao import processar_arquivo

# --- CONFIGURAÇÃO DE SSL ---
# Desabilita o aviso de "InsecureRequestWarning" de forma limpa
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURAÇÕES DO PROJETO ---
BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"
DIR_TEMP = "./downloads_ans"
ARQUIVO_FINAL = "consolidado_despesas.csv"

# Lista dos trimestres que queremos
TRIMESTRES_ALVO = [
    ("2023", "3T2023"),
    ("2023", "2T2023"),
    ("2023", "1T2023")
]

def baixar_arquivo(ano, trimestre):
    """Baixa o ZIP e retorna o caminho do arquivo extraído de interesse"""
    variacoes_nome = [f"{trimestre.upper()}.zip", f"{trimestre.lower()}.zip"]
    
    for nome_arquivo in variacoes_nome:
        url = f"{BASE_URL}{ano}/{nome_arquivo}"
        print(f"[{trimestre}] Baixando de: {url}...")
        
        try:
            # verify=False ignora verificação SSL
            resp = requests.get(url, verify=False, timeout=120) # Aumentei timeout para 120s
            if resp.status_code == 200:
                os.makedirs(DIR_TEMP, exist_ok=True)
                with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                    z.extractall(DIR_TEMP)
                
                # Procura o CSV extraído
                for f in os.listdir(DIR_TEMP):
                    # Procura arquivo que tenha o nome do trimestre ou 'despesa'
                    if f.endswith('.csv') and (trimestre.upper() in f.upper() or trimestre.lower() in f.lower()):
                        return os.path.join(DIR_TEMP, f)
                
                # Fallback: pega o primeiro CSV que encontrar se o nome não bater
                csvs = [f for f in os.listdir(DIR_TEMP) if f.endswith('.csv')]
                if csvs:
                    return os.path.join(DIR_TEMP, csvs[0])
                    
            elif resp.status_code != 404:
                print(f"Erro {resp.status_code}")
                
        except Exception as e:
            print(f"Erro de conexão no download: {e}")
            
    return None

def executar_pipeline():
    print("--- INICIANDO PIPELINE DE ETL ---")
    lista_dfs = []
    
    for ano, tri in TRIMESTRES_ALVO:
        # 1. DOWNLOAD
        caminho_csv = baixar_arquivo(ano, tri)
        
        if caminho_csv:
            # 2. TRANSFORMAÇÃO
            print(f"[{tri}] Processando dados (isso pode demorar)...")
            df_trimestre = processar_arquivo(caminho_csv)
            
            if df_trimestre is not None:
                linhas_antes = len(df_trimestre)
                
                # Limpeza Extra: Remove valores zerados
                df_trimestre = df_trimestre[df_trimestre['Valor'] != 0.0]
                
                linhas_depois = len(df_trimestre)
                print(f"[{tri}] ✅ Sucesso! {linhas_depois} registros úteis (removidos {linhas_antes - linhas_depois} zerados).")
                
                lista_dfs.append(df_trimestre)
                
                # Remove o arquivo temporário CSV para limpar a pasta
                try:
                    os.remove(caminho_csv)
                except:
                    pass
            else:
                print(f"[{tri}] ❌ Falha na transformação.")
        else:
            print(f"[{tri}] ❌ Falha no download.")

    # 3. CONSOLIDAÇÃO
    if lista_dfs:
        print("\nConsolidando todos os trimestres...")
        df_final = pd.concat(lista_dfs, ignore_index=True)
        
        print(f"Total de registros processados: {len(df_final)}")
        
        # Salvando CSV
        df_final.to_csv(ARQUIVO_FINAL, index=False, sep=';', encoding='utf-8')
        print(f"✅ Arquivo CSV gerado: {ARQUIVO_FINAL}")
        
        # Salvando ZIP
        nome_zip = ARQUIVO_FINAL.replace('.csv', '.zip')
        with zipfile.ZipFile(nome_zip, 'w', zipfile.ZIP_DEFLATED) as z:
            z.write(ARQUIVO_FINAL)
        print(f"✅ Arquivo ZIP gerado: {nome_zip}")
        
    else:
        print("❌ Nenhum dado foi processado com sucesso.")

if __name__ == "__main__":
    executar_pipeline()