import requests
import os
import pandas as pd
import io
import urllib3

urllib3.disable_warnings()

# Configuração
URL_CADASTRO = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv"
ARQUIVO_SAIDA = "./downloads_ans/Relatorio_cadop.csv"

def baixar_cadastro():
    print(f"Baixando dados cadastrais de: {URL_CADASTRO}...")
    try:
        r = requests.get(URL_CADASTRO, verify=False, timeout=60)
        if r.status_code == 200:
            # Salva o arquivo no disco
            with open(ARQUIVO_SAIDA, 'wb') as f:
                f.write(r.content)
            print("✅ Download concluído!")
            return True
        else:
            print(f"❌ Erro {r.status_code}")
            return False
    except Exception as e:
        print(f"Erro: {e}")
        return False

def analisar_colunas():
    if os.path.exists(ARQUIVO_SAIDA):
        print("\nAnalisando colunas do arquivo cadastral...")
        try:
            # Tenta ler as primeiras 5 linhas para ver o cabeçalho
            # CSVs da ANS costumam usar ponto e vírgula (;) e encoding latin1
            df = pd.read_csv(ARQUIVO_SAIDA, sep=';', encoding='latin1', nrows=5)
            print("--- Colunas encontradas ---")
            for col in df.columns:
                print(f" - {col}")
        except Exception as e:
            print(f"Erro ao ler CSV: {e}")

if __name__ == "__main__":
    if baixar_cadastro():
        analisar_colunas()