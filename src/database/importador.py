import pandas as pd
from sqlalchemy import create_engine, text
import os

# --- CONFIGURAÇÃO PARA SQLITE ---
# Troca a conexão complexa por um arquivo local simples
DB_URL = "sqlite:///banco_teste.db"

# Caminhos
CSV_COMPLETO = "dados_completos_para_sql.csv"
CSV_AGREGADO = "despesas_agregadas.csv"
SQL_SCHEMA   = "./src/database/schema.sql"

def criar_banco_e_tabelas():
    print("--- 1. Preparando Banco de Dados (SQLite) ---")
    try:
        engine = create_engine(DB_URL)
        
        # Lê o script SQL
        with open(SQL_SCHEMA, 'r', encoding='utf-8') as f:
            sql_script = f.read()
            
        with engine.connect() as conn:
            # O SQLite executa scripts múltiplos com executescript (via raw connection)
            # ou podemos dividir comando por comando. Vamos dividir por ';'
            comandos = sql_script.split(';')
            
            for cmd in comandos:
                if cmd.strip():
                    conn.execute(text(cmd))
            
            conn.commit()
            print("✅ Tabelas criadas com sucesso no arquivo 'banco_teste.db'.")
            return engine
            
    except Exception as e:
        print(f"❌ Erro ao criar tabelas: {e}")
        return None

def importar_dados(engine):
    print("\n--- 2. Importando Dados ---")
    
    # A. OPERADORAS
    print("Importando operadoras...")
    df = pd.read_csv(CSV_COMPLETO, sep=';', encoding='utf-8')
    df_ops = df[['RegistroANS', 'CNPJ', 'RazaoSocial', 'Modalidade', 'UF']].drop_duplicates(subset=['RegistroANS'])
    df_ops.columns = ['registro_ans', 'cnpj', 'razao_social', 'modalidade', 'uf']
    
    # Convertendo para string para garantir compatibilidade com TEXT do SQLite
    df_ops['registro_ans'] = df_ops['registro_ans'].astype(str)
    
    df_ops.to_sql('operadoras', engine, if_exists='append', index=False, chunksize=1000)
    print(f"✅ {len(df_ops)} operadoras salvas.")
    
    # B. DESPESAS
    print("Importando despesas (pode demorar alguns segundos)...")
    df_desp = df[['RegistroANS', 'Ano', 'Trimestre', 'DESCRICAO', 'Valor']].copy()
    df_desp.columns = ['registro_ans', 'ano', 'trimestre', 'descricao', 'valor']
    df_desp['registro_ans'] = df_desp['registro_ans'].astype(str)
    
    df_desp.to_sql('despesas', engine, if_exists='append', index=False, chunksize=1000)
    print(f"✅ {len(df_desp)} despesas salvas.")
    
    # C. AGREGADOS
    print("Importando tabela de desempenho...")
    if os.path.exists(CSV_AGREGADO):
        df_agg = pd.read_csv(CSV_AGREGADO, sep=';', encoding='utf-8')
        df_agg.columns = ['razao_social', 'uf', 'total_despesas', 'media_trimestral', 'desvio_padrao']
        df_agg.to_sql('desempenho_operadora', engine, if_exists='append', index=False)
        print(f"✅ Tabela de desempenho salva.")

if __name__ == "__main__":
    # Remove o banco antigo se existir para começar limpo
    if os.path.exists("banco_teste.db"):
        os.remove("banco_teste.db")
        
    engine = criar_banco_e_tabelas()
    if engine:
        importar_dados(engine)
        print("\n🚀 SUCESSO! Banco de dados pronto para análise.")