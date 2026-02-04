import pandas as pd
from sqlalchemy import create_engine, text

# Conexão com nosso banco SQLite
DB_URL = "sqlite:///banco_teste.db"
engine = create_engine(DB_URL)

def executar_analise():
    print("--- INICIANDO ANÁLISE DE DADOS (MISSÃO 3.4 - FINAL) ---")

    # --- QUERY 1: CRESCIMENTO DAS DESPESAS ---
    print("\n📊 1. Top 5 Operadoras com maior crescimento de despesas (1T vs 3T):")
    
    query1 = """
    WITH despesas_1t AS (
        SELECT registro_ans, SUM(valor) as total_1t
        FROM despesas 
        WHERE trimestre = '1T'  -- <--- CORRIGIDO: Agora busca '1T'
        GROUP BY registro_ans
    ),
    despesas_3t AS (
        SELECT registro_ans, SUM(valor) as total_3t
        FROM despesas 
        WHERE trimestre = '3T'  -- <--- CORRIGIDO: Agora busca '3T'
        GROUP BY registro_ans
    )
    SELECT 
        o.razao_social,
        d1.total_1t,
        d3.total_3t,
        ROUND(((d3.total_3t - d1.total_1t) / d1.total_1t) * 100, 2) as crescimento_percentual
    FROM despesas_1t d1
    JOIN despesas_3t d3 ON d1.registro_ans = d3.registro_ans
    JOIN operadoras o ON d1.registro_ans = o.registro_ans
    WHERE d1.total_1t > 500000 -- Filtro: Ignora operadoras pequenas para focar nas relevantes
    ORDER BY crescimento_percentual DESC
    LIMIT 5;
    """
    try:
        df1 = pd.read_sql(query1, engine)
        pd.options.display.float_format = '{:,.2f}'.format
        print(df1.to_string(index=False))
    except Exception as e:
        print(f"Erro na Query 1: {e}")

    # --- QUERY 2: DISTRIBUIÇÃO POR UF ---
    print("\n📊 2. Top 5 Estados com maiores despesas totais:")
    
    query2 = """
    SELECT 
        o.uf,
        SUM(d.valor) as total_despesas,
        COUNT(DISTINCT o.registro_ans) as qtd_operadoras,
        ROUND(SUM(d.valor) / COUNT(DISTINCT o.registro_ans), 2) as media_por_operadora
    FROM despesas d
    JOIN operadoras o ON d.registro_ans = o.registro_ans
    WHERE o.uf != 'INDEFINIDO' 
    GROUP BY o.uf
    ORDER BY total_despesas DESC
    LIMIT 5;
    """
    try:
        df2 = pd.read_sql(query2, engine)
        print(df2.to_string(index=False))
    except Exception as e:
        print(f"Erro na Query 2: {e}")

    # --- QUERY 3: ACIMA DA MÉDIA ---
    print("\n📊 3. Operadoras 'Gastonas' (Acima da média em >= 2 trimestres):")
    
    query3 = """
    WITH medias_trimestrais AS (
        SELECT trimestre, AVG(valor) as media_mercado
        FROM despesas
        GROUP BY trimestre
    ),
    operadoras_acima AS (
        SELECT 
            d.registro_ans,
            d.trimestre
        FROM despesas d
        JOIN medias_trimestrais m ON d.trimestre = m.trimestre
        WHERE d.valor > m.media_mercado
    )
    SELECT 
        o.razao_social,
        COUNT(*) as qtd_trimestres_acima
    FROM operadoras_acima oa
    JOIN operadoras o ON oa.registro_ans = o.registro_ans
    GROUP BY o.razao_social
    HAVING qtd_trimestres_acima >= 2
    ORDER BY qtd_trimestres_acima DESC, o.razao_social
    LIMIT 10;
    """
    try:
        df3 = pd.read_sql(query3, engine)
        print(df3.to_string(index=False))
    except Exception as e:
        print(f"Erro na Query 3: {e}")

if __name__ == "__main__":
    executar_analise()