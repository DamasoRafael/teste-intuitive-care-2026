from sqlalchemy import create_engine, text

# Conecta no arquivo do banco que está nesta mesma pasta
engine = create_engine("sqlite:///banco_teste.db")

with engine.connect() as conn:
    print("--- Verificando o formato da coluna TRIMESTRE ---")
    # Pega todos os valores únicos que existem nessa coluna
    result = conn.execute(text("SELECT DISTINCT trimestre FROM despesas"))
    
    for row in result:
        print(f"Valor encontrado: {row}")