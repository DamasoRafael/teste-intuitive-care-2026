from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from typing import List, Optional
import os

# --- CONFIGURAÇÃO ---
app = FastAPI(title="API Intuitive Care", description="Teste Técnico 2026")

# Habilita CORS (Permite que o Frontend Vue.js converse com este Backend Python)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Em produção, troque '*' pelo domínio do site
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Conexão com Banco (SQLite)
# O banco está na raiz, então voltamos duas pastas (../../)
DB_URL = "sqlite:///banco_teste.db" 
engine = create_engine(DB_URL)

# --- ROTAS ---

@app.get("/")
def home():
    return {"message": "API Online! Acesse /docs para ver a documentação."}

@app.get("/api/operadoras")
def listar_operadoras(
    page: int = 1, 
    limit: int = 10, 
    search: Optional[str] = None
):
    """
    Lista todas as operadoras com paginação e busca.
    Requisito: GET /api/operadoras (paginação: page, limit) [cite: 143]
    """
    offset = (page - 1) * limit
    
    with engine.connect() as conn:
        # Query Base
        sql = "SELECT registro_ans, cnpj, razao_social, uf FROM operadoras"
        params = {"limit": limit, "offset": offset}
        
        # Filtro de Busca (Requisito 4.3.1 - Busca Híbrida/Server-side)
        if search:
            sql += " WHERE razao_social LIKE :search OR cnpj LIKE :search"
            params["search"] = f"%{search}%"
            
        sql += " LIMIT :limit OFFSET :offset"
        
        result = conn.execute(text(sql), params)
        operadoras = [dict(row._mapping) for row in result]
        
        # Conta o total para o frontend saber quantas páginas existem
        sql_count = "SELECT COUNT(*) FROM operadoras"
        if search:
            sql_count += " WHERE razao_social LIKE :search OR cnpj LIKE :search"
        
        total = conn.execute(text(sql_count), params).scalar()

    return {
        "data": operadoras,
        "total": total,
        "page": page,
        "limit": limit
    }

@app.get("/api/operadoras/{identifier}")
def detalhes_operadora(identifier: str):
    """
    Busca por CNPJ ou Registro ANS.
    Requisito: GET /api/operadoras/{cnpj} [cite: 144]
    """
    with engine.connect() as conn:
        # Tenta achar por CNPJ ou RegistroANS
        query = """
            SELECT * FROM operadoras 
            WHERE cnpj = :id OR registro_ans = :id
        """
        result = conn.execute(text(query), {"id": identifier}).fetchone()
        
        if not result:
            raise HTTPException(status_code=404, detail="Operadora não encontrada")
            
        return dict(result._mapping)

@app.get("/api/operadoras/{identifier}/despesas")
def historico_despesas(identifier: str):
    """
    Retorna histórico de despesas.
    Requisito: GET /api/operadoras/{cnpj}/despesas [cite: 144]
    """
    with engine.connect() as conn:
        # 1. Acha o registro_ans primeiro (caso o user passe CNPJ)
        op = conn.execute(text("SELECT registro_ans FROM operadoras WHERE cnpj = :id OR registro_ans = :id"), {"id": identifier}).fetchone()
        
        if not op:
            raise HTTPException(status_code=404, detail="Operadora não encontrada")
        
        registro = op.registro_ans
        
        # 2. Busca as despesas
        query = """
            SELECT ano, trimestre, descricao, valor 
            FROM despesas 
            WHERE registro_ans = :reg
            ORDER BY ano DESC, trimestre DESC
        """
        result = conn.execute(text(query), {"reg": registro})
        return [dict(row._mapping) for row in result]

@app.get("/api/estatisticas")
def estatisticas_gerais():
    """
    Retorna estatísticas agregadas (Total, Média, Top 5).
    Requisito: GET /api/estatisticas [cite: 145]
    """
    with engine.connect() as conn:
        # Total Geral
        total = conn.execute(text("SELECT SUM(valor) FROM despesas")).scalar()
        
        # Média por Trimestre
        media = conn.execute(text("SELECT AVG(valor) FROM despesas")).scalar()
        
        # Top 5 Operadoras (Mesma lógica da Missão 3)
        top_5_query = """
            SELECT razao_social, total_despesas 
            FROM desempenho_operadora 
            ORDER BY total_despesas DESC 
            LIMIT 5
        """
        top_5 = [dict(row._mapping) for row in conn.execute(text(top_5_query))]
        
        # Top 5 Estados
        top_uf_query = """
            SELECT uf, SUM(valor) as total 
            FROM despesas d 
            JOIN operadoras o ON d.registro_ans = o.registro_ans 
            WHERE o.uf != 'INDEFINIDO'
            GROUP BY o.uf 
            ORDER BY total DESC 
            LIMIT 5
        """
        top_uf = [dict(row._mapping) for row in conn.execute(text(top_uf_query))]

    return {
        "total_geral": total,
        "media_geral": media,
        "top_operadoras": top_5,
        "distribuicao_uf": top_uf
    }