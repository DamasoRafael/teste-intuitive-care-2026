-- 1. Tabela de Dimensão: OPERADORAS
CREATE TABLE IF NOT EXISTS operadoras (
    registro_ans TEXT PRIMARY KEY,
    cnpj TEXT,
    razao_social TEXT,
    modalidade TEXT,
    uf TEXT
);

-- 2. Tabela de Fato: DESPESAS
CREATE TABLE IF NOT EXISTS despesas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    registro_ans TEXT,
    ano INTEGER,
    trimestre TEXT,
    descricao TEXT,
    valor REAL,
    FOREIGN KEY(registro_ans) REFERENCES operadoras(registro_ans)
);

-- 3. Tabela Agregada: PERFORMANCE_UF
CREATE TABLE IF NOT EXISTS desempenho_operadora (
    razao_social TEXT,
    uf TEXT,
    total_despesas REAL,
    media_trimestral REAL,
    desvio_padrao REAL
);

-- Índices (Otimização)
CREATE INDEX IF NOT EXISTS idx_despesas_registro ON despesas(registro_ans);
CREATE INDEX IF NOT EXISTS idx_despesas_ano_tri ON despesas(ano, trimestre);