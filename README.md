🧪 Teste Técnico — Intuitive Care

Candidato: Rafael Damaso
Vaga: Estágio em Desenvolvimento

📌 Visão Geral

Este projeto implementa um pipeline completo de ETL (Extract, Transform, Load), seguido por uma API em FastAPI e um frontend simples para visualização dos dados.

O objetivo é baixar arquivos públicos, tratar inconsistências, realizar enriquecimento via join, armazenar os dados em um banco relacional e disponibilizar consultas através de uma API.

🚀 Como Executar o Projeto
✅ Pré-requisitos

Python 3.8 ou superior

Navegador Web moderno

📦 Passo 1 — Instalação

Crie e ative um ambiente virtual, depois instale as dependências:

python -m venv venv

# Windows
.\venv\Scripts\activate

# Linux/Mac
source venv/bin/activate

pip install -r requirements.txt

🔄 Passo 2 — Pipeline ETL

Execute os scripts na ordem abaixo:

# 1. Baixar e consolidar os CSVs
python src/etl/main.py

# 2. Enriquecimento de dados (Join)
python src/etl/missao2.py

# 3. Importação para o banco de dados SQLite
python src/database/importador.py

▶️ Passo 3 — Executar a Aplicação

Inicie a API:

uvicorn src.api.main:app --reload


Depois, abra no navegador:

src/frontend/index.html

📚 Documentação da API

Após iniciar o servidor, a documentação automática estará disponível em:

http://localhost:8000/docs

🛠 Decisões Técnicas e Trade-offs
🗄 Banco de Dados — SQLite vs PostgreSQL

Foi utilizado SQLite durante o desenvolvimento.

Motivo:

Simplicidade de configuração

Evita dependência de servidor local

Problemas de permissão de porta durante testes

Impacto:
Como o projeto usa SQLAlchemy, a migração para PostgreSQL exige apenas alteração da string de conexão.

🌐 Frontend — Vue.js via CDN

Frontend desenvolvido em um único arquivo HTML utilizando Vue.js via CDN.

Motivo:

Evitar configuração de bundlers (Vite/Webpack)

Facilitar execução imediata

Foco na lógica do desafio

🔄 Tratamento de Dados (ETL)

Join por Registro ANS
Arquivos financeiros não possuem CNPJ. O relacionamento é feito via Registro ANS.

Dados Incompletos
Aproximadamente 4% das operadoras financeiras não aparecem no cadastro ativo.
Essas entradas foram mantidas e marcadas como:

Operadora Não Identificada


Padronização de Chave
Aplicado zfill(6) no Registro ANS para corrigir diferenças de formato:

123  -> 000123

⚡ Backend — FastAPI

Utilizado FastAPI por:

Alto desempenho

Tipagem forte

Validação automática

Swagger integrado

📂 Estrutura do Projeto
src/
│
├── api/          # Rotas FastAPI
├── etl/          # Scripts de ETL
├── database/     # Importação e modelos
├── frontend/     # Interface web
└── utils/        # Funções auxiliares

✅ Considerações Finais

O projeto prioriza:

Clareza de código

Reprodutibilidade

Facilidade de execução

Organização modular

