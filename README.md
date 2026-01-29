# Teste Técnico - Intuitive Care

## Decisões Técnicas e Trade-offs
Os arquivos contábeis originais utilizam a chave primária REG_ANS, não o CNPJ. Para esta etapa de consolidação, mantivemos o REG_ANS e faremos o enriquecimento (join) para obter o CNPJ na etapa 2.2.

