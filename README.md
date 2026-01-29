# 🏥 Teste_ANS – Integração com API Pública da ANS

[![Python](https://img.shields.io/badge/python-3.12-blue)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-10%2B-blue)](https://www.postgresql.org/)

> Implementação das Etapas 1, 2, 3 e 4 do teste técnico da ANS: integração com API pública, normalização, consolidação, validação, enriquecimento, análise de dados e interface web.

---

## **📋 Descrição do Projeto**

Este projeto implementa um pipeline de processamento de dados (ETL) dividido em quatro etapas fundamentais:

1. **Integração com API da ANS (Etapa 1)**
   - Download automatizado dos arquivos ZIP referentes aos últimos 3 trimestres disponíveis.
   - Extração e normalização dinâmica de colunas (tratando variações como `REG_ANS` vs `RegistroANS`).
   - Consolidação de mais de **2,1 milhões de registros** em um único CSV.

2. **Transformação e Agregação (Etapa 2)**
   - **Enriquecimento:** Cruzamento de dados financeiros com a base cadastral oficial da ANS via `RegistroANS`.
   - **Saneamento:** Tratamento de escala decimal e valores nulos.
   - **Cálculo Estatístico:** Geração de métricas de Total de Despesas, Média Trimestral e Desvio Padrão por Operadora e UF.
   - **Resultados:** Geração do arquivo `despesas_agregadas.csv` e compactação final no ZIP solicitado.

3. **Banco de Dados e Análise SQL (Etapa 3)**
   - Modelagem relacional no **PostgreSQL** utilizando o modelo Estrela (*Star Schema*).
   - Implementação de integridade referencial flexível para comportar inconsistências nativas da fonte.
   - Scripts de carga e queries analíticas para insights de mercado.

4. **API e Interface Web (Etapa 4)**
   - Backend em **FastAPI** fornecendo rotas para operadoras, detalhes, histórico e estatísticas.
   - Frontend em **Vue.js** exibindo tabela paginada, busca/filtro, gráficos e modal de detalhes.

---

## **🛠 Tecnologias e Bibliotecas**

- **Linguagem:** Python 3.12, JavaScript (Vue.js 2)
- **Bibliotecas Python:** `pandas`, `fastapi`, `uvicorn`, `pydantic`, `requests`
- **Bibliotecas JS:** `axios`, `vue`, `chart.js`
- **Banco de Dados:** PostgreSQL > 10 (opcional para Etapa 4)
- **Modelagem:** Relacional com Chaves Estrangeiras (FK)

---

## **📂 Estrutura de Pastas**

```text
Teste_Caroline_Alexandre/
│
├── src/                    # ETAPAS 1 e 2 (pipeline)
│   ├── main.py             # Ingestão (Etapa 1)
│   ├── transform.py        # Enriquecimento e agregações (Etapa 2)
│   └── utils.py
│
├── api/                    # ETAPA 4 (backend web)
│   └── main.py             # FastAPI (servidor)
│
├── frontend/               # ETAPA 4 (Vue.js)
│   └── index.html
│
├── db/                     # ETAPA 3
│   ├── create_tables.sql
│   ├── load_data.sql
│   └── analytics.sql
│
├── data/                   # CSVs gerados (ignorado pelo Git)
│
├── requirements.txt
├── .gitignore
└── README.md
```

---

## **🚀 Como Executar**

1. Criar virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

```


2. Processamento de Dados (ETAPAS 1 e 2)

```bash
python src/etapa1_ingestao.py
python src/etapa2_enriquecimento.py
```


3. Banco de Dados (ETAPA 3)
    - Execute o script db/create_tables.sql no PostgreSQL.
    - Importe os CSVs (Cadastro primeiro, depois Despesas).
    - Execute updates de correção de escala contidos em db/load_data.sql.
    - O CSV consolidado será gerado em:
    `data/despesas_agregadas.csv`

4. API e Frontend (ETAPA 4)
Rodar backend
```bash
cd Teste_Caroline_Alexandre
uvicorn api.main:app --reload
```

Abrir frontend
```bash
cd frontend
# abrir index.html no navegador (Chrome ou Firefox)
``` 

Testar API
    - Swagger: http://127.0.0.1:8000/docs
    - Redoc: https://127.0.0.1:8000/redoc


## 💡 Decisões Técnicas e Trade-offs

## ETAPAS 1-3

- **Tratamento de inconsistências:**
    Durante a carga, identificou-se que 11 operadoras (ex: Registro 350141) possuíam lançamentos financeiros mas não constavam no cadastro de "Ativas". Optou-se pelo uso de LEFT JOIN e remoção de CONSTRAINTS rígidas para garantir que nenhum dado financeiro fosse perdido.
- **Correção de Escala Decimal:** 
    Devido ao comportamento de importação de alguns clientes SQL que ignoram o separador decimal, foi aplicado um saneamento via SQL (SET valor = valor / 100) para garantir a precisão dos trilhões para a escala correta de milhões/bilhões.
- **Performance:** 
    Performance: O processamento em Python utiliza o pandas com mapeamento de tipos otimizados, permitindo o tratamento de milhões de linhas em segundos em hardware convencional.

## ETAPA 4 – API e Frontend
``` 
| Componente                  | Escolha          | Justificativa                                            |
|------------------------------|-----------------|----------------------------------------------------------|
| Backend Framework            | FastAPI         | Performance, validação automática, documentação integrada|
| Paginação                    | Offset-based    | Simples, eficiente para dataset < 2k registros           |
| Estatísticas                 | Calcular sempre | Dataset pequeno, simplicidade e consistência             |
| Estrutura de resposta        | Dados + metadados | Facilita frontend e paginação                          |
| Busca/Filtração              | Cliente         | Resposta instantânea, dataset pequeno                    |
| Gerenciamento de estado      | Props/Events    | Simples, suficiente para aplicação pequena               |
| Renderização tabela          | v-for           | Dataset pequeno, sem necessidade de virtual scroll       |
| Erros/loading/dados vazios   | Mensagens específicas e loading | Melhor UX e feedback claro               |
```

## 📊 Funcionalidades da Interface Web

    1. Tabela paginada de operadoras com RegistroANS, Razão Social, UF e TotalDespesas.
    2. Busca instantânea no cliente por RegistroANS ou Razão Social.
    3. Gráfico de distribuição de despesas por UF usando Chart.js.
    4. Modal de detalhes da operadora, exibindo histórico de despesas (Média Trimestral e Desvio Padrão).
    5. Tratamento de erros e loading: mensagens claras e feedback visual.    

## 📊 Resultados Finais

- **Total de registros processados: 2.163.924** 
- **Operadoras cadastradas: 1.110**
- **Integridade: Dados financeiros 100% preservados, incluindo contas com valores negativos (estornos contábeis).** 

## 📝 Documentação da API

**Coleção Postman incluída:** `/docs/postman_collection.json.`
Contém exemplos de requisições para todas as rotas, incluindo paginação, filtros, detalhes e estatísticas.

## 👩‍💻 Autora
- Caroline Alexandre  
- [GitHub](https://github.com/Carolalx)