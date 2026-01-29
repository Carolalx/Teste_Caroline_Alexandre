# 🏥 Teste_ANS – Integração com API Pública da ANS

[![Python](https://img.shields.io/badge/python-3.12-blue)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-10%2B-blue)](https://www.postgresql.org/)

> Implementação das Etapas 1, 2 e 3 do teste técnico da ANS: integração com API pública, normalização, consolidação, validação, enriquecimento e análise de dados.

---

## **📋 Descrição do Projeto**

Este projeto implementa um pipeline de processamento de dados (ETL) dividido em três etapas fundamentais:

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

---

## **🛠 Tecnologias e Bibliotecas**

- **Linguagem:** Python 3.12
- **Bibliotecas:** `pandas`, `requests`, `beautifulsoup4`, `urllib3`
- **Banco de Dados:** PostgreSQL > 10
- **Modelagem:** Relacional com Chaves Estrangeiras (FK)

---

## **📂 Estrutura de Pastas**

```text
Teste_ANS/
│
├── src/
│   ├── main.py           # Ingestão (Etapa 1)
│   ├── transform.py      # Enriquecimento e Agregação (Etapa 2)
│   └── utils.py          # Funções auxiliares e validações
│
├── db/
│   ├── create_tables.sql # DDL para criação das tabelas
│   ├── load_data.sql     # Comandos de correção de escala e índices
│   └── analytics.sql     # Queries analíticas solicitadas
│
├── data/                 # Pasta local para armazenamento de CSVs (ignorada pelo Git)
│
├── requirements.txt      # Dependências do projeto
├── .gitignore            # Filtro de arquivos para o repositório
└── README.md             # Documentação do projeto
```

---

## **🚀 Como Executar**

1. Criar virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

```


2. Processamento de Dados:

```bash
python src/etapa1_ingestao.py
python src/etapa2_enriquecimento.py
```


3. Banco de Dados:
    - Execute o script db/create_tables.sql no seu cliente PostgreSQL.
    - Realize a importação dos CSVs (Cadastro primeiro, depois Despesas).
    - Execute os UPDATEs de correção de escala contidos no db/load_data.sql.

**O CSV consolidado será gerado em:**
`data/consolidado/consolidado_despesas.zip`

## 💡 Decisões Técnicas e Trade-offs

- **Tratamento de inconsistências:**
    Durante a carga, identificou-se que 11 operadoras (ex: Registro 350141) possuíam lançamentos financeiros mas não constavam no cadastro de "Ativas". Optou-se pelo uso de LEFT JOIN e remoção de CONSTRAINTS rígidas para garantir que nenhum dado financeiro fosse perdido.
- **Correção de Escala Decimal:** 
    Devido ao comportamento de importação de alguns clientes SQL que ignoram o separador decimal, foi aplicado um saneamento via SQL (SET valor = valor / 100) para garantir a precisão dos trilhões para a escala correta de milhões/bilhões.
- **Performance:** 
    Performance: O processamento em Python utiliza o pandas com mapeamento de tipos otimizados, permitindo o tratamento de milhões de linhas em segundos em hardware convencional.

## 📊 Resultados Finais

- **Total de registros processados: 2.163.924** 
- **Operadoras cadastradas: 1.110**
- **Integridade: Dados financeiros 100% preservados, incluindo contas com valores negativos (estornos contábeis).** 



## 👩‍💻 Autora
- Caroline Alexandre  
- [GitHub](https://github.com/Carolalx)