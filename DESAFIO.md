# ✈️ Desafio: Lakehouse de Pontualidade da Aviação Civil Brasileira

> Lakehouse completo no Databricks com **Databricks Asset Bundles (DABs)**: ingestão multi-fonte, tratamento com qualidade, modelagem dimensional (Kimball) na camada gold e um modelo de ML para prever atrasos de voos.

**Início:** `____/____/______` · **Previsão de término:** `____/____/______`

---

## 📌 Contexto do negócio

A **AeroInsights** é uma consultoria que vende análises de pontualidade para aeroportos e companhias aéreas. Ela precisa de uma plataforma que responda perguntas como:

- Qual rota mais atrasa em dias de chuva?
- Quanto o preço do querosene e o câmbio se relacionam com cancelamentos?
- Qual a probabilidade de um voo específico atrasar mais de 15 minutos?

---

## 🗂️ Fontes de dados

| # | Fonte | Formato | Link principal |
|---|-------|---------|----------------|
| 1 | ANAC – Voo Regular Ativo (VRA) | CSV mensal | https://www.gov.br/anac/pt-br/assuntos/dados-e-estatisticas/historico-de-voos |
| 2 | Open-Meteo – Historical Weather API | API REST (JSON) | https://open-meteo.com/en/docs/historical-weather-api |
| 3 | OurAirports – cadastro de aeroportos | CSV | https://ourairports.com/data/ |
| 4 | Banco Central – API SGS | API REST (JSON/CSV) | https://dadosabertos.bcb.gov.br/ |
| 5 | ANP – preços de QAV (querosene de aviação) | CSV / planilhas | https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos |
| 6 | Cadastro de frota (sintético, CDC) | JSON gerado por você | — |

### 1. ANAC – Voo Regular Ativo (VRA)

Base com todos os voos regulares: horários previstos e realizados, atrasos, cancelamentos e justificativas.

- Página dos arquivos mensais: https://www.gov.br/anac/pt-br/assuntos/dados-e-estatisticas/historico-de-voos
- Página no portal de dados abertos da ANAC (metadados + CSVs): https://www.gov.br/anac/pt-br/acesso-a-informacao/dados-abertos/areas-de-atuacao/voos-e-operacoes-aereas/voo-regular-ativo-vra
- Catálogo no dados.gov.br: https://dados.gov.br/dados/conjuntos-dados/dadosabertos-areas-de-atuacao-voos-e-operacoes-aereas-voo-regular-ativo-vra
- Página antiga (útil para entender o histórico e a descrição de variáveis/justificativas): https://www.gov.br/anac/pt-br/assuntos/dados-e-estatisticas/historico-de-voos/historico-de-voos-descontinuada

**Por que é difícil:** encoding latin-1, separador `;`, nomes de colunas que mudaram ao longo dos anos (schema drift), mudança de regulamentação em abril/2020 (IAC 1504 → Resolução 191), meses com dados incompletos (ex.: período da Copa de 2014), horários locais em vários fusos, cancelados com campos "realizados" nulos.

> 💡 Automatize o download dos arquivos (script que lista os links da página e baixa para um Volume) em vez de baixar na mão. Isso já é um bom exercício.

### 2. Open-Meteo – Historical Weather API

Clima histórico horário por latitude/longitude. Gratuita para uso não comercial e sem API key.

- Documentação: https://open-meteo.com/en/docs/historical-weather-api
- Endpoint: `https://archive-api.open-meteo.com/v1/archive`
- Exemplo: `https://archive-api.open-meteo.com/v1/archive?latitude=-19.63&longitude=-43.96&start_date=2024-01-01&end_date=2024-01-31&hourly=temperature_2m,precipitation,wind_speed_10m,cloud_cover,visibility&timezone=UTC`

**Por que é difícil:** uma chamada por aeroporto × período, rate limit (consulte os termos de uso), necessidade de retry/backoff, JSON com arrays paralelos (`time[]`, `precipitation[]`...) que precisam ser "zipados" e explodidos, controle de watermark para ingestão incremental.

### 3. OurAirports

Cadastro mundial de aeroportos, atualizado diariamente, em domínio público.

- Página de dados e dicionário: https://ourairports.com/data/
- Download direto: https://davidmegginson.github.io/ourairports-data/airports.csv
- Pistas (opcional, para features): https://davidmegginson.github.io/ourairports-data/runways.csv
- Repositório: https://github.com/davidmegginson/ourairports-data

**Por que é difícil:** reconciliar códigos ICAO/IATA com os da ANAC (nem todos batem), aeroportos fechados ou duplicados, construir uma dimensão conformada usada como origem e destino.

### 4. Banco Central – API SGS

Séries temporais econômicas.

- Portal: https://dadosabertos.bcb.gov.br/
- Padrão de URL: `https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=json&dataInicial=dd/MM/aaaa&dataFinal=dd/MM/aaaa`
- Séries sugeridas:
  - `1` – Dólar (venda), diária
  - `10813` – Dólar (compra), diária
  - `433` – IPCA, mensal

**Por que é difícil:** desde março/2025 as consultas por período são limitadas a 10 anos por requisição (você precisa particionar as chamadas); datas em `dd/MM/aaaa`; granularidades diferentes (diária x mensal); dias sem cotação (fins de semana/feriados) que precisam de forward-fill ao juntar com a data do voo.

### 5. ANP – preços de querosene de aviação (QAV)

- Página de preços da ANP: https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos
- Anuário Estatístico em dados abertos (tabela 3.26 – preço médio do QAV por município): https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/anuario-estatistico-brasileiro-do-petroleo-gas-natural-e-biocombustiveis

**Por que é difícil:** layouts que mudam entre publicações, planilhas com cabeçalhos em várias linhas, granularidade grossa (anual/mensal, por município) que precisa ser alinhada a voos diários. Decida e documente como você vai "descer" essa granularidade sem inventar dados.

### 6. Cadastro de frota (sintético, com CDC)

Escreva um gerador em Python (`faker`) que simula o sistema de cadastro de aeronaves das companhias e grava arquivos JSON num Volume.

Campos mínimos: `aircraft_id`, `matricula`, `modelo`, `fabricante`, `assentos`, `companhia_icao`, `ano_fabricacao`, `operation` (`INSERT`/`UPDATE`/`DELETE`), `sequence_num`, `event_ts`.

Regras do gerador:
- [ ] Aeronaves trocam de companhia ao longo do tempo (gera histórico SCD2)
- [ ] ~2% de eventos duplicados
- [ ] ~2% de eventos fora de ordem
- [ ] Alguns `DELETE` de aeronaves aposentadas
- [ ] Modo "contínuo" (grava um arquivo a cada N segundos) para o desafio extra de streaming

> 💡 Opcional: use o Registro Aeronáutico Brasileiro (RAB) da ANAC como semente para matrículas e modelos reais.

---

## 📋 Requisitos por camada

### Bronze

Tudo no Unity Catalog, com dados brutos em Volumes e ingestão incremental: Auto Loader para arquivos e checkpoint de watermark para as APIs. **Nenhuma transformação de negócio nesta camada.** Guarde os metadados de ingestão (arquivo de origem, timestamp e hash da linha). O histórico da VRA deve ter pelo menos 3 anos, para ter volume real.

### Silver

Use Lakeflow Declarative Pipelines (o antigo DLT) com expectations. Escolha conscientemente entre `warn`, `drop` e `fail` e documente o porquê. Esta camada precisa de:

- deduplicação;
- tipagem correta;
- timezone padronizado (os horários da ANAC são locais e o Brasil tem vários fusos);
- a tabela de-para de justificativas;
- o cadastro de frota com `AUTO CDC` / `APPLY CHANGES` em SCD2;
- quarentena para registros inválidos, em vez de simplesmente descartá-los.

### Gold – modelagem dimensional (Kimball)

Defina a granularidade antes de escrever qualquer código. Sugestão mínima:

- `fato_voo`: um voo por data prevista, com métricas de atraso na partida e na chegada e flag de cancelamento;
- `fato_clima_aeroporto_hora`: fato periódico;
- `dim_data` e `dim_hora`;
- `dim_aeroporto`: conformada, usada como origem e destino (role-playing);
- `dim_companhia`;
- `dim_aeronave`: SCD2, com join ponto-no-tempo no fato;
- `dim_justificativa`.

Use surrogate keys e membros "desconhecidos" (`-1`) para chaves órfãs. Por cima, crie duas ou três tabelas agregadas ou métricas para consumo de BI.

### ML

O alvo é prever se um voo vai atrasar mais de 15 minutos na partida.

- Feature tables no Unity Catalog, com features como taxa histórica de atraso da rota nos últimos 30 dias, clima previsto na origem, horário, dia da semana, modelo da aeronave e preço do QAV.
- **Nada de vazamento de dados.** A feature de um voo só pode usar informações disponíveis antes dele. Use point-in-time lookups. Esse é o ponto mais difícil e mais valioso do desafio.
- Split temporal, não aleatório.
- Baseline simples (regressão logística) comparado com um gradient boosting, tudo rastreado no MLflow.
- Classes desbalanceadas: justifique a métrica escolhida (PR-AUC, recall etc.).
- Modelo registrado no UC com aliases `champion`/`challenger` e job de batch scoring diário que grava as previsões numa tabela gold.

## ⚙️ Requisitos de engenharia (DABs)

- Bundle com targets `dev` e `prod` e variáveis para catálogo e schema. O `dev` usa o modo `development` (prefixo por usuário).
- Pipelines, jobs, o experimento do MLflow e o modelo registrado declarados como recursos no bundle. **Nada criado na mão pela interface.**
- Job orquestrador com dependências entre tasks: ingestão das APIs → pipeline bronze→silver→gold → features → scoring.
- Código Python empacotado como wheel, com testes unitários em pytest para as funções de parsing e transformação.
- CI no GitHub Actions: `databricks bundle validate` e testes em todo PR; `deploy` em `prod` no merge da `main`.
- Segredos (se precisar) em secret scopes, nunca no código.

---

## 🏗️ Arquitetura

```
Fontes ──► Volumes (landing) ──► Bronze ──► Silver ──► Gold (estrela) ──► Features ──► Modelo ──► Previsões (gold)
                                   │          │            │
                              Auto Loader  Expectations  AI/BI Dashboard
                              + watermark  + quarentena
```

Catálogos sugeridos no Unity Catalog: `aero_dev` e `aero_prod`, com schemas `landing`, `bronze`, `silver`, `gold`, `ml`.

### Estrutura sugerida do repositório

```
.
├── databricks.yml
├── resources/
│   ├── jobs/
│   ├── pipelines/
│   └── ml/
├── src/
│   └── aero/              # pacote python (wheel)
│       ├── ingestion/
│       ├── transformations/
│       ├── features/
│       └── utils/
├── notebooks/             # exploração, nunca produção
├── pipelines/             # código dos Lakeflow Declarative Pipelines
├── tests/
├── .github/workflows/
├── docs/
│   ├── bus_matrix.md
│   ├── decisoes.md        # ADRs
│   └── dicionario_dados.md
└── DESAFIO.md
```

---

## ✅ Checklist de progresso

### Fase 0 – Fundação (semana 1)

- [ ] Workspace Databricks configurado (verificar se o Free Edition permite chamadas às APIs externas)
- [ ] Databricks CLI instalado e autenticado
- [ ] `databricks bundle init` e estrutura do repositório criada
- [ ] Targets `dev` (modo `development`) e `prod` definidos no `databricks.yml`
- [ ] Variáveis de bundle para catálogo e schemas
- [ ] Catálogos e schemas criados (via bundle ou script versionado, nunca na mão)
- [ ] Volumes de landing criados
- [ ] Pacote Python com `pyproject.toml` gerando wheel
- [ ] Secret scope criado (se alguma fonte precisar de credencial)
- [ ] GitHub Actions: `bundle validate` + pytest em todo PR
- [ ] `databricks bundle deploy -t dev` funcionando com um job "hello world"

### Fase 1 – Bronze (semanas 2 e 3)

- [ ] **VRA:** script de download dos CSVs mensais para o Volume
- [ ] **VRA:** Auto Loader com `schemaEvolutionMode` e `_rescued_data`
- [ ] **VRA:** carga de pelo menos 3 anos de histórico
- [ ] **Open-Meteo:** cliente com retry, backoff e controle de rate limit
- [ ] **Open-Meteo:** tabela de controle de watermark por aeroporto
- [ ] **OurAirports:** ingestão do `airports.csv` (snapshot diário)
- [ ] **BCB:** ingestão particionada respeitando o limite de 10 anos por chamada
- [ ] **ANP:** parser defensivo para as planilhas/CSVs de QAV
- [ ] **Frota CDC:** gerador escrito, testado e gravando no Volume
- [ ] Nenhuma regra de negócio aplicada na bronze
- [ ] Metadados de ingestão em todas as tabelas (`_source_file`, `_ingested_at`, `_row_hash`)
- [ ] Reprocessar um mês não duplica dados (idempotência)

### Fase 2 – Silver (semanas 4 e 5)

- [ ] Pipeline em Lakeflow Declarative Pipelines declarado no bundle
- [ ] Expectations em todas as tabelas, com a escolha de `warn`/`drop`/`fail` documentada
- [ ] Tabelas de quarentena para registros inválidos
- [ ] VRA: tipagem, deduplicação e padronização de colunas entre layouts antigos e novos
- [ ] VRA: conversão de horários locais para UTC (usando o fuso do aeroporto)
- [ ] VRA: cálculo de `atraso_partida_min` e `atraso_chegada_min`
- [ ] De-para de códigos de justificativa de atraso
- [ ] Clima: JSON achatado para granularidade aeroporto × hora
- [ ] Aeroportos: reconciliação ICAO/IATA ANAC × OurAirports, com relatório de não encontrados
- [ ] Câmbio: série diária completa com forward-fill
- [ ] Frota: `AUTO CDC` / `APPLY CHANGES` com SCD tipo 2, tratando duplicados e fora de ordem
- [ ] Testes unitários das funções de transformação

### Fase 3 – Gold / Modelagem dimensional (semana 6)

- [ ] Bus matrix desenhada em `docs/bus_matrix.md`
- [ ] Granularidade de cada fato escrita **antes** de codar
- [ ] `dim_data` e `dim_hora`
- [ ] `dim_aeroporto` (conformada, role-playing origem/destino)
- [ ] `dim_companhia`
- [ ] `dim_aeronave` (SCD2)
- [ ] `dim_justificativa`
- [ ] `fato_voo` (grão: 1 voo por data prevista) com join ponto-no-tempo na `dim_aeronave`
- [ ] `fato_clima_aeroporto_hora` (fato periódico)
- [ ] Surrogate keys e membro desconhecido (`-1`) para chaves órfãs
- [ ] Agregados para BI (ex.: pontualidade por rota × mês)
- [ ] Query de validação: top 10 rotas com maior atraso médio em dias com precipitação > 5 mm, por companhia

### Fase 4 – ML (semanas 7 e 8)

- [ ] Alvo definido: `atrasou_15min` (partida)
- [ ] Feature tables no Unity Catalog
- [ ] Features de histórico (taxa de atraso da rota/companhia/aeroporto nos últimos N dias)
- [ ] Features de clima na origem e no destino
- [ ] Features de calendário (hora, dia da semana, feriado, alta temporada)
- [ ] Features de aeronave e contexto econômico (câmbio, QAV)
- [ ] **Point-in-time lookups: nenhuma feature usa informação posterior ao horário previsto do voo**
- [ ] Split temporal (treino / validação / teste por período)
- [ ] Baseline: regressão logística
- [ ] Modelo principal: gradient boosting (LightGBM ou XGBoost)
- [ ] Tratamento do desbalanceamento, com a métrica escolhida justificada (PR-AUC, recall, F1)
- [ ] Experimento do MLflow declarado como recurso no bundle
- [ ] Experimentos rastreados no MLflow
- [ ] Modelo registrado declarado como recurso no bundle
- [ ] Modelo registrado no UC com aliases `champion` e `challenger`
- [ ] Job de batch scoring diário gravando previsões na gold
- [ ] Análise de importância de features (SHAP ou similar)

### Fase 5 – Orquestração e produção (semana 9)

- [ ] Job orquestrador no bundle: ingestão APIs → pipeline → features → scoring
- [ ] Notificações de falha configuradas
- [ ] Deploy em `prod` via GitHub Actions no merge da `main`
- [ ] `bundle deploy` em workspace limpo sobe tudo do zero
- [ ] Revisão final: nenhum recurso criado na mão pela interface
- [ ] README com como rodar, arquitetura e decisões
- [ ] Dicionário de dados da gold

---

## 🚀 Desafios extras

- [ ] Dashboard AI/BI com as métricas gold, também declarado no bundle
- [ ] Lakehouse Monitoring para drift do modelo e das features
- [ ] Silver em modo contínuo consumindo o gerador de CDC em tempo real
- [ ] Liquid clustering na `fato_voo`, medindo tempo de query antes e depois
- [ ] Data contracts / testes de schema na fronteira bronze → silver
- [ ] Genie space em cima da gold para perguntas em linguagem natural

---

## 🧠 Decisões técnicas (ADRs)

Registre aqui cada decisão relevante com contexto, opções consideradas e escolha.

| # | Data | Decisão | Motivo |
|---|------|---------|--------|
| 001 | | | |
| 002 | | | |

---

## 📝 Diário de bordo

| Data | O que fiz | Dificuldades | Próximo passo |
|------|-----------|--------------|---------------|
| | | | |

---

## 📚 Referências úteis

- Databricks Asset Bundles: https://docs.databricks.com/en/dev-tools/bundles/index.html
- Lakeflow Declarative Pipelines: https://docs.databricks.com/en/dlt/index.html
- Auto Loader: https://docs.databricks.com/en/ingestion/cloud-object-storage/auto-loader/index.html
- Feature Engineering no Unity Catalog: https://docs.databricks.com/en/machine-learning/feature-store/index.html
- MLflow no Databricks: https://docs.databricks.com/en/mlflow/index.html
- Kimball – *The Data Warehouse Toolkit* (capítulos sobre granularidade, SCD e role-playing dimensions)

---

## 💡 Dicas

1. Comece com **um mês** da VRA de ponta a ponta antes de carregar o histórico todo.
2. Notebooks são para explorar; código de produção vai para `src/` com testes.
3. O ponto mais valioso do ML é evitar vazamento de dados. Se o modelo ficar bom demais, desconfie.
4. Escreva as decisões enquanto toma. Daqui a um mês você não vai lembrar por que escolheu `drop` em vez de `warn`.
