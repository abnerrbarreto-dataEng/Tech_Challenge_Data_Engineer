# Pipeline de Dados de Cervejarias - Arquitetura Medallion

## Descrição do Projeto

Este projeto implementa um pipeline de dados completo para coletar, processar e analisar informações sobre cervejarias utilizando a API Open Brewery DB. O objetivo é demonstrar habilidades em engenharia de dados, seguindo a arquitetura Medallion (Bronze, Silver e Gold), com orquestração usando Apache Airflow, processamento com PySpark e armazenamento em um data lake simulado com MinIO.

O pipeline extrai dados brutos da API, transforma-os em formatos otimizados e gera agregações analíticas, tudo de forma automatizada e escalável.

## Arquitetura do Data Lake (Medallion)

A arquitetura Medallion divide o data lake em três camadas principais:

### Camada Bronze (Raw Data)
- **Propósito**: Armazenamento dos dados brutos exatamente como recebidos da fonte.
- **Formato**: JSON nativo da API.
- **Localização**: `s3a://brewery-datalake/bronze/breweries/{data}/breweries_raw.json`

```plaintext
Diagrama da Camada Bronze:

+---------------------+
|   API Open Brewery  |
|       DB            |
+---------------------+
          |
          v
+---------------------+
|   Extração          |
|   (extract_breweries.py) |
+---------------------+
          |
          v
+---------------------+
|   Camada Bronze     |
|   - Formato: JSON   |
|   - Dados brutos    |
|   - Local: MinIO    |
+---------------------+
```

![MinIO - Camada Bronze](images/MinIO_Camada_bronze.png)

### Camada Silver (Curated Data)
- **Propósito**: Dados transformados e limpos, prontos para análise.
- **Transformações**:
  - Padronização de campos nulos (ex.: `state_province` usa `state` como fallback).
  - Remoção de duplicatas por ID.
  - Conversão para formato columnar Parquet.
- **Particionamento**: Por localização (`state_province`).
- **Formato**: Parquet.
- **Localização**: `s3a://brewery-datalake/silver/breweries/{data}/`

```plaintext
Diagrama da Camada Silver:

+---------------------+
|   Camada Bronze     |
|   (JSON)            |
+---------------------+
          |
          v
+---------------------+
|   Transformação     |
|   (transform_silver.py) |
|   - Limpeza de dados |
|   - Deduplicação     |
|   - Particionamento  |
+---------------------+
          |
          v
+---------------------+
|   Camada Silver     |
|   - Formato: Parquet|
|   - Particionado por |
|     state_province  |
+---------------------+
```

![MinIO - Camada Silver](images/MinIO_Camada_silver.png)

### Camada Gold (Aggregated Data)
- **Propósito**: Dados agregados para relatórios e análises rápidas.
- **Agregações**: Contagem de cervejarias por tipo e localização.
- **Formato**: Parquet.
- **Localização**: `s3a://brewery-datalake/gold/brewery_summary/{data}/`

```plaintext
Diagrama da Camada Gold:

+---------------------+
|   Camada Silver     |
|   (Parquet)         |
+---------------------+
          |
          v
+---------------------+
|   Agregação         |
|   (aggregate_gold.py)|
|   - Contagem por     |
|     tipo e local    |
+---------------------+
          |
          v
+---------------------+
|   Camada Gold       |
|   - Formato: Parquet|
|   - Dados agregados |
+---------------------+
```

![MinIO - Camada Gold](images/MinIO_Camada_gold.png)

## Tecnologias Utilizadas

- **Orquestração**: Apache Airflow 2.8.1
- **Processamento**: PySpark 3.5.0
- **Armazenamento**: MinIO (simulando S3)
- **Banco de Dados**: PostgreSQL (para metadados do Airflow)
- **Linguagem**: Python 3.8
- **Containerização**: Docker e Docker Compose
- **Bibliotecas**: requests, minio, pyspark

## Pré-requisitos

Antes de executar o projeto, certifique-se de que sua máquina possui:

- **Docker**: Versão 20.10 ou superior.
- **Docker Compose**: Versão 2.0 ou superior.
- **Git**: Para clonar o repositório.
- **Pelo menos 4GB de RAM disponível** (recomendado 8GB para melhor performance).
- **Espaço em disco**: Pelo menos 5GB livres para containers e dados.

Se você não tiver Docker instalado, siga as instruções oficiais:
- [Instalar Docker no Windows](https://docs.docker.com/desktop/install/windows-install/)
- [Instalar Docker no macOS](https://docs.docker.com/desktop/install/mac-install/)
- [Instalar Docker no Linux](https://docs.docker.com/engine/install/)

## Instalação e Configuração

1. **Clone o repositório**:
   ```bash
   git clone https://github.com/abnerrbarreto-dataEng/Tech_Challenge_Data_Engineer.git
   cd Tech_Challenge_Data_Engineer
   ```

2. **Verifique os arquivos**:
   - Certifique-se de que todos os arquivos estão presentes, especialmente `docker-compose.yaml`, `Dockerfile` e as pastas `dags/`, `scripts/`.

3. **Configure variáveis de ambiente (opcional)**:
   - O projeto usa valores padrão, mas você pode ajustar no `docker-compose.yaml` se necessário.

## Executando Testes

A solução inclui testes automatizados que validam a integridade dos dados (paginação, transformações e agregações). Para rodar os testes:

```bash
pip install -r requirements.txt
pytest
```

## Como Executar

### 1. Subir os Containers

Execute o comando abaixo na raiz do projeto:

```bash
docker-compose up --build
```

Este comando irá:
- Construir a imagem do Airflow com PySpark e dependências.
- Iniciar os serviços: Airflow Webserver, Scheduler, Worker, PostgreSQL e MinIO.

```plaintext
Exemplo de Comando para Subir Containers:

No terminal, execute:
docker-compose up --build

Isso irá:
- Construir a imagem do Airflow
- Iniciar serviços: Airflow, PostgreSQL, MinIO
- Aguardar saúde dos serviços

Output esperado:
Creating network...
Building airflow...
Starting postgres...
Starting minio...
Starting airflow-webserver...

Acesse: http://localhost:8080
```

![Docker Desktop - Containers](images/Docker_Images.png)

A primeira execução pode levar alguns minutos devido ao download das imagens e construção.

### 2. Acessar o Airflow

- **URL**: http://localhost:8080
- **Usuário**: airflow
- **Senha**: airflow

Na interface do Airflow, você verá o DAG `brewery_data_pipeline`.

```plaintext
Interface do Airflow:

- URL: http://localhost:8080
- Usuário: airflow
- Senha: airflow

Na tela inicial, você verá:
- Lista de DAGs
- Status: Paused/Running
- Últimas execuções

Para o DAG 'brewery_data_pipeline':
- Clique para ver detalhes
- Botão 'Trigger DAG' para executar
```

![Airflow - Tela Inicial](images/Airflow_tela_Inicial.png)

![Airflow - DAG](images/Airflow_DAG.png)

### 3. Executar o Pipeline

- No Airflow UI, ative o DAG clicando no botão de toggle.
- Clique em "Trigger DAG" para executar manualmente, ou aguarde o agendamento diário.

O pipeline executará as tarefas em sequência: extração, transformação e agregação.

### 4. Verificar os Dados

Acesse o MinIO em http://localhost:9001 (credenciais: minioadmin/minioadmin) para visualizar os arquivos nas camadas Bronze, Silver e Gold.

![MinIO - Visão Geral de Camadas](images/MinIO_Camadas.png)

```plaintext
Interface do MinIO:

- URL: http://localhost:9001
- Usuário: minioadmin
- Senha: minioadmin

Buckets:
- brewery-datalake
  - bronze/
    - breweries/
      - {data}/breweries_raw.json
  - silver/
    - breweries/
      - {data}/ (particionado)
  - gold/
    - brewery_summary/
      - {data}/ (agregado)
```

### 5. Parar os Containers

Para parar tudo:

```bash
docker-compose down
```

Para remover volumes também:

```bash
docker-compose down -v
```

## Estrutura do Projeto

```
brewery-data-pipeline/
├── dags/
│   └── brewery_pipeline.py      # DAG do Airflow definindo o pipeline
├── scripts/
│   ├── extract_breweries.py     # Script para extrair dados da API e salvar na Bronze
│   ├── transform_silver.py      # Script PySpark para transformar Bronze em Silver
│   ├── aggregate_gold.py        # Script PySpark para agregar Silver em Gold
│   └── run_pyspark.sh           # Script shell para executar PySpark
├── logs/                        # Logs das execuções do Airflow
├── tests/                       # Suíte de testes automatizados (pytest)
├── images/                      # Imagens para documentação
├── docker-compose.yaml          # Configuração dos serviços Docker
├── Dockerfile                   # Imagem customizada do Airflow
├── requirements.txt             # Dependências Python
└── README.md                    # Esta documentação
```

- **dags/**: Contém o arquivo de definição do pipeline no Airflow.
- **scripts/**: Scripts Python e shell que executam as transformações de dados.
- **logs/**: Armazena logs das execuções do pipeline.

## Explicação do Código e Tratamento de Erros

### Pipeline Principal (brewery_pipeline.py)

O DAG define três tarefas principais:

1. **extract_bronze_layer**: Executa `extract_breweries.py` para buscar dados da API.
2. **transform_silver_layer**: Executa `transform_silver.py` via PySpark.
3. **aggregate_gold_layer**: Executa `aggregate_gold.py` via PySpark.

Cada tarefa tem configuração de retry (1 tentativa) e dependências sequenciais.

### Extração de Dados (extract_breweries.py)

- **Funcionalidade**: Faz requisições paginadas à API Open Brewery DB, coletando todas as cervejarias.
- **Tratamento de Erros**:
  - Captura erros HTTP e de conexão, relançando exceções para o Airflow.
  - Valida JSON recebido.
  - Valida integridade dos registros (campos obrigatórios como `id`, `name` e `brewery_type`).
  - Verifica existência do bucket MinIO antes de salvar.
- **Exemplo de Execução**: Salva dados como JSON no MinIO.

```plaintext
Fluxo de Extração:

1. Conectar à API Open Brewery DB
2. Paginar requisições (50 por página)
3. Coletar todos os dados
4. Salvar como JSON no MinIO (Bronze)

Tratamento de Erros:
- Retry em falhas HTTP
- Validação de JSON
- Logs detalhados
```

### Transformação Silver (transform_silver.py)

- **Funcionalidade**: Lê JSON da Bronze, aplica transformações e salva como Parquet particionado.
- **Tratamento de Erros**:
  - Verifica se há dados na Bronze.
  - Trata campos nulos com coalesce.
  - Remove duplicatas.
  - Relança exceções para falha no pipeline.
- **Exemplo**: Dados particionados por estado/província.

```plaintext
Transformação Silver:

1. Ler JSON da Bronze
2. Coalesce campos nulos (state_province)
3. Remover duplicatas por ID
4. Converter para Parquet
5. Particionar por state_province
6. Salvar no MinIO

Exemplo de Transformação:
- Campo nulo -> Usar fallback
- Deduplicação: Manter único por ID
```

### Agregação Gold (aggregate_gold.py)

- **Funcionalidade**: Agrega contagens por tipo de cervejaria e localização.
- **Tratamento de Erros**:
  - Verifica dados na Silver.
  - Agrupa e conta, ordenando resultados.
  - Salva em Parquet.
- **Exemplo**: Tabela com quantidades por tipo e local.

```plaintext
Agregação Gold:

1. Ler Parquet da Silver
2. Agrupar por brewery_type e state_province
3. Contar quantidade (qtd_breweries)
4. Ordenar resultados
5. Salvar como Parquet no MinIO

Exemplo de Saída:
brewery_type | state_province | qtd_breweries
micro        | California     | 150
brewpub      | Texas          | 80
```

### Tratamento Geral de Erros

- **Retries**: Airflow tenta novamente tarefas falhadas (1 vez por padrão).
- **Logs**: Todos os scripts logam mensagens detalhadas para debug.
- **Exceções**: Erros são relançados para que o Airflow marque tarefas como falhadas.
- **Validações**: Scripts verificam existência de dados antes de processar.

## Monitoramento e Alertas

Para garantir a saúde e confiabilidade do pipeline, algumas sugestões de processos de monitoramento incluem observar de perto os indicadores de performance e possíveis pontos de falha. Por exemplo, podemos acompanhar métricas como o tempo de execução de cada tarefa, o volume de dados processados e a taxa de sucesso das operações. Em relação a questões de qualidade dos dados, é importante verificar regularmente a integridade das informações extraídas, como a presença de campos obrigatórios, a consistência dos tipos de dados e a ausência de duplicatas inesperadas.

No caso de falhas no pipeline, podemos implementar alertas automáticos que notifiquem a equipe responsável sempre que uma tarefa não for concluída com sucesso, permitindo uma resposta rápida para investigar e corrigir problemas. Isso pode incluir notificações por email, mensagens em ferramentas de comunicação como Slack ou integrações com sistemas de monitoramento como Prometheus e Grafana.

Além disso, para detectar anomalias, podemos configurar thresholds para métricas críticas, como o número mínimo de registros esperados em cada camada, e alertar quando esses limites não forem atingidos. Uma prática útil é também manter logs detalhados e dashboards visuais que permitam visualizar o fluxo de dados em tempo real, facilitando a identificação precoce de gargalos ou inconsistências.
