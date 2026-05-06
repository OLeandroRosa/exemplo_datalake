# Datalake — Pipeline Exemplo Kaggle (Olist)

Pipeline de dados completo com arquitetura **Medallion (Bronze → Silver → Gold)**, orquestrado pelo **Apache Airflow** e processado com **PySpark + Delta Lake**. Os dados de origem são do dataset público da [Olist no Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

---

## Sumário

- [Arquitetura](#arquitetura)
- [Pré-requisitos](#pré-requisitos)
- [Estrutura de diretórios](#estrutura-de-diretórios)
- [Configuração do ambiente](#configuração-do-ambiente)
- [Variáveis do Airflow](#variáveis-do-airflow)
- [Subindo os containers](#subindo-os-containers)
- [Datasets de origem](#datasets-de-origem)
- [Executando a DAG](#executando-a-dag)
- [Camadas do datalake](#camadas-do-datalake)
- [Serviços auxiliares](#serviços-auxiliares)
- [Solução de problemas](#solução-de-problemas)

---

## Arquitetura

```
CSV (Kaggle)
    │
    ▼
┌─────────┐      ┌─────────┐      ┌──────┐
│  Bronze │ ───▶ │  Silver │ ───▶ │ Gold │ ───▶ PostgreSQL (gold)
└─────────┘      └─────────┘      └──────┘
  Ingestão         Limpeza e      Agregações      Consumo via
  raw (Delta)      tipagem        analíticas      Dremio / BI
```

Todas as camadas são armazenadas em formato **Delta Lake** no diretório `datalake/`. O Airflow orquestra a execução sequencial por arquivo: Bronze → Silver → Gold.

---

## Pré-requisitos

| Ferramenta | Versão mínima | Observação |
|---|---|---|
| Docker Desktop | 4.x | WSL2 habilitado no Windows |
| Docker Compose | 2.x | Incluído no Docker Desktop |
| RAM disponível para Docker | 8 GB | Mínimo recomendado pelo Airflow |
| Disco livre | 15 GB | Para imagens + datalake |

---

## Estrutura de diretórios

```
.
├── dags/
│   ├── pipeline_exemplo_kaggle.py          # Definição da DAG
│   └── helpers/
│       └── pipeline_exemplo_kaggle/
│           ├── objects_bronze.py           # Schemas PySpark (Bronze)
│           ├── objects_silver.py           # UDFs de padronização (Silver)
│           ├── pipeline_bronze.py          # Ingestão CSV → Delta
│           ├── pipeline_silver.py          # Limpeza e tipagem
│           └── pipeline_gold.py            # Agregações analíticas
├── utils/
│   ├── datalake_manipulation.py            # Funções write Delta / PostgreSQL
│   └── spark_config.py                     # Fábrica de SparkSession
├── datasets/
│   └── Kaggle/                             # CSVs de origem (copiar aqui)
├── datalake/
│   ├── bronze/Kaggle/                      # Dados brutos particionados por data
│   ├── silver/Kaggle/                      # Dados limpos e tipados
│   └── gold/                               # Agregações finais
├── logs/                                   # Logs do Airflow (gerado automaticamente)
├── plugins/                                # Plugins do Airflow
├── config/                                 # Configurações do Airflow
├── Dockerfile                              # Imagem customizada do Airflow
└── docker-compose.yaml                     # Orquestração de todos os serviços
```

---

## Configuração do ambiente

### 1. Clonar o repositório

```bash
git clone <url-do-repositorio>
cd <nome-do-projeto>
```

### 2. Criar o arquivo `.env`

Crie um arquivo `.env` na raiz do projeto com o conteúdo abaixo. No Linux, o `AIRFLOW_UID` deve ser o UID do seu usuário (`id -u`). No Windows/Mac, use `50000`.

```dotenv
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=.
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
```

### 3. Criar os diretórios necessários

```bash
mkdir -p dags logs plugins config datasets/Kaggle datalake
```

### 4. Ajustar permissões (Linux apenas)

```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
sudo chown -R $(id -u):0 logs dags plugins config datalake datasets
```

---

## Variáveis do Airflow

Após subir os containers, cadastre as variáveis abaixo na interface do Airflow em **Admin → Variables**, ou via CLI:

```bash
docker exec -it <airflow-worker-container> airflow variables set url_postgres_gold postgresql
docker exec -it <airflow-worker-container> airflow variables set user_postgres_gold usuario_postgres
docker exec -it <airflow-worker-container> airflow variables set password_postgres_gold postgres_gold123
docker exec -it <airflow-worker-container> airflow variables set host_postgres_gold postgres_gold
```

| Variável | Valor |
|---|---|
| `url_postgres_gold` | `postgresql` |
| `user_postgres_gold` | `usuario_postgres` |
| `password_postgres_gold` | `postgres_gold123` |
| `host_postgres_gold` | `postgres_gold` |

> **Atenção:** O host deve ser o nome do serviço Docker (`postgres_gold`), não `localhost`, para que os workers do Airflow consigam se comunicar com o banco dentro da rede Docker.

---

## Subindo os containers

### Primeira execução — build da imagem customizada

```bash
docker compose up --build -d
```

### Execuções subsequentes

```bash
docker compose up -d
```

### Verificar os serviços

```bash
docker compose ps
```

Aguarde todos os serviços atingirem o estado `healthy` antes de prosseguir. O Airflow pode levar de 2 a 5 minutos na primeira inicialização.

### Parar os containers

```bash
docker compose down
```

Para remover também os volumes (apaga o banco de dados):

```bash
docker compose down -v
```

---

## Datasets de origem

Faça o download do dataset da Olist no Kaggle e copie os arquivos CSV para `datasets/Kaggle/`:

[https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

Os arquivos esperados são:

```
datasets/Kaggle/
├── olist_customers_dataset.csv
├── olist_orders_dataset.csv
├── olist_sellers_dataset.csv
├── olist_geolocation_dataset.csv
├── olist_order_items_dataset.csv
├── olist_order_payments_dataset.csv
├── olist_order_reviews_dataset.csv
├── olist_products_dataset.csv
└── product_category_name_translation.csv
```

---

## Executando a DAG

1. Acesse a interface do Airflow em [http://localhost:8080](http://localhost:8080) com as credenciais `airflow / airflow`.
2. Localize a DAG `pipeline_exemplo_kaggle`.
3. Ative a DAG pelo toggle e dispare a execução manual clicando em **Trigger DAG**.

### Fluxo de execução

A DAG processa cada arquivo de forma sequencial na ordem abaixo, garantindo que a Gold só comece após todos os arquivos passarem pelas camadas Bronze e Silver:

```
bronze_olist_customers_dataset.csv
    └── silver_olist_customers_dataset.csv
            └── dummy_olist_customers_dataset
                    └── bronze_olist_orders_dataset.csv
                            └── silver_olist_orders_dataset.csv
                                    └── ...
                                            └── gold_kaggle
```

---

## Camadas do datalake

### Bronze — Ingestão

Lê os CSVs com schema explícito e grava em Delta Lake particionado por `year/month/day`. Modo de escrita: `append`.

Caminho de destino:
```
datalake/bronze/Kaggle/<nome_arquivo>/year=YYYY/month=MM/day=DD/
```

### Silver — Limpeza e tipagem

Lê a partição do dia corrente da Bronze, aplica as transformações abaixo e grava em Silver com `merge` (upsert por chave primária).

| Transformação | Tabelas afetadas |
|---|---|
| Conversão de timestamps (`yyyy-MM-dd HH:mm:ss`) | orders, order_items, order_payments, order_reviews |
| Preenchimento de nulos com valores padrão | customers, sellers, geolocation, products, reviews |
| Remoção de duplicatas por chave primária | todas |
| Normalização para minúsculas | order_status, product_category_name |
| Campo `dt_update` com timestamp da carga | todas (exceto customers) |

Caminho de destino:
```
datalake/silver/Kaggle/<nome_arquivo>/
```

### Gold — Agregações analíticas

Lê as tabelas Silver, monta um DataFrame base enriquecido e calcula as seguintes métricas, gravando em Delta Lake e opcionalmente no PostgreSQL:

| Tabela Gold | Descrição | Granularidade |
|---|---|---|
| `monthly_sales` | Receita total, pedidos e ticket médio por mês | Pedido |
| `top_product_categories` | Top 10 categorias por receita | Item |
| `sales_by_customer_state` | Receita e pedidos por estado do cliente | Pedido |
| `seller_performance` | Receita, itens vendidos e ticket médio por vendedor | Item |
| `delivery_metrics` | Prazo médio de entrega real vs estimado por estado | Pedido |

Caminhos de destino:
```
datalake/gold/sales/monthly_sales/
datalake/gold/product/top_product_categories/
datalake/gold/sales/sales_by_customer_state/
datalake/gold/sellers/seller_performance/
datalake/gold/logistics/delivery_metrics/
```

---

## Serviços auxiliares

### Airflow — Orquestração

| URL | Credenciais |
|---|---|
| http://localhost:8080 | `airflow / airflow` |

### PostgreSQL Gold — Armazenamento das métricas

Banco de dados dedicado para consumo das tabelas Gold por ferramentas de BI.

| Parâmetro | Valor |
|---|---|
| Host (externo ao Docker) | `localhost` |
| Porta | `5433` |
| Banco | `gold` |
| Usuário | `usuario_postgres` |
| Senha | `postgres_gold123` |

String de conexão (SQLAlchemy):
```
postgresql+psycopg2://usuario_postgres:postgres_gold123@localhost:5433/gold
```

### Dremio — Query Engine

Permite executar queries SQL diretamente nos arquivos Delta Lake sem movimentação de dados.

| URL | Observação |
|---|---|
| http://localhost:9047 | Acesso à UI web |
| `localhost:31010` | Conexão JDBC/ODBC |

Para conectar o Dremio ao datalake, adicione uma fonte do tipo **File System (NAS)** apontando para `/opt/dremio/data-lake`, que está mapeado para o diretório `datalake/` do projeto.

### Flower — Monitor do Celery (opcional)

```bash
docker compose --profile flower up -d
```

Disponível em: http://localhost:5555

---

## Solução de problemas

**Containers não sobem / ficam em `unhealthy`**
Verifique se o Docker tem pelo menos 4 GB de RAM alocados em Docker Desktop → Settings → Resources.

**`AIRFLOW_UID not set` no log do airflow-init**
Crie o arquivo `.env` na raiz com `AIRFLOW_UID=50000` (ou o resultado de `id -u` no Linux).

**Erro `write_to_postgresql() got an unexpected keyword argument 'db_name'`**
A assinatura da função `write_to_postgresql` em `datalake_manipulation.py` precisa incluir o parâmetro `db_name`. Verifique se o arquivo está atualizado conforme a versão mais recente.

**Workers do Airflow não conseguem conectar ao `postgres_gold`**
O host nas variáveis do Airflow deve ser `postgres_gold` (nome do serviço Docker), nunca `localhost`. Dentro da rede Docker, `localhost` aponta para o próprio container worker.

**Spark não encontra a tabela Delta na Bronze**
Confirme que a DAG foi executada no mesmo dia em que os CSVs foram colocados em `datasets/Kaggle/`. O caminho da Bronze é particionado por `year/month/day` com a data de execução.

**Dremio não exibe os arquivos Delta**
Certifique-se de que o volume `./datalake:/opt/dremio/data-lake` está configurado no `docker-compose.yaml` e que o container Dremio foi reiniciado após a primeira carga de dados.
