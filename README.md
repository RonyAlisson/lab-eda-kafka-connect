# Lab EDA: Kafka Connect

Pipeline de dados orientado a eventos (EDA), **sem código de aplicação**: toda alteração em uma tabela do PostgreSQL é capturada por CDC, publicada no Kafka e gravada como arquivo no Data Lake (MinIO).

> Projeto de estudo da Pós-Graduação em Engenharia de Dados, baseado no lab **21.Ingestao-Dados-Kafka-Connect** do material do curso. As configurações seguem o material e incluem práticas de produção onde fazem sentido.

![Arquitetura](docs/arquitetura.png)

```
PostgreSQL ──(Source: Debezium)──► Kafka ──(Sink: S3)──► MinIO (bucket bronze)
                                     ▲                          │
                                     └──── evento "arquivo criado" ┘  (Parte 7)
```

**Fluxo detalhado** (WAL, offsets, arquivos gerados, estrutura de um evento): [docs/fluxo-completo.svg](docs/fluxo-completo.svg)

---

## Status

| Parte | Componente | Status |
|---|---|---|
| — | PostgreSQL (WAL `logical`) + pgAdmin + dados de exemplo | ✅ Concluído |
| — | MinIO + criação dos buckets | ✅ Concluído |
| 1 | Zookeeper | ✅ Concluído |
| 2 | Kafka broker | ✅ Concluído |
| 3 | Interface web do Kafka (AKHQ) | ✅ Concluído |
| 4 | Kafka Connect | ✅ Concluído |
| 5 | Conector Source (PostgreSQL → Kafka) | ✅ Concluído |
| 6 | Conector Sink (Kafka → MinIO) | ✅ Concluído |
| 7 | Notificação MinIO → Kafka | ⏳ Pendente |

---

## Stack

| Serviço | Imagem | Função |
|---|---|---|
| `postgres` | `postgres:16` | Banco de origem, com WAL em `logical` para o CDC |
| `pg-admin` | `dpage/pgadmin4:9.0` | Interface web do PostgreSQL |
| `minio` | `quay.io/minio/minio:RELEASE.2025-02-28T09-55-16Z` | Data Lake compatível com S3 |
| `mc` | (mesma imagem do `minio`) | Cria os buckets `bronze`, `silver` e `gold`, depois termina |
| `zookeeper` | `confluentinc/cp-zookeeper:7.6.1` | Coordenação do cluster Kafka |
| `kafka-broker` | `confluentinc/cp-kafka:7.6.1` | Broker Kafka |
| `connect` | `fernandos/kafka-connet-debezium-lab:v215` | Kafka Connect com os plugins Debezium PostgreSQL 2.6.1 e Confluent S3 Sink 10.5.5 |
| `akhq` | `tchiotludo/akhq:0.20.0` | Interface web do Kafka e do Kafka Connect |

---

## Como executar

### Pré-requisitos
- Docker Desktop (com WSL 2 no Windows)
- Docker Compose v2

### 1. Configurar as credenciais

```sh
cp .env.example .env
```

Preencha o `.env`. Ele guarda **apenas usuários e senhas**; as versões das imagens ficam fixas no `docker-compose.yml`.

| Variável | Uso |
|---|---|
| `PG_USER` / `PG_PASSWORD` | Superusuário do PostgreSQL (também usado pelo conector Source) |
| `PG_DB` | Banco criado na primeira inicialização |
| `PGADMIN_EMAIL` / `PGADMIN_PASSWORD` | Login do pgAdmin (o email precisa ter formato válido) |
| `MINIO_USER` / `MINIO_PASSWORD` | Administrador do MinIO (senha com no mínimo 8 caracteres) |
| `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | Credencial própria do conector Sink (criada no console do MinIO, ver passo 3) |
| `KAFKA_UI_USER` / `KAFKA_UI_PASSWORD` | Login do Kafka UI (alternativa ao AKHQ, ver `docker-compose.example.yml`) |

> O `.env` está no `.gitignore` e não vai para o Git. O `.env.example` contém só as chaves, com valores de exemplo.

### 2. Subir o ambiente

```sh
docker compose up -d
docker compose ps -a
```

Os serviços sobem na ordem certa graças ao `depends_on` com `condition: service_healthy`: `zookeeper` → `kafka-broker` → `connect` → `akhq`. O processo leva de 1 a 2 minutos.

Na primeira inicialização, o PostgreSQL executa `postgres/init/01-inventory.sql`, que cria o schema `inventory` com as tabelas e os dados de exemplo do material.

### 3. Criar a credencial do conector Sink no MinIO

No console (http://localhost:9001), vá em **Access Keys → Create access key** e coloque os valores em `MINIO_ACCESS_KEY` e `MINIO_SECRET_KEY` no `.env`. Depois, recrie o Connect para ele receber as variáveis:

```sh
docker compose up -d connect
```

### 4. Criar os conectores

Os conectores são criados enviando os JSONs da pasta `conectores/` para a API REST do Connect. O `PUT .../config` cria ou atualiza o conector.

```sh
# Source: PostgreSQL → Kafka
docker compose exec connect curl -s -X PUT -H "Content-Type: application/json" -d "@/conectores/conector-postgres.json" http://localhost:8083/connectors/connector-postgres/config

# Sink: Kafka → MinIO
docker compose exec connect curl -s -X PUT -H "Content-Type: application/json" -d "@/conectores/conector-minio.json" http://localhost:8083/connectors/connector-minio/config
```

### 5. Parar o ambiente

```sh
docker compose down      # remove os containers e MANTÉM os dados (volumes)
docker compose down -v   # remove os containers e APAGA os dados
```

---

## Conectores

| Conector | Arquivo | Papel | Configurações principais |
|---|---|---|---|
| `connector-postgres` | `conectores/conector-postgres.json` | **Producer**: lê o WAL do PostgreSQL e publica no Kafka | `plugin.name: pgoutput`, `topic.prefix: postgres`, `schema.include.list: inventory`, `tasks.max: 1` |
| `connector-minio` | `conectores/conector-minio.json` | **Consumer** (group `connect-connector-minio`): lê o tópico e grava no bucket `bronze` | `topics: postgres.inventory.products`, `flush.size: 3`, `behavior.on.null.values: ignore` |

**Credenciais fora dos JSONs:** os conectores usam `${env:PG_USER}`, `${env:MINIO_ACCESS_KEY}` etc. O Kafka Connect resolve esses valores pelo **Config Provider** (`EnvVarConfigProvider`), a partir das variáveis de ambiente do container `connect`. As senhas não ficam gravadas nos arquivos nem no tópico `connect-configs`.

```
.env  →  environment do serviço connect  →  ${env:...} no JSON do conector
```

**Resultado do pipeline:**
- Tópicos `postgres.inventory.customers`, `orders`, `products` e `products_on_hand`, no formato `<prefixo>.<schema>.<tabela>`
- Primeiro, o **snapshot** das linhas existentes (`op: "r"`); depois, cada alteração em tempo real (`op: "c"`, `"u"`, `"d"`)
- Arquivos no MinIO em `bronze/topics/postgres.inventory.products/partition=0/`, com 3 eventos cada. O nome do arquivo traz o offset da primeira mensagem (`...+0+0000000013.json`)

---

## Interfaces e portas

| Serviço | Endereço na sua máquina | Endereço dentro da rede `dados` |
|---|---|---|
| pgAdmin | http://localhost:5433 | — |
| MinIO Console | http://localhost:9001 | — |
| MinIO API (S3) | `localhost:9000` | `minio:9000` |
| AKHQ | http://localhost:8080/ui | — |
| Kafka Connect (API REST) | http://localhost:8083 | `connect:8083` |
| PostgreSQL | `localhost:5432` | `postgres:5432` |
| Kafka | `localhost:9092` | `kafka-broker:29092` |
| Zookeeper | `localhost:2181` (só para estudo) | `zookeeper:2181` |

> **Regra dos endereços:** clientes **dentro** do Docker (Connect, AKHQ, MinIO) usam o **nome do serviço** e a porta interna, como `kafka-broker:29092`. Clientes **fora** (scripts na sua máquina) usam `localhost:9092`.

O pgAdmin já sobe com o servidor PostgreSQL cadastrado (`pgadmin/servers.json`); a senha é pedida no primeiro acesso.

---

## Testes

O roteiro completo, com a explicação de cada comando, está em **[docs/manual.md](docs/manual.md)**. Os principais:

**PostgreSQL: WAL pronto para o CDC**
```sh
docker compose exec postgres psql -U <PG_USER> -d <PG_DB> -c "SHOW wal_level;"
# esperado: logical
```

**Zookeeper e Kafka**
```sh
docker compose exec zookeeper bash -c "echo ruok | nc localhost 2181"
# esperado: imok
docker compose exec zookeeper zookeeper-shell localhost:2181 ls /brokers/ids
# esperado: [1]  (o broker se registrou)
docker compose exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --list
```

**Kafka Connect e conectores**
```sh
docker compose exec connect curl -s http://localhost:8083/connector-plugins
docker compose exec connect curl -s http://localhost:8083/connectors/connector-postgres/status
docker compose exec connect curl -s http://localhost:8083/connectors/connector-minio/status
# esperado: conector e task em RUNNING
```

**CDC em tempo real**: deixe um consumer aberto e altere a tabela pelo pgAdmin:
```sh
docker compose exec kafka-broker kafka-console-consumer --bootstrap-server localhost:9092 --topic postgres.inventory.products --from-beginning --property print.key=true
```
```sql
INSERT INTO inventory.products (name, description, weight) VALUES ('Lapis', 'O melhor', 1);
UPDATE inventory.products SET weight = 2 WHERE name = 'Lapis';
DELETE FROM inventory.products WHERE name = 'Lapis';
```

**O Sink como consumer group** (offset e LAG)
```sh
docker compose exec kafka-broker kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group connect-connector-minio
```

**Investigar um container que não sobe**
```sh
docker compose ps -a              # ele parou? (Exited)
docker compose logs <serviço>     # por que parou?
```

---

## Estrutura do projeto

```
.
├── docker-compose.yml           # Ambiente do lab (1 broker)
├── docker-compose.example.yml   # Estudo: cluster com 3 brokers + AKHQ e Kafka UI
├── .env.example                 # Modelo das credenciais
├── .gitignore                   # Mantém o .env fora do Git
├── DockerFile                   # Reservado: imagem própria do Kafka Connect
├── conectores/
│   ├── conector-postgres.json   # Source: PostgreSQL → Kafka (Debezium)
│   └── conector-minio.json      # Sink: Kafka → MinIO (S3)
├── postgres/
│   └── init/
│       └── 01-inventory.sql     # Schema inventory e dados de exemplo (1ª inicialização)
├── pgadmin/
│   └── servers.json             # Servidor PostgreSQL pré-cadastrado no pgAdmin
└── docs/
    ├── arquitetura.png          # Arquitetura do material
    ├── fluxo-completo.svg       # Fluxo detalhado do pipeline
    └── manual.md                # Comandos de operação e teste
```

---

## Diferenças em relação ao material do curso

| Material | Este projeto | Motivo / impacto |
|---|---|---|
| `debezium/example-postgres` | `postgres:16` com `wal_level=logical` no `command` | Configuração do WAL explícita |
| Schema `inventory` embutido na imagem | `postgres/init/01-inventory.sql`, com as mesmas tabelas e dados | Ambiente reprodutível. Removidos a extensão PostGIS e a tabela `geom`, que o `postgres:16` não suporta. Os tópicos têm os mesmos nomes do material. |
| Plugin de decodificação já instalado na imagem | PostgreSQL "puro" | O conector Debezium usa `"plugin.name": "pgoutput"`, nativo do PostgreSQL |
| Usuário e banco `postgres`, senhas nos JSONs | Variáveis do `.env` + Config Provider | Senhas fora do código, dos JSONs e do tópico `connect-configs` |
| Conector Sink com a chave `cursolab` | Access Key própria, via `${env:MINIO_ACCESS_KEY}` | Credencial dedicada, não a do administrador |
| Versões no `.env` | Versões fixas no compose | O `.env` guarda só credenciais |
| `ZOOKEEPER_4LW_COMMANDS_WHITELIST: "*"` | `KAFKA_OPTS: -Dzookeeper.4lw.commands.whitelist=ruok` | A imagem `cp-zookeeper:7.6.1` **ignora** a variável do material. A propriedade Java funciona e libera só o necessário. |
| Connect com tracing (OpenTelemetry) | Sem tracing | O `otel-collector` é do módulo de Observabilidade e não existe neste ambiente |
| `container_name` fixo | Nomes gerados pelo Compose | Usar `docker compose exec <serviço>` em vez de `docker exec <container>` |

---

## Práticas de produção aplicadas

- **Versões fixas** das imagens (nunca `latest`)
- **Healthchecks** no PostgreSQL, Zookeeper, Kafka e Kafka Connect, e `depends_on` com `service_healthy`, para que nenhum serviço suba antes da dependência estar pronta
- **`restart: unless-stopped`** no Zookeeper, Kafka, Connect e AKHQ
- **Credenciais fora do código:** `.env` (fora do Git) + Config Provider do Kafka Connect
- **Credencial dedicada** para o conector Sink no MinIO (menor privilégio)
- **Ambiente reprodutível:** schema e dados recriados automaticamente pelo script de inicialização
- **Volumes nomeados** para os dados (Kafka, Zookeeper, PostgreSQL, MinIO, pgAdmin)
- **Volumes separados** para os snapshots e o log de transações do Zookeeper
- **Whitelist mínima** de comandos 4LW no Zookeeper (só `ruok`)
- **`ulimits`** de arquivos abertos no Zookeeper e Kafka (efeito real em servidores Linux)
- **Rede dedicada** (`dados`) e porta interna do Kafka (`29092`) não exposta

---

## Pontos a melhorar

- **Segurança do Kafka:** trocar `PLAINTEXT` por `SASL_SSL` (criptografia + autenticação)
- **Criação de tópicos:** `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"` e tópicos criados de forma explícita (hoje nascem com o padrão de 1 partição)
- **Retenção:** definir por pipeline no conector (`topic.creation.default.retention.ms`)
- **API do Connect:** não expor a porta `8083` ou protegê-la com autenticação
- **Zookeeper:** não expor a porta `2181`, desligar o AdminServer (`ZOOKEEPER_ADMIN_ENABLE_SERVER: "false"`) e usar 3 nós (quórum)
- **Alta disponibilidade:** 3 brokers com replicação 3 (ver `docker-compose.example.yml`)
- **Usuário dedicado para o Debezium**, com `REPLICATION` e `SELECT`, em vez do superusuário
- **Imagem própria do Connect** (`DockerFile`), em vez da imagem pronta do material
- **`restart: unless-stopped`** também no PostgreSQL, pgAdmin e MinIO
- **Formato dos arquivos:** Parquet ou Avro no lugar de JSON, e `rotate.interval.ms` para gravar por tempo mesmo com poucas mensagens
- **Modo KRaft**, que elimina o Zookeeper (não adotado neste lab, para seguir o material)
- **Interface AKHQ:** a versão 0.20.0 é antiga. Existem versões mais novas e a alternativa Kafka UI (kafbat).

---

## Avisos

- **Imagens do MinIO:** o download das imagens do MinIO (`quay.io/minio/*` e `minio/*`) está sendo negado pelos registries. O ambiente funciona porque a imagem do servidor já está baixada localmente, e o serviço `mc` reaproveita essa imagem, que traz o cliente `mc` embutido. **Não remova essa imagem** (`docker image rm`, `docker system prune -a`). Para recriar o ambiente em outra máquina, será preciso uma alternativa compatível com S3.
- **Configurações de primeira inicialização** (usuário, banco e script `01-inventory.sql` do PostgreSQL, servidor do pgAdmin) só são aplicadas com o volume vazio. Para recriar só o banco, sem apagar o Kafka, veja "Recriar o banco do zero" no [manual](docs/manual.md).
- **Variáveis novas no `.env`** só chegam aos containers depois de recriá-los (`docker compose up -d <serviço>`).

---

## Referências

- Material do curso: `Engenharia-Dados/21.Ingestao-Dados-Kafka` e `21.Ingestao-Dados-Kafka-Connect`
- [Apache ZooKeeper: Administrator's Guide](https://zookeeper.apache.org/doc/current/zookeeperAdmin.html)
- [Apache Kafka: Documentation](https://kafka.apache.org/documentation/)
- [Kafka Connect: Config Providers (KIP-887, EnvVarConfigProvider)](https://cwiki.apache.org/confluence/display/KAFKA/KIP-887%3A+Add+ConfigProvider+to+make+use+of+environment+variables)
- [Confluent: Docker Configuration Reference](https://docs.confluent.io/platform/current/installation/docker/config-reference.html)
- [Confluent: Amazon S3 Sink Connector](https://docs.confluent.io/kafka-connectors/s3-sink/current/overview.html)
- [Debezium: PostgreSQL connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- [Docker Compose: file reference](https://docs.docker.com/reference/compose-file/services/)
