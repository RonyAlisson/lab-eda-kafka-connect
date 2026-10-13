# Manual de comandos e testes

Comandos para operar e testar o ambiente. Todos rodam na raiz do projeto, no PowerShell.

> **`docker compose exec <serviço> <comando>`** executa um comando **dentro** do container daquele serviço. Use sempre o nome do **serviço** (`postgres`, `kafka-broker`...), não o nome do container.

---

## 1. Ambiente

| Comando | O que faz |
|---|---|
| `docker compose up -d` | Sobe todos os serviços em segundo plano (`-d`), na ordem definida pelo `depends_on`. |
| `docker compose up -d <serviço>` | Sobe um serviço e suas dependências. |
| `docker compose ps -a` | Lista os containers, **inclusive os parados** (`-a`). Mostra se estão `healthy`. |
| `docker compose logs <serviço>` | Mostra o log do serviço. Use quando um container para ou fica `unhealthy`. |
| `docker compose logs -f <serviço>` | Acompanha o log em tempo real (saia com `Ctrl+C`). |
| `docker compose config` | Valida o `docker-compose.yml` e mostra a versão final, com as variáveis do `.env` aplicadas. |
| `docker compose stop` | Para os containers sem removê-los. |
| `docker compose down` | Remove os containers. **Os dados ficam** nos volumes. |
| `docker compose down -v` | Remove os containers **e apaga os volumes** (tabelas, tópicos, arquivos). |

**Investigando um container que não sobe:**
```powershell
docker compose ps -a            # ele parou? (Exited)
docker compose logs <serviço>   # por que parou?
```

---

## 2. PostgreSQL

**WAL pronto para o CDC**
```powershell
docker compose exec postgres psql -U lab_eda -d lab_eda_kafka -c "SHOW wal_level;"
```
Esperado: `logical`. Sem isso, o Debezium não consegue ler as alterações.

**Consultar os dados**
```powershell
docker compose exec postgres psql -U lab_eda -d lab_eda_kafka -c "SELECT * FROM inventory.products;"
```
`psql` é o cliente de linha de comando do PostgreSQL: `-U` é o usuário, `-d` o banco e `-c` o comando SQL.

**Banco pronto para conexões (o mesmo teste do healthcheck)**
```powershell
docker compose exec postgres pg_isready -U lab_eda
```
Esperado: `accepting connections`.

**Conferir se o script de inicialização rodou**
```powershell
docker compose logs postgres | findstr inventory
```
Esperado: `running /docker-entrypoint-initdb.d/01-inventory.sql`.

**Recriar o banco do zero** (para o script `postgres/init/` rodar de novo), sem apagar o Kafka:
```powershell
docker compose stop postgres pg-admin connect
docker compose rm -f postgres
docker volume rm lab-eda-kafka-connect_pg_data
docker compose up -d
```

---

## 3. Zookeeper

**Healthcheck manual**
```powershell
docker compose exec zookeeper bash -c "echo ruok | nc localhost 2181"
```
Pergunta "are you ok?" (`ruok`). Esperado: `imok`. Só funciona porque o `ruok` está liberado na whitelist (`KAFKA_OPTS`).

**Ver a árvore de dados**
```powershell
docker compose exec zookeeper zookeeper-shell localhost:2181 ls /
```
Lista os nós guardados pelo Zookeeper. Com o Kafka no ar, aparecem `brokers`, `config` etc.

**Brokers registrados**
```powershell
docker compose exec zookeeper zookeeper-shell localhost:2181 ls /brokers/ids
```
Esperado: `[1]`, o `KAFKA_BROKER_ID`. Confirma que o Kafka se registrou no Zookeeper.

---

## 4. Kafka

> Dentro do container do Kafka, o endereço é `localhost:9092`.

**Listar tópicos**
```powershell
docker compose exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --list
```

**Criar um tópico**
```powershell
docker compose exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --create --topic teste --partitions 3 --replication-factor 1
```
`--partitions` divide o tópico para permitir paralelismo; `--replication-factor` define quantas cópias existem (no máximo, o número de brokers).

**Detalhes de um tópico** (partições, líder, réplicas)
```powershell
docker compose exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --describe --topic teste
```

**Producer: enviar mensagens**
```powershell
docker compose exec kafka-broker kafka-console-producer --bootstrap-server localhost:9092 --topic teste
```
Cada linha digitada vira uma mensagem. Saia com `Ctrl+C`.

**Consumer: ler mensagens**
```powershell
docker compose exec kafka-broker kafka-console-consumer --bootstrap-server localhost:9092 --topic teste --from-beginning
```
`--from-beginning` lê desde a primeira mensagem guardada. Sem ele, lê só as novas. Para ver também a **chave** da mensagem, acrescente `--property print.key=true`.

**Consumer groups** (quem está lendo e o quanto está atrasado)
```powershell
docker compose exec kafka-broker kafka-consumer-groups --bootstrap-server localhost:9092 --list
docker compose exec kafka-broker kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group <grupo>
```
A coluna `LAG` mostra quantas mensagens o grupo ainda não leu.

**Configurações do broker** (por exemplo, a retenção)
```powershell
docker compose exec kafka-broker kafka-configs --bootstrap-server localhost:9092 --entity-type brokers --entity-name 1 --describe --all | findstr retention
```

---

## 5. Kafka Connect

> A API REST do Connect fica na porta `8083`. Também dá para abrir os endereços `GET` no navegador, por exemplo: http://localhost:8083/connector-plugins

**Plugins instalados**
```powershell
docker compose exec connect curl -s http://localhost:8083/connector-plugins
```
Precisam aparecer `io.debezium.connector.postgresql.PostgresConnector` (Source) e `io.confluent.connect.s3.S3SinkConnector` (Sink). Sem o plugin, o conector não pode ser criado.

**Conectores criados**
```powershell
docker compose exec connect curl -s http://localhost:8083/connectors
```

**Status de um conector**
```powershell
docker compose exec connect curl -s http://localhost:8083/connectors/<nome>/status
```
O conector e as tasks precisam estar `RUNNING`. Se aparecer `FAILED`, o campo `trace` mostra o erro.

**Tópicos internos do Connect**: na lista de tópicos do Kafka, devem aparecer:

| Tópico | Guarda |
|---|---|
| `connect-configs` | A configuração dos conectores |
| `connect-offsets` | Até onde cada Source já leu (no Debezium, a posição no WAL) |
| `connect-status` | O status de conectores e tasks |

---

## 6. MinIO

**Conferir a criação dos buckets**
```powershell
docker compose logs mc
```
Esperado: `Bucket created successfully` para `bronze`, `silver` e `gold`. O container `mc` fica `Exited (0)`, e isso é o normal: ele roda uma vez e termina.

---

## 7. Interfaces web

| Interface | Endereço | Login |
|---|---|---|
| pgAdmin | http://localhost:5433 | `PGADMIN_EMAIL` / `PGADMIN_PASSWORD` do `.env` |
| MinIO Console | http://localhost:9001 | `MINIO_USER` / `MINIO_PASSWORD` do `.env` |
| AKHQ | http://localhost:8080/ui | sem login |
| API do Connect | http://localhost:8083 | sem login |
