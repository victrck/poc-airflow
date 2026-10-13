# Plano de Trabalho: Orquestração de Dados com Apache Airflow e Arquitetura Lakehouse Híbrida

## 1. Visão Geral do Projeto e Pré-requisitos

* **Carga Horária Estimada:** 64 horas
* **Objetivo:** Desenvolver um ambiente local de nível de produção com Airflow utilizando uma arquitetura híbrida. O projeto processará o dataset **EmpresasBR** para viabilizar um comparativo com a ferramenta Mage.ai, além de demonstrar o isolamento de projetos via Role-Based Access Control (RBAC).
* **Hardware:** O ambiente foi validado para execução em máquina com **32GB de RAM**, garantindo performance para múltiplos serviços robustos e computação efêmera.

## 2. Stack Tecnológica (Local via Docker Compose)

* **Orquestrador:** Apache Airflow (LocalExecutor com mapeamento do `docker.sock` para *Docker-out-of-Docker*).
* **Segurança:** RBAC nativo do Airflow (DAG-level Access Control).
* **Catálogo e Metadados:** PostgreSQL (Compartilhado entre o Airflow e o catálogo JDBC do Iceberg).
* **Armazenamento:** MinIO (Data Lake local com camadas Raw e Silver).
* **Engine Computacional:** Trino (Transformação via SQL distribuído) com Apache Iceberg.
* **Computação Efêmera (Ingestão):** DockerOperator.
* **Observabilidade:** Airflow -> StatsD Exporter -> Prometheus -> Grafana.

## 3. Cronograma Definitivo (64 Horas)

### Fase 1: Setup da Infraestrutura Lakehouse Local (10 horas)
* Escrever o `docker-compose.yml` consolidando: Airflow, PostgreSQL, MinIO, Trino, StatsD-Exporter, Prometheus e Grafana.
* Mapear o volume `/var/run/docker.sock:/var/run/docker.sock` no *worker* do Airflow.
* Provisionamento automático do bucket `lakehouse` no MinIO e configuração do arquivo `iceberg.properties` para o Trino.

### Fase 2: Core Airflow e Segurança (Multi-tenancy) (10 horas)
* Estruturar o repositório base (`dags/`, `plugins/`, `tests/`, `docker/`).
* **Configuração de RBAC:** Habilitar *DAG-level Access Control* no `airflow.cfg`.
* Criar *Roles* e usuários isolados (ex: Usuário A com acesso apenas ao EmpresasBR, Usuário B com acesso a fluxos financeiros).
* Configurar *Connections* do Airflow para o MinIO (S3 Hook) e Trino (Trino Hook).

### Fase 3: Ingestão Híbrida - Contexto EmpresasBR (12 horas)
* Modelar as DAGs utilizando a *TaskFlow API* e *TaskGroups*.
* **Ingestão Desacoplada (ETL):** Usar o `DockerOperator` para disparar um container efêmero contendo o script de extração. Este container fará o download da base pública do **EmpresasBR** (CNPJ), salvará o arquivo na camada `raw/` do MinIO e será destruído.

### Fase 4: Processamento Lakehouse (Trino + Iceberg) (12 horas)
* **Transformação Distribuída (ELT):** Utilizar o `SQLExecuteQueryOperator` no Airflow.
* O Airflow enviará comandos SQL ao Trino para ler os dados das empresas da camada *Raw*, aplicar limpezas, agregar métricas (ex: CNPJs ativos por região) e salvar em tabelas particionadas no formato Apache Iceberg na camada `silver/`.
* Mapear mentalmente as diferenças e vantagens dessa abordagem em comparação aos blocos Python do Mage.ai.

### Fase 5: Observabilidade, Qualidade e Testes (10 horas)
* **Monitoramento:** Configurar o envio de métricas UDP (StatsD) e construir dashboards no Grafana focados na saúde do orquestrador (ex: taxa de falha das DAGs, duração da ingestão do EmpresasBR).
* **Testes (CI/CD):** Aplicar linting (`black`, `flake8`) e desenvolver testes unitários com `pytest` para verificar erros de importação e quebras de DAGs.

### Fase 6: Produção das Vídeo Aulas (10 horas)

**Vídeo 1: Fundamentos e Arquitetura (3 horas)**
* *Teoria Base:* O que é DAG, Task, XCom, Sensor e Operator.
* *Arquitetura:* Scheduler vs. Webserver vs. Worker.
* *Boas Práticas (Anti-patterns):* Evitar código solto (top-level code), não processar dados volumosos (Pandas) na memória do worker e não usar XComs para transferir gigabytes de dados.

**Vídeo 2: Prática EmpresasBR, RBAC e Comparativo Mage (7 horas)**
* *Screencast Dinâmico:* Demonstração prática do código e infraestrutura.
* *RBAC na Prática:* Login com usuários distintos para demonstrar que o Airflow atende a requisitos de governança de projetos isolados.
* *Execução Híbrida:* Rodar o container efêmero de ingestão e, em seguida, a consulta de transformação no Trino.
* *Análise Crítica:* Breve debate sobre as vantagens observadas no Airflow vs. desenvolvimento prévio no Mage.ai.

## 4. Estrutura de Diretórios Final

```text
projeto-airflow-empresasbr/
├── dags/                     
│   ├── empresasbr_pipeline.py
│   └── financeiro_dummy.py   # Para teste do RBAC
├── plugins/                  
├── logs/                     
├── config/                   
│   ├── trino/                
│   │   └── iceberg.properties 
│   ├── prometheus/
│   │   └── prometheus.yml
│   └── airflow/
│       └── webserver_config.py # Configurações de segurança/RBAC
├── docker/
│   └── ingestao_empresasbr/
│       ├── Dockerfile        
│       └── script.py         
├── tests/                    
├── docker-compose.yml        
└── Makefile                  
```