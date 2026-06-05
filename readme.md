# Coupang-Scale Low-Latency Multi-Stage Product Discovery Agent
### 쿠팡 대규모 트래픽 대응 초저지연 멀티 스테이지 상품 검색 에이전트

An enterprise-grade blueprint and proof-of-concept architecture for high-throughput, asymmetric multi-stage semantic product search. This repository models how to handle petabyte-scale e-commerce catalogs under strict transaction and latency boundaries (<250ms p99 SLA) using a decoupled data lifecycle and state-of-the-art MLOps tools.

본 리포지토리는 고처리량 및 비대칭형 멀티 스테이지 시맨틱 상품 검색을 구현한 엔터프라이즈급 아키텍처 청사진입니다. 분리된 데이터 라이프사이클과 최신 MLOps 도구를 활용하여, 엄격한 트랜잭션 및 지연 시간 제약 조건(<250ms p99 SLA) 하에서 페타바이트 규모의 이커머스 카탈로그를 처리하는 방법을 모델링합니다.

---
1. 📂 Project Structure / 디렉토리 구조

```text
├── 📂 core/              # AsyncIO Gateway & Triton High-Throughput Client
├── 📂 deployment/        # Kubernetes / Kubeflow Manifests & Triton Models config.pbtxt
├── 📂 infrastructure/    # DB Abstraction Interface (Qdrant & Milvus Impl)
├── 📂 orchestration/     # Airflow ETL Pipeline DAG
└── 📂 pipelines/         # Spark Batch Processing & Ray Distributed Embedding
```
2. 🔍 각 파일의 명확한 역할과 엔지니어링 팩트

① 실시간 트래픽 처리 및 비즈니스 랭킹 결합 (core/)

gateway.py (ASGI 웹서버 엔진): 외부 유저의 검색 요청을 FastAPI 비동기(async/await) 구조로 수신합니다. 단순 ML 점수 정렬이 아니라, 쿠팡 광고주 상품 가중치(ad_boost_multiplier)와 데이터 기반 클릭률 보정값(ctr_score_weight)을 실시간으로 결합해 최종 랭킹을 뽑아내는 Multi-Objective Optimization이 완벽히 구현되어 있습니다. 하단에는 성능을 극대화하기 위해 uvicorn을 멀티 워커(workers=4)와 고속 이벤트 루프(loop="uvloop")로 기동하는 진입점까지 명시되어 있습니다.

triton_client.py (고성능 추론 클라이언트): 허깅페이스의 Rust 기반 Fast 토크나이저를 사용해 텍스트를 실시간 인코딩하며 CPU 병목을 방어합니다. 엔지니어링의 백미는 Triton 서버에 데이터를 던질 때 binary_data=True 옵션을 주어 텐서를 원시 바이트 패킷으로 직렬화해 전송하는 오버헤드 최적화, 그리고 Triton 장애나 큐 정체 시 전체 시스템이 다운되는 것을 막기 위해 timeout=0.05(50ms)로 제어하는 서킷 브레이커(Fallback) 로직이 장착된 점입니다.

② 백엔드 빅데이터 플랫폼 및 주기적 인덱싱 (pipelines/ & orchestration/)

data_etl_spark.py (Spark 파이프라인): 대규모 분산 환경에서 로그 데이터를 긁어와 노출(impression), 클릭(click), 구매(purchase) 데이터를 분산 집계(groupBy().agg())합니다. 특히 데이터 처리 중 터지기 쉬운 0 나누기 에러(Division by Zero)를 F.when().otherwise()로 원천 방어하고, 조인 스큐(Data Skew)를 막기 위해 na.fill() 처리 후 카테고리별로 파티셔닝 적재를 수행하는 완벽한 상용 사양입니다.

catalog_indexing_dag.py (Airflow 스케줄러): 위에서 언급한 Spark 분산 ETL 작업이 성공적으로 완료되면, 이어서 Ray 분산 클러스터를 가동해 대규모 상품 카탈로그의 고차원 벡터 임베딩을 빌드하도록 데이터 의존성 셔플 흐름(execute_spark_etl >> execute_distributed_embeddings)을 제어합니다.

③ 스토리지 격리 및 인프라 레이어 (infrastructure/ & deployment/)

base_store.py, qdrant_impl.py, milvus_impl.py (벡터 스토어 추상화): 데이터베이스 의존성을 차단하기 위해 인터페이스 구조를 채택했습니다. Qdrant는 prefer_grpc=True를 이용해 HTTP/2 고속 멀티플렉싱 오버헤드를 줄였고, Milvus는 Segment를 RAM에 직접 상주시키는 최적화(collection.load()) 기법을 활용해 1차 필터링 검색 속도를 극대화했습니다.

config.pbtxt (Triton 추론 엔진 설정): 대형 Cross-Encoder 모델을 GPU 인스턴스 2개 풀(count: 2)에 병렬 할당하고, 처리량 향상과 지연 시간 제어의 트레이드오프를 맞추기 위해 최대 큐 대기 제한을 5ms(max_queue_delay_microseconds: 5000)로 제어하는 Dynamic Batching 사양이 정의되어 있습니다.

kubernetes-spec.yaml & kubeflow_pipeline.py (클라우드 네이티브 배포 및 가용성): 고가용성(HA)을 보장하기 위해 최초 10개의 파드로 시작해 트래픽 폭주 시 최대 200개 파드까지 자동으로 확장되는 HPA(HorizontalPodAutoscaler), 컨테이너 헬스체크(readinessProbe), 그리고 Kubeflow를 통한 지속적 재학습 모델 검증 자동화가 설계되어 있습니다.

3. 🎯 한 줄 결럼
이 코드는 "대규모 커머스 환경에서 지연 시간 SLA(<120ms p99)를 사수하면서도, ML 모델의 정밀도와 플랫폼의 비즈니스 수익(광고/CTR)을 동시에 극대화해 낸 최고 수준의 아키텍처 실전 진본"입니다.

## 🏗️ System Topology & Dataflow / 시스템 구조도

```text
 [ Raw E-Commerce Logs / Catalogs (SQL / BigQuery) ]
                        │
                        ▼ (Scheduled Orchestration via Apache Airflow)
 ┌──────────────────────────────────────────────────────────┐
 │ 1. DATA & TRAINING LIFECYCLE (Ray & Apache Spark Cluster)│
 │  - Spark: Petabyte ETL, clickstream aggregation, SQL     │
 │  - Ray: Distributed PyTorch / Transformers Embedding     │
 │  - MLflow: Hyperparameter tracking & Model Registry      │
 └──────────────────────────────────────────────────────────┘
                        │
         ┌──────────────┴──────────────┐
         ▼                             ▼
 [ High-Recall Vector Upsert ]     [ Optimized Model Weights ]
         │                             │
         ▼                             ▼ (CD Manifests via Kubeflow)
 ┌────────────────────────────────────────────────────────┐
 │ 2. LOW-LATENCY PRODUCTION RUNTIME (Kubernetes Cluster) │
 │                                                        │
 │   ┌──────────────────┐      ┌──────────────────────┐   │
 │   │ Triton Inf. Serv.│ ◄──► │ Core Agent Gateway   │   │
 │   │ - PyTorch Models │      │ - AsyncIO Python     │ ◄─┼─► [ End User ]
 │   │ - Cross-Encoder  │      │ - HuggingFace Tokens │   │
 │   └──────────────────┘      └──────────────────────┘   │
 │            ▲                                           │
 │            └───────────► [ Qdrant / Milvus Cluster ]   │
 │                          (Stage-1 Pre-filtered Search) │
 └────────────────────────────────────────────────────────┘
```
## 🛠️ Technology Stack / 기술 스택

* **Data Lifecycle:** Apache Spark, Apache Airflow, BigQuery, SQL
* **Distributed AI / MLOps:** Ray, PyTorch, Hugging Face Transformers, MLflow, Kubeflow
* **Runtime & Serving:** Python AsyncIO, Triton Inference Server
* **Vector DB Ecosystem:** Qdrant, Milvus
* **Infrastructure:** Kubernetes Cluster

## 🛠️ Technology Stack & Purpose / 핵심 기술 및 활용 목적

| Category | Component | Purpose / 활용 목적 |
| :--- | :--- | :--- |
| **ML & Search** | PyTorch, Cross-Encoder | 고정밀 문맥 매칭 및 검색 결과 실시간 재정렬(Re-ranking) 연산 |
| **Vector Engine** | Qdrant, Milvus | 컴퓨트-스토리지 분리 구조 기반 대규모 벡터 인덱싱 및 필터링 |
| **Data Platform** | Apache Spark, Ray | 페타바이트급 데이터 정제(ETL) 및 분산 GPU 임베딩 생성 자동화 |
| **Serving / MLOps** | Triton, Kubernetes, Airflow | Dynamic Batching을 활용한 모델 추론 가속 및 파이프라인 배포 자동화 |
---

## 🎯 Production SLA / 성능 목표

* **Stage-1 벡터 검색 지연 시간:** `< 15ms` (HNSW index pre-filtered)
* **Stage-2 Triton 모델 추론 지연 시간:** `< 35ms` (Dynamic batching queued)
* **코어 게이트웨이 최종 p99 목표 지연 시간:** `< 120ms`
  
## 🔍 Search Pipeline Architecture / 검색 파이프라인 구조

The system employs a multi-stage approach to balance latency boundaries (<250ms p99 SLA) with high retrieval accuracy:

1. **Stage-1: Pre-filtered Vector Search**
   * The Core Agent Gateway receives user input, processes tokens, and queries the Qdrant/Milvus cluster.
   * Filters and isolates candidate subsets rapidly from massive vector spaces.

2. **Stage-2: High-Precision Re-ranking**
   * Top candidates are passed to Triton Inference Server.
   * A heavy Cross-Encoder model runs intense sequence-pair interactions to output final optimized product rankings.

## 🚀 Key Architectural Pillars / 핵심 아키텍처 요소

### 1. Data & Training Lifecycle (데이터 및 학습 라이프사이클)
* **Orchestration:** Managed via Apache Airflow scheduling to automate workloads from raw SQL/BigQuery data logs.
* **Data Processing:** Powered by an Apache Spark cluster for petabyte-scale ETL pipelines and clickstream aggregation.
* **Distributed ML:** Utilizes Ray for distributed PyTorch and Hugging Face Transformers embedding generation.
* **Experimentation:** Leverages MLflow for hyperparameter tracking and managing the central Model Registry.

### 2. Low-Latency Production Runtime (초저지연 프로덕션 런타임)
* **Core Agent Gateway:** Built with asynchronous Python (AsyncIO) and Hugging Face Tokenizers for ultra-fast, non-blocking request handling.
* **Model Serving:** Powered by Triton Inference Server executing optimized PyTorch and Cross-Encoder models.
* **Vector Indexing:** Uses a Qdrant or Milvus cluster to perform high-recall, pre-filtered Stage-1 vector search.
---
