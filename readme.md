# 쿠팡 초저지연 멀티 스테이지 상품 검색 에이전트

본 리포지토리는 고처리량 및 비대칭형 멀티 스테이지 시맨틱 상품 검색을 구현한 엔터프라이즈급 아키텍처 청사진입니다. 분리된 데이터 라이프사이클과 최신 MLOps 도구를 활용하여, 엄격한 트랜잭션 및 지연 시간 제약 조건(<250ms p99 SLA) 하에서 페타바이트 규모의 이커머스 카탈로그를 처리하는 방법을 모델링합니다.

1. 📂 Directory Structure / 디렉토리 구조

```text
├── 📂 core/              # AsyncIO Gateway & Triton High-Throughput Client
├── 📂 deployment/        # Kubernetes / Kubeflow Manifests & Triton Models config.pbtxt
├── 📂 infrastructure/    # DB Abstraction Interface (Qdrant & Milvus Impl)
├── 📂 orchestration/     # Airflow ETL Pipeline DAG
└── 📂 pipelines/         # Spark Batch Processing & Ray Distributed Embedding
```
2. 🔍 Component Breakdown & Core Engineering Facts / 각 파일의 명확한 역할과 엔지니어링 팩트

### 실시간 트래픽 처리 및 복합 비즈니스 랭킹 결합(`core/`)

* **`gateway.py` (Asynchronous ASGI Web Server Engine / ASGI 웹서버 엔진)**
  * **영문 (EN):** Ingests external client search requests asynchronously utilizing `FastAPI`’s `async/await` paradigm to handle thousands of concurrent connections without thread blocking.
  * **한글 (KO):** 외부 클라이언트의 검색 요청을 `FastAPI` 비동기(`async/await`) 루프 체계로 수신하여, 스레드 블로킹 없이 수천 건의 동시 연결을 안정적으로 처리합니다.
  * **Engineering Fact:** Implements a sophisticated **Multi-Objective Optimization** ranking algorithm directly into the final hot path. Instead of blindly sorting items by raw machine learning scores, it dynamically computes a hybrid rank by combining the contextual semantic relevance (`base_ml_score`) with programmatic business modifiers: the ad sponsored boost (`ad_boost_multiplier`) and historical click-through rate adjustments (`ctr_score_weight`). For maximum execution performance under heavy production traffic, the entry point boots `uvicorn` utilizing multiple processes (`workers=4`) bound to the high-performance `uvloop` event loop.
  * **핵심 팩트:** 단순 ML 스코어 줄세우기를 넘어, 딥러닝 문맥 점수(`base_ml_score`)에 쿠팡 비즈니스의 핵심인 광고 가중치(`ad_boost_multiplier`)와 클릭률 보정값(`ctr_score_weight`)을 결합하는 **다중 목적 최적화(Multi-Objective Optimization)** 랭킹 수식을 실시간 핫 패스에 구현했습니다. 트래픽 처리량을 극대화하기 위해 `uvicorn` 멀티 프로세스 워커(`workers=4`) 및 고속 `uvloop` 이벤트 엔진을 연동했습니다.

* **`triton_client.py` (High-Throughput Inference Client / 고성능 추론 클라이언트)**
  * **영문 (EN):** Orchestrates the encoding phase and handles strict inference protocol communications with the centralized Triton Inference cluster.
  * **한글 (KO):** 실시간 텍스트 인코딩 단계를 제어하고, 중앙화된 Triton 인프라 클러스터와의 엄격한 고속 추론 프로토콜 통신을 관장합니다.
  * **Engineering Fact:** Mitigates severe CPU bottlenecks by integrating HuggingFace's Rust-backed Fast Tokenizer to compute contextual string sequences in real time. To eliminate JSON overhead and string serialization costs over HTTP, it utilizes **`binary_data=True`** parameters to streamline raw byte tensor packets. Critically, to preserve strict p99 latency SLAs and guard against cascading downstream queue timeouts, it encapsulates a **Circuit Breaker** fallback layer governed by a definitive 50ms constraint (`timeout=0.05`), returning zeroed arrays if the server encounters cluster degradation.
  * **핵심 팩트:** 실시간 문자열 처리 시 발생하는 CPU 병목을 방어하기 위해 허깅페이스의 Rust 기반 Fast Tokenizer를 장착했습니다. HTTP 통신 과정에서 발생하는 JSON 스트링 직렬화 비용을 원천 차단하고자 **`binary_data=True`** 옵션을 통해 원시 바이트 패킷 형태로 텐서를 전송합니다. 특히 전체 시스템의 p99 지연 시간 SLA를 사수하고 큐 정체로 인한 장애 전파를 막기 위해, 50ms 타임아웃(`timeout=0.05`) 제약 기반의 **서킷 브레이커(Fallback)** 방어 레이어를 구축했습니다.
  
### 나. Analytical Data Lake & Pipeline Orchestration (`pipelines/` & `orchestration/`)
### 백엔드 빅데이터 플랫폼 및 주기적 인덱싱 파이프라인

* **`data_etl_spark.py` (Enterprise-Scale Spark Pipeline / 대규모 스케일 Spark 파이프라인)**
  * **영문 (EN):** Processes massive distributed user clickstream log dumps and raw catalogs across the company’s data lake to build aggregated analytical feature stores.
  * **한글 (KO):** 사내 데이터 레이크에 축적된 대규모 분산 유저 클릭스트림 로그 덤프와 원천 상품 카탈로그를 정제하여 대용량 분석 피처 스토어를 빌드합니다.
  * **Engineering Fact:** Performs high-throughput data aggregation (`groupBy().agg()`) to synthesize historical impression, click, and purchase event sequences into actionable metrics. Engineered to handle large-scale distributed edge cases, it utilizes declarative `F.when().otherwise()` matrices to fundamentally block **Division-by-Zero runtime exceptions** when computing CTR or conversion rates. It explicitly applies `na.fill()` rules to neutralize severe data skew issues before writing out highly optimized Category-partitioned Parquet files back to storage.
  * **핵심 팩트:** 초고처리량 분산 집계 연산(`groupBy().agg()`)을 통해 과거 노출, 클릭, 구매 이벤트 시퀀스를 활용 가능한 피처 지표로 정제합니다. 분산 데이터 처리 중 발생하기 쉬운 예외 상황을 방어하기 위해, `F.when().otherwise()` 연산식을 사용하여 CTR 및 전환율 계산 시 **0 나누기 에러(Division-by-Zero)를 원천 차단**합니다. 또한, 조인 과정에서 터지기 쉬운 데이터 스큐(Data Skew) 현상을 방지하기 위해 명시적인 `na.fill()` 처리 후 카테고리별 파티셔닝 Parquet 형태로 안전하게 분산 적재합니다.

* **`catalog_indexing_dag.py` (Apache Airflow Scheduler / Airflow 배치 스케줄러)**
  * **영문 (EN):** Enforces definitive data dependency workflows, orchestrating the periodic index update cycles for the entire system.
  * **한글 (KO):** 데이터 간의 명확한 의존성 워크플로우를 강제하며, 시스템 전체의 주기적인 벡터 인덱싱 업데이트 사이클을 조율합니다.
  * **Engineering Fact:** Configures explicit task sequence parameters (`execute_spark_etl >> execute_distributed_embeddings`) using the `KubernetesPodOperator`. This structural design ensures that the high-recall Ray multi-GPU embedding cluster is only initialized after the upstream PySpark data lake synchronization job terminates with an absolute success state, maintaining transactional consistency across the vector storage tier.
  * **핵심 팩트:** `KubernetesPodOperator` 환경에서 작업 간 선후 관계 파라미터(`execute_spark_etl >> execute_distributed_embeddings`)를 명확히 구성했습니다. 이를 통해 업스트림의 PySpark 데이터 정제 작업이 완벽히 정합성을 유지하며 정상 종료된 경우에만, 다운스트림인 Ray 멀티 GPU 분산 임베딩 클러스터가 기동되도록 강제하여 벡터 스토리지 전체의 트랜잭션 일관성을 보장합니다.

### 다. Isolated Storage Abstraction & Cloud-Native Deployment (`infrastructure/` & `deployment/`)
### 스토리지 격리 추상화 및 클라우드 네이티브 배포 레이어

* **`base_store.py`, `qdrant_impl.py`, `milvus_impl.py` (Vector Store Layer Abstraction / 벡터 스토어 레이어 추상화)**
  * **영문 (EN):** Acts as the persistent decoupled layer isolating core routing logic from specialized database client SDK specifications.
  * **한글 (KO):** 코어 에이전트 라우팅 로직이 특정 데이터베이스 클라이언트 SDK 명세에 종속되지 않도록 영속성 레이어의 결합도를 완벽히 분리(Decoupling)합니다.
  * **Engineering Fact:** Follows clean object-oriented architecture patterns. The Qdrant driver implements a dedicated gRPC channel multiplexing approach via **`prefer_grpc=True`** to drastically curtail connection layer handshake overheads. Simultaneously, the Milvus implementation leverages aggressive in-memory index caching (`collection.load()`) to enforce high-recall pre-filtering logic natively inside the HNSW storage segments, accelerating Stage-1 query execution times.
  * **핵심 팩트:** 객체지향 인터페이스 패턴을 엄격히 준수합니다. Qdrant 구현체는 커넥션 레이어의 핸드셰이크 오버헤드를 제로에 가깝게 줄이기 위해 **`prefer_grpc=True`** 설정을 통한 gRPC 채널 멀티플렉싱을 적용했습니다. Milvus 구현체는 상용 트래픽 대응을 위해 세그먼트를 RAM 영역에 영구 상주시키는 고속 캐싱 기법(`collection.load()`)을 연동하여, HNSW 그래프 레이어 내부에서 데이터베이스 레벨의 Pre-filtering 1단계 검색 속도를 극대화했습니다.

* **`config.pbtxt` (Triton Inference Engine Specification / Triton 추론 엔진 설정)**
  * **영문 (EN):** Configures low-level hardware resource mapping and request optimization strategies for heavy Cross-Encoder models.
  * **한글 (KO):** 연산 비용이 높은 무거운 Cross-Encoder 모델을 가속하기 위해 하위 하드웨어 자원 매핑 및 요청 최적화 전략을 제어합니다.
  * **Engineering Fact:** Scales hardware throughput limits by deploying concurrent model instances mapped across an active GPU pool (`count: 2`). To extract optimal hardware saturation while managing tail latencies, it enables **Dynamic Batching** constrained by a maximum execution delay threshold of 5 milliseconds (`max_queue_delay_microseconds: 5000`), allowing the engine to aggregate individual inference calls into compact matrices without violating real-time SLA bounds.
  * **핵심 팩트:** 활성화된 GPU 자원 풀 전체에 복수의 모델 인스턴스를 병렬 배치(`count: 2`)하여 하드웨어 처리량 한계를 확장했습니다. 테일 레이턴시(Tail Latency)를 엄격히 통제하면서 하드웨어 자원을 100% 소모하기 위해 최대 대기 제한을 5ms로 묶은 **다이내믹 배칭(Dynamic Batching, `max_queue_delay_microseconds: 5000`)** 프로덕션 규격을 선언, 실시간 SLA 한계를 침범하지 않고 요청들을 고속 행렬 연산으로 취합 처리합니다.

* **`kubernetes-spec.yaml` & `kubeflow_pipeline.py` (Cloud-Native Orchestration & Resiliency / 가용성 및 파이프라인 자동화)**
  * **영문 (EN):** Defines infrastructure-as-code manifests to maintain high availability and automated model lifecycle governance.
  * **한글 (KO):** 인프라 명세를 코드로 관리(IaC)하여 시스템의 상용 고가용성을 유지하고, 데이터 지속 재학습 및 모델 라이프사이클 관리를 자동화합니다.
  * **Engineering Fact:** Guarantees web tier resiliency by specifying an active multi-pod deployment baseline paired with a native **HorizontalPodAutoscaler (HPA)** configured to automatically elastic-scale from a minimum of 10 up to 200 replicas during massive traffic spikes. Employs predictive `readinessProbe` hooks for traffic control during continuous deployment cycles, while matching the system with Kubeflow pipeline code designed to automate model validation and continuous evaluation thresholds.
  * **핵심 팩트:** 웹 레이어의 회복 탄력성을 보장하기 위해 최초 10개의 파드로 시작해 대규모 트래픽 폭주 시 **최대 200개 레플리카 파드까지 자동 확장되는 HPA(HorizontalPodAutoscaler)** 인프라 사양을 선언했습니다. 무중단 배포 스케줄러 가동 시 트래픽 진입을 안전하게 제어하는 `readinessProbe` 헬스체크 훅을 연동했으며, 검증 지연을 막기 위해 지정된 메트릭 임계치 기반의 자동화된 Kubeflow 파이프라인 지속 검증 코드를 명시했습니다.
 ---

3. 🏗️ System Topology & Dataflow / 시스템 구조도

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
4. 🛠️ Technology Stack / 기술 스택

* **Data Lifecycle:** Apache Spark, Apache Airflow, BigQuery, SQL
* **Distributed AI / MLOps:** Ray, PyTorch, Hugging Face Transformers, MLflow, Kubeflow
* **Runtime & Serving:** Python AsyncIO, Triton Inference Server
* **Vector DB Ecosystem:** Qdrant, Milvus
* **Infrastructure:** Kubernetes Cluster

5. 🛠️ Technology Stack & Purpose / 핵심 기술 및 활용 목적

| Category | Component | Purpose / 활용 목적 |
| :--- | :--- | :--- |
| **ML & Search** | PyTorch, Cross-Encoder | 고정밀 문맥 매칭 및 검색 결과 실시간 재정렬(Re-ranking) 연산 |
| **Vector Engine** | Qdrant, Milvus | 컴퓨트-스토리지 분리 구조 기반 대규모 벡터 인덱싱 및 필터링 |
| **Data Platform** | Apache Spark, Ray | 페타바이트급 데이터 정제(ETL) 및 분산 GPU 임베딩 생성 자동화 |
| **Serving / MLOps** | Triton, Kubernetes, Airflow | Dynamic Batching을 활용한 모델 추론 가속 및 파이프라인 배포 자동화 |
---

6. 🎯 Production SLA / 성능 목표

* **Stage-1 벡터 검색 지연 시간:** `< 15ms` (HNSW index pre-filtered)
* **Stage-2 Triton 모델 추론 지연 시간:** `< 35ms` (Dynamic batching queued)
* **코어 게이트웨이 최종 p99 목표 지연 시간:** `< 120ms`
  
7. 🔍 Search Pipeline Architecture / 검색 파이프라인 구조

The system employs a multi-stage approach to balance latency boundaries (<250ms p99 SLA) with high retrieval accuracy:

가. **Stage-1: Pre-filtered Vector Search**
   * The Core Agent Gateway receives user input, processes tokens, and queries the Qdrant/Milvus cluster.
   * Filters and isolates candidate subsets rapidly from massive vector spaces.

나. **Stage-2: High-Precision Re-ranking**
   * Top candidates are passed to Triton Inference Server.
   * A heavy Cross-Encoder model runs intense sequence-pair interactions to output final optimized product rankings.

8. 🚀 Key Architectural Pillars / 핵심 아키텍처 요소

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
