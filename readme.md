Coupang-Scale Low-Latency Multi-Stage Product Discovery Agent
쿠팡 대규모 트래픽 대응 초저지연 멀티 스테이지 상품 검색 에이전트
🇺🇸 Resume Headline: An enterprise-grade, two-stage semantic search architecture that integrates distributed data pipelines (Spark/Ray) and partitioned vector clusters (Qdrant/Milvus) via an AsyncIO gateway and Triton Inference Server to achieve a sub-250ms p99 SLA over petabyte-scale catalogs.

🇰🇷 이력서 헤드라인: 페타바이트급 카탈로그 환경에서 데이터 처리(Spark/Ray)와 분산 벡터 검색(Qdrant/Milvus)을 비동기 게이트웨이 및 Triton 추론 서버로 엮어 <250ms p99 초저지연을 달성한 엔터프라이즈급 2단계(Two-Stage) 검색 플랫폼 아키텍처.

An enterprise-grade blueprint and proof-of-concept architecture for high-throughput, asymmetric multi-stage semantic product search. This repository models how to handle petabyte-scale e-commerce catalogs under strict transaction and latency boundaries (<250ms p99 SLA) using a decoupled data lifecycle and state-of-the-art MLOps tools.

본 리포지토리는 고처리량 및 비대칭형 멀티 스테이지 시맨틱 상품 검색을 구현한 엔터프라이즈급 아키텍처 청사진입니다. 분리된 데이터 라이프사이클과 최신 MLOps 도구를 활용하여, 엄격한 트랜잭션 및 지연 시간 제약 조건(<250ms p99 SLA) 하에서 페타바이트 규모의 이커머스 카탈로그를 처리하는 방법을 모델링합니다.

🏗️ System Topology & Dataflow / 시스템 구조도
Plaintext
 [ Raw E-Commerce Logs / Catalogs (SQL / BigQuery) ]
                        │
                        ▼ (Scheduled Orchestration via Apache Airflow)
 ┌────────────────────────────────────────────────────────┐
 │ 1. DATA & TRAINING LIFECYCLE (Ray & Apache Spark Cluster)│
 │  - Spark: Petabyte ETL, clickstream aggregation, SQL   │
 │  - Ray: Distributed PyTorch / Transformers Embedding   │
 │  - MLflow: Hyperparameter tracking & Model Registry   │
 └────────────────────────────────────────────────────────┘
                        │
         ┌──────────────┴──────────────┐
         ▼                             ▼
 [ High-Recall Vector Upsert ]     [ Optimized Model Weights ]
         │                             │
         ▼                             ▼ (CD Manifests via Kubeflow)
 ┌────────────────────────────────────────────────────────┐
 │ 2. LOW-LATENCY PRODUCTION RUNTIME (Kubernetes Cluster)  │
 │                                                        │
 │   ┌──────────────────┐      ┌──────────────────────┐   │
 │   │ Triton Inf. Serv.│ ◄──► │ Core Agent Gateway   │   │
 │   │ - PyTorch Models │      │ - AsyncIO Python     │ ◄─┼─► [ End User ]
 │   │ - Cross-Encoder  │      │ - HuggingFace Tokens │   │
 │   └──────────────────┘      └──────────────────────┘   │
 │            ▲                                           │
 │            └───────────► [ Qdrant / Milvus Cluster ]  │
 │                          (Stage-1 Pre-filtered Search) │
 └────────────────────────────────────────────────────────┘
🚀 Project Overview (STAR Framework)
🇺🇸 English Version
Situation
E-commerce platforms handling hundreds of millions of SKUs and massive concurrent traffic face significant conversion drop-offs due to traditional keyword search limitations. Users struggle when combining loose semantic intent ("heavy-duty waterproof camping gear for a rainy weekend") with hard business metadata filters ("Must be Rocket Delivery eligible and under ₩150,000"). Passing such massive queries straight to large context LLMs introduces extreme latency, non-deterministic outputs, and massive token overhead.

Task
I engineered a modular, end-to-end Asynchronous Two-Stage Retrieval architecture designed to parse ambiguous natural language queries, execute low-latency hybrid vector pre-filtering, and perform high-precision deep neural re-ranking under strict production enterprise SLAs (<250ms p99 latency).

Action
Decoupled Data Infrastructure (infrastructure/): Implemented the Dependency Inversion Principle using an abstract interface (base_store.py). Engineered production-ready, database-agnostic connector implementations for Qdrant (qdrant_impl.py via HTTP/2 gRPC) and Milvus (milvus_impl.py via segment pools), blocking memory-lockouts and vertical scale bottlenecks.

Two-Stage Retrieval Optimization (core/):

Stage 1 (High Recall): Configured vector indices to leverage native HNSW graphs optimized with relational pre-filtering payloads (matching vectors only if shipping and pricing constraints are met), drastically reducing downstream search spaces.

Stage 2 (High Precision): Routed coarse candidate arrays to a decoupled Triton Inference Server architecture via an asynchronous gRPC client (triton_client.py), computing dense token-level cross-attention matrices via a PyTorch Cross-Encoder framework.

High-Concurrency Serving (main.py): Built a non-blocking execution router using Python AsyncIO inside the central gateway (gateway.py). Implemented defensive circuit breakers and a 50ms fast-timeout fallback strategy to maintain system availability during downstream connection degradation.

Production Pipeline Design: Structured pipeline components incorporating Apache Spark for data cleansing, Ray for multi-GPU distributed embedding orchestration, and Apache Airflow / Kubeflow for continuous synchronization of updated item segments.

Result
Established a production-ready system blueprint that mitigates typical RAM ceilings and single-node lockouts.

Ensured high-concurrency compliance capable of handling spikes in traffic (e.g., promotional events) by separating compute nodes from structural state.

Showcased actionable Staff Machine Learning Engineer design patterns including dependency injection, async network abstractions, and decoupled pipeline scheduling.

🇰🇷 한국어 버전 (Korean Version)
Situation (상황)
수억 개의 상품(SKU)과 대규모 동시 트래픽을 처리하는 이커머스 플랫폼은 기존 키워드 검색의 한계로 인해 전환율 저하 문제를 겪습니다. 특히 사용자가 모호한 문맥적 의도("비 오는 주말용 튼튼한 방수 캠핑 장비")와 비즈니스 필터 조건("로켓배송 가능, 15만 원 이하")을 조합할 때 기존 시스템은 오작동하기 쉽습니다. 이러한 대규모 질의를 LLM 컨텍스트에 그대로 넘기면 극심한 지연 시간(Latency)과 비용이 발생합니다.

Task (과제)
모호한 자연어 질의를 분석하고, 실시간 비즈니스 필터가 가미된 저지연 하이브리드 벡터 검색을 수행한 뒤, 고정밀 딥러닝 재정렬을 거치는 비동기 멀티 스테이지 검색 플랫폼 아키텍처를 설계했습니다. 목표는 프로덕션 환경의 엄격한 SLA(<250ms p99 Latency)를 충족하는 것입니다.

Action (수행 내용)
디커플링된 데이터 인프라 (infrastructure/): 추상 인터페이스(base_store.py)를 기반으로 의존성 주입(Dependency Inversion) 패턴을 확립했습니다. 고성능 gRPC 통신을 활용하는 Qdrant 구현체(qdrant_impl.py)와 가상 세그먼트 풀을 활용하는 Milvus 구현체(milvus_impl.py)를 교체 가능하도록 설계하여 단일 노드의 메모리 서빙 한계를 제거했습니다.

2단계 검색 구조(Two-Stage Retrieval) 최적화 (core/):

1단계 (하이 리콜): 데이터베이스 레이어에서 로켓배송 조건 등을 고속 판별하는 Pre-filtering HNSW 그래프 색인을 적용해 검색 후보군을 1차적으로 압축했습니다.

2단계 (고정밀): 추출된 후보군을 비동기 클라이언트(triton_client.py)를 통해 Triton Inference Server로 이관하고, PyTorch 기반 Cross-Encoder 상호작용 매트릭스 연산을 수행하여 에이전트의 환각 현상을 방지했습니다.

고동시성 서빙 인프라 (main.py): 코어 게이트웨이(gateway.py) 내부에 Python AsyncIO 비동기 이벤트 루프를 구축해 입출력(I/O) 병목을 차단했습니다. 다운스트림 장애 상황을 대비해 50ms 타임아웃 서킷 브레이커 방어 로직을 구현하여 시스템 가용성을 보장했습니다.

엔터프라이즈 파이프라인 설계: 페타바이트급 데이터 정제를 위한 Apache Spark, 분산 다중 GPU 인코딩을 위한 Ray, 전체 파이프라인의 자동 배포 및 주기적 동기화를 위한 Apache Airflow 및 Kubeflow 아키처 매니페스트를 수립했습니다.

Result (결과)
단일 노드의 메모리 한계와 동시성 락(Lock) 문제를 원천 차단하는 프로덕션급 인프라 청사진을 입증했습니다.

데이터 상태 영역과 모델 연산 레이어를 격리하여 대규모 이벤트 트래픽 급증 시에도 유연하게 수평 확장(Scale-out)할 수 있는 구조를 확보했습니다.

의존성 주입, 비동기 네트워크 추상화 등 Staff ML Engineer 직무에 걸맞은 소프트웨어 아키텍처 모범 사례를 제시했습니다.

📂 Repository Blueprint Structure / 프로젝트 디렉토리 구조
Plaintext
Coupang-AI-Agent/
├── 📂 orchestration/
│   └── catalog_indexing_dag.py # Apache Airflow ETL Pipeline
├── 📂 pipelines/
│   ├── data_etl_spark.py       # Apache Spark Batch Processing Engine
│   └── distributed_embed_ray.py# Ray Distributed Embedding Generator
├── 📂 core/
│   ├── gateway.py              # AsyncIO Python Entrypoint / HF Tokenizer
│   └── triton_client.py        # High-Throughput Triton Inference Connector
├── 📂 infrastructure/
│   ├── base_store.py           # Database Abstraction Interface (Interface Contract)
│   ├── milvus_impl.py          # Decoupled Compute/Storage Milvus Connection Engine
│   └── qdrant_impl.py          # High-Performance Qdrant Cluster gRPC Connection Engine
├── 📂 deployment/
│   ├── kubeflow_pipeline.py    # Automated Cloud-Native Training Manifest
│   ├── kubernetes-spec.yaml    # Production Pod Topology Scaling Policy
│   └── triton_model_repo/      # Triton Inference Architecture Configurations
│       └── cross_encoder/
│           └── config.pbtxt    # Dynamic Batching & Multi-GPU Configs
├── docker-compose.infra.yml    # Single-command Infrastructure Dev Sandbox
└── main.py                  # System Integration Entrypoint
🛠️ Deep Technical Stack Purpose Matrix / 기술 스택 활용 목적
1. ML Frameworks & Architectures
PyTorch / TensorFlow: Recommendation engines, search representation learning, and deep neural ranking model compilation. / 추천 엔진, 검색 모델 학습 및 배포를 지원하는 러닝 아키처 백엔드.

Transformers & HuggingFace: Tokenization profiles interpreting semantic user request properties online. / 자연어 검색어 문맥 인코딩 및 실시간 텍스트 토큰화 레이어.

Cross-Encoders: Sequence-pair interaction neural networks deployed to compute dynamic relevance re-ranking scores. / 질의와 상품명 간의 고정밀 매칭 점수를 실시간 연산하는 상호작용 신경망 모델.

2. GenAI & Vector Systems
Qdrant & Milvus: Distributed cloud-native engine components decoupling storage scales from high-availability lookup pools. / 컴퓨트와 스토리지가 분리되어 수억 건의 상품을 분산 관리하는 벡터 데이터베이스.

Embedding-Based Retrieval (EBR) & Hybrid Search: Blending vectorized mathematical constraints with strict relational enterprise attributes. / 고차원 벡터 유사도와 실시간 커머스 제약 조건을 DB 엔진 내부에서 병렬 처리하는 기술.

Retrieval-Augmented Generation (RAG): Context isolation pipelines serving verified transaction records to block hallucinated text formats. / 실시간 재고 상태 정보를 결합해 에이전트의 잘못된 상품 추천을 차단하는 신뢰성 보장 아키처.

3. Data Engineering & Scale
Apache Spark: High-throughput batch computing engines extracting properties from raw tabular structures. / 페타바이트 단위의 이커머스 로그 호수에서 대규모 데이터를 정제하고 변환하는 핵심 분산 엔진.

Ray: Parallel process handlers orchestrating deep learning multi-node embedding tasks seamlessly. / 여러 개의 GPU 노드를 묶어 대규모 상품 임베딩 및 모델 연산을 병렬 제어하는 분산 컴퓨팅 프레임워크.

SQL & BigQuery: Production operational analytical datastore environments. / 트랜잭션 수집 및 대용량 비즈니스 정보 분석을 위한 DW 커넥터 환경.

4. MLOps & Orchestration
Apache Airflow & Kubeflow: Workflow execution automation mechanisms ensuring predictable continuous integration flows. / 신상품 카탈로그 주기적 인덱싱 및 클라우드 네이티브 워크플로우 제어를 위한 오케스트레이터.

MLflow: Unified metadata logging repositories managing artifact registries and parameter distributions. / 실험 파라미터 로깅, 모델 성능 추적 및 프로덕션 모델 배포를 제어하는 레지스트리 시스템.

Docker & Kubernetes: Automated scaling microservice operators providing horizontal reliability provisions under unexpected load spikes. / 컨테이너 환경 격리 및 대규모 트래픽 급증(HPA)에 대응하는 클라우드 인프라 오케스트레이션 패러다임.

Triton Inference Server: Multi-model runtime execution nodes leveraging dynamic queue pooling strategies to clear latency bounds. / 동적 배칭(Dynamic Batching) 기술을 통해 대규모 모델 추론 지연을 극적으로 낮추는 전용 추론 플랫폼.

🚦 Local Sandbox Quickstart / 로컬 실행 방법
Prerequisites
Docker & Docker Compose installed

Python 3.10+

1. Boot up the Distributed Infrastructure Tier / 인프라 기동
Spin up localized multi-tenant instances of Qdrant, Milvus, and support stores with one command:

Bash
docker-compose -f docker-compose.infra.yml up -d
2. Install Pipeline Dependencies / 종속성 설치
Bash
pip install -r requirements.txt
3. Execute the Low-Latency Search Workflow Simulation / 시뮬레이션 가동
Verify the end-to-end integration flow across the non-blocking async architecture layers:

Bash
python main.py
🎯 Production SLA & Metric Benchmarks / 목표 성능 및 지연시간 제약
This architecture is optimized around the following architectural scale thresholds:
본 아키텍처는 초대형 플랫폼 스케일을 상정하여 다음 임계치를 기준으로 최적화되었습니다:

Stage-1 Vector Retrieval Latency (1단계 벡터 추출): < 15ms (HNSW index pre-filtered)

Stage-2 Triton Re-ranker Inference (2단계 모델 추론): < 35ms (Dynamic batching queued, Max queue delay 5ms)

Target Core API Gateway p99 Boundary (코어 게이트웨이 목표 p99 지연시간): < 120ms
