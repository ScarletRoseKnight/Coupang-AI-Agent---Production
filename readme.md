# Coupang-Scale Low-Latency Multi-Stage Product Discovery Agent
### 쿠팡 대규모 트래픽 대응 초저지연 멀티 스테이지 상품 검색 에이전트

An enterprise-grade blueprint and proof-of-concept architecture for high-throughput, asymmetric multi-stage semantic product search. This repository models how to handle petabyte-scale e-commerce catalogs under strict transaction and latency boundaries (<250ms p99 SLA) using a decoupled data lifecycle and state-of-the-art MLOps tools.[cite: 1]

본 리포지토리는 고처리량 및 비대칭형 멀티 스테이지 시맨틱 상품 검색을 구현한 엔터프라이즈급 아키텍처 청사진입니다. 분리된 데이터 라이프사이클과 최신 MLOps 도구를 활용하여, 엄격한 트랜잭션 및 지연 시간 제약 조건(<250ms p99 SLA) 하에서 페타바이트 규모의 이커머스 카탈로그를 처리하는 방법을 모델링합니다.[cite: 1]

---

## 🏗️ System Topology & Dataflow / 시스템 구조도

```text
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
```[cite: 1]

---

## 🚀 Key Architectural Pillars / 핵심 아키텍처 요소

### 1. Data & Training Lifecycle (데이터 및 학습 라이프사이클)
* **Orchestration:** Managed via Apache Airflow scheduling to automate workloads from raw SQL/BigQuery data logs[cite: 1].
* **Data Processing:** Powered by an Apache Spark cluster for petabyte-scale ETL pipelines and clickstream aggregation[cite: 1].
* **Distributed ML:** Utilizes Ray for distributed PyTorch and Hugging Face Transformers embedding generation[cite: 1].
* **Experimentation:** Leverages MLflow for hyperparameter tracking and managing the central Model Registry[cite: 1].

### 2. Low-Latency Production Runtime (초저지연 프로덕션 런타임)
* **Core Agent Gateway:** Built with asynchronous Python (AsyncIO) and Hugging Face Tokenizers for ultra-fast, non-blocking request handling[cite: 1].
* **Model Serving:** Powered by Triton Inference Server executing optimized PyTorch and Cross-Encoder models[cite: 1].
* **Vector Indexing:** Uses a Qdrant or Milvus cluster to perform high-recall, pre-filtered Stage-1 vector search[cite: 1].

---

## 🔍 Search Architecture Pipeline / 검색 파이프라인 구조

The system employs a multi-stage approach to balance latency boundaries (<250ms p99 SLA) with high retrieval accuracy[cite: 1]:

1. **Stage-1: Pre-filtered Vector Search**
   * The Core Agent Gateway receives user input, processes tokens, and queries the Qdrant/Milvus cluster[cite: 1].
   * Filters and isolates candidate subsets rapidly from massive vector spaces[cite: 1].

2. **Stage-2: High-Precision Re-ranking**
   * Top candidates are passed to Triton Inference Server[cite: 1].
   * A heavy Cross-Encoder model runs intense sequence-pair interactions to output final optimized product rankings[cite: 1].

---

## 🛠️ Technology Stack / 기술 스택

* **Data Lifecycle:** Apache Spark, Apache Airflow, BigQuery, SQL[cite: 1]
* **Distributed AI / MLOps:** Ray, PyTorch, Hugging Face Transformers, MLflow, Kubeflow[cite: 1]
* **Runtime & Serving:** Python AsyncIO, Triton Inference Server[cite: 1]
* **Vector DB Ecosystem:** Qdrant, Milvus[cite: 1]
* **Infrastructure:** Kubernetes Cluster[cite: 1]

📂 Project Structure / 디렉토리 구조
├── 📂 orchestration/     # Airflow ETL Pipeline DAG
├── 📂 pipelines/         # Spark Batch Processing & Ray Distributed Embedding
├── 📂 core/              # AsyncIO Gateway & Triton High-Throughput Client
├── 📂 infrastructure/    # DB Abstraction Interface (Qdrant & Milvus Impl)
└── 📂 deployment/        # Kubernetes / Kubeflow Manifests & Triton Models config.pbtxt

🛠️ Technology Stack & Purpose / 핵심 기술 및 활용 목적CategoryComponentPurpose / 활용 목적ML & SearchPyTorch, Cross-Encoder고정밀 문맥 매칭 및 검색 결과 실시간 재정렬 연산Vector EngineQdrant, Milvus컴퓨트-스토리지 분리 구조 기반 대규모 벡터 인덱싱Data PlatformApache Spark, Ray페타바이트급 데이터 정제(ETL) 및 분산 GPU 임베딩 생성Serving/MLOpsTriton, Kubernetes, AirflowDynamic Batching을 활용한 모델 추론 가속 및 배포 자동화

🎯 Production SLA / 성능 목표1단계 벡터 검색 지연 시간: < 15ms (HNSW index pre-filtered)2단계 Triton 모델 추론 지연 시간: < 35ms (Dynamic batching queued)코어 게이트웨이 최종 p99 목표 지연 시간: < 120ms

