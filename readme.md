#Coupang-Scale Low-Latency Multi-Stage Product Discovery Agent
#대규모 트래픽 대응 초저지연 멀티 스테이지 상품 검색 에이전트

##🇺🇸 Two-stage semantic search framework utilizing distributed processing (Spark/Ray), partitioned vector DBs (Qdrant/Milvus), and Triton Inference Server to hit a <250ms p99 SLA on petabyte catalogs.
##🇰🇷 대용량 데이터 처리(Spark/Ray)와 분산 벡터 DB(Qdrant/Milvus)를 Triton 및 AsyncIO로 엮어 p99 <250ms 초저지연을 달성한 엔터프라이즈급 2단계 검색 아키텍처.

###🏗️ System Architecture / 시스템 구조
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
```

## 🚀 Core Architecture (STAR Summary)

---

### 🇺🇸 English Version

#### **Situation**
Handling complex, large-scale search queries across hundreds of millions of product SKUs while simultaneously applying real-time business constraints (e.g., Rocket Delivery eligibility, dynamic price ceilings) creates a critical engineering bottleneck. Relying on traditional keyword search or directly coupling user queries to Large Language Models (LLMs) induces prohibitive latency and unsustainable computational resource overhead in high-throughput environments.

#### **Task**
The objective was to architect a cloud-native, asynchronous, two-stage semantic search platform capable of executing high-performance hybrid filtering and high-precision relevance re-ranking. The entire system was designed from the ground up to rigorously enforce strict production-grade enterprise SLAs, securing a sub-250ms latency boundary at the $p99$ threshold.

#### **Action**
* **Infrastructure Abstraction (`infrastructure/`):** Established an interface-driven architecture adhering strictly to the **Dependency Inversion Principle**. Engineered pluggable, decoupled connector implementations for **Qdrant** (gRPC-driven) and **Milvus**, mitigating single-node memory lockouts and eliminating vertical scaling bottlenecks.
* **Two-Stage Retrieval Optimization (`core/`):**
  * **Stage 1 (High Recall):** Configured low-latency vector indexing leveraging native HNSW graphs optimized with relational metadata pre-filtering to rapidly compress the downstream candidate search space.
  * **Stage 2 (High Precision):** Orchestrated asynchronous network routing to offload coarse candidate arrays to a decoupled **Triton Inference Server**, executing dense, token-level cross-attention matrices via a PyTorch Cross-Encoder framework for high-fidelity relevance scoring.
* **Asynchronous Serving Gateway (`main.py`):** Built a high-concurrency, non-blocking execution layer utilizing **Python AsyncIO** for network I/O operations. Implemented defensive circuit breakers coupled with a strict 50ms fast-timeout fallback strategy to guarantee high system availability and block cascading downstream degradation.
* **Data & MLOps Pipeline Integration:** Formulated a clean separation of concerns by designing robust big-data batch architectures with **Apache Spark** and **Ray** for multi-GPU distributed embedding generation, unified via **Apache Airflow** and **Kubeflow** for predictable, continuous synchronization of the product lifecycle state.

#### **Result**
Successfully demonstrated a highly scalable, production-ready system blueprint that solves the concurrency locks and memory constraints inherent in massive e-commerce architectures. The isolated computing layer ensures seamless horizontal scale-out capabilities to absorb sudden seasonal traffic spikes (e.g., flash sales or promotional events), establishing an actionable enterprise design pattern aligned with Staff Machine Learning Engineer competencies.

---

### 🇰🇷 한국어 버전 (Korean Version)

#### **Situation (상황)**
수억 개의 상품 SKU 전체에 걸쳐 대규모 시맨틱 쿼리를 처리하는 동시에, 실시간 비즈니스 제약 조건(예: 로켓배송 조건, 동적 가격 상한선)을 동시 적용하는 것은 심각한 엔지니어링 병목을 유발합니다. 기존의 키워드 검색에만 의존하거나 사용자 쿼리를 거대 언어 모델(LLM)에 직접 결합하는 방식은 고처리량(High-Throughput) 환경에서 수용 불가능한 지연 시간과 지속 불가능한 컴퓨팅 자원 오버헤드를 발생시킵니다.

#### **Task (과제)**
고성능 하이브리드 필터링과 고정밀 연관성 재정렬(Re-ranking)을 수행할 수 있는 클라우드 네이티브 기반의 비동기 2단계(Two-Stage) 시맨틱 검색 플랫폼을 설계하는 것이었습니다. 전체 시스템은 엄격한 프로덕션급 엔터프라이즈 SLA를 준수하고, $p99$ 기준 250ms 이하의 초저지연 경계를 확보하도록 설계되었습니다.

#### **Action (수행 내용)**
* **인프라 추상화 (`infrastructure/`):** **의존성 역전 원칙(Dependency Inversion Principle)**을 엄격히 준수하는 인터페이스 지향 아키텍처를 확립했습니다. **Qdrant**(gRPC 기반) 및 **Milvus**에 대한 플러그형 커넥터 구현체를 설계하여, 단일 노드의 메모리 락아웃을 완화하고 수직 확장(Vertical Scaling)의 한계를 제거했습니다.
* **2단계 검색 구조 최적화 (`core/`):**
  * **1단계 (하이 리콜):** 관계형 메타데이터 사전 필터링(Pre-filtering)에 최적화된 고유 HNSW 그래프를 활용하여 저지연 벡터 인덱싱을 구성하고, 후속 후보 검색 공간을 고속 압축했습니다.
  * **2단계 (고정밀):** 추출된 후보군 배열을 분리된 **Triton Inference Server**로 이관하는 비동기 네트워크 라우팅을 오케스트레이션하고, PyTorch Cross-Encoder 프레임워크를 통해 토큰 레벨의 교차 주의 집중(Cross-Attention) 매트릭스를 연산하여 고정밀 연관성 점수를 산출했습니다.
* **비동기 서빙 게이트웨이 (`main.py`):** 네트워크 I/O 작업을 위해 **Python AsyncIO**를 활용하는 고동시성 비동기 실행 레이어를 구축했습니다. 다운스트림의 연쇄적인 장애 전파를 차단하고 높은 시스템 가용성을 보장하기 위해, 엄격한 50ms 패스트 타임아웃(Fast-Timeout) 폴백 전략과 결합된 방어적 서킷 브레이커(Circuit Breaker)를 구현했습니다.
* **데이터 및 MLOps 파이프라인 연동:** 대용량 데이터 배치 처리를 위한 **Apache Spark**와 멀티 GPU 분산 임베딩 생성을 위한 **Ray** 아키처를 설계하여 관심사를 명확히 분리했으며, **Apache Airflow** 및 **Kubeflow**를 통해 상품 라이프사이클 상태의 주기적이고 예측 가능한 동기화를 구현했습니다.

#### **Result (결과)**
대규모 이커머스 아키텍처 고유의 동시성 락(Concurrency Lock) 및 메모리 제약 문제를 해결하는 확장 가능한 프로덕션급 시스템 청사진을 입증했습니다. 격리된 컴퓨팅 레이어는 대규모 타임 세일이나 프로모션 이벤트와 같은 급격한 시즌별 트래픽 급증을 흡수할 수 있는 유연한 수평 확장(Scale-out) 역량을 보장하며, Staff Machine Learning Engineer 역량에 부합하는 실전 엔터프라이즈 디자인 패턴을 제시했습니다.

📂 Project Structure / 디렉토리 구조
├── 📂 orchestration/     # Airflow ETL Pipeline DAG
├── 📂 pipelines/         # Spark Batch Processing & Ray Distributed Embedding
├── 📂 core/              # AsyncIO Gateway & Triton High-Throughput Client
├── 📂 infrastructure/    # DB Abstraction Interface (Qdrant & Milvus Impl)
└── 📂 deployment/        # Kubernetes / Kubeflow Manifests & Triton Models config.pbtxt

🛠️ Technology Stack & Purpose / 핵심 기술 및 활용 목적CategoryComponentPurpose / 활용 목적ML & SearchPyTorch, Cross-Encoder고정밀 문맥 매칭 및 검색 결과 실시간 재정렬 연산Vector EngineQdrant, Milvus컴퓨트-스토리지 분리 구조 기반 대규모 벡터 인덱싱Data PlatformApache Spark, Ray페타바이트급 데이터 정제(ETL) 및 분산 GPU 임베딩 생성Serving/MLOpsTriton, Kubernetes, AirflowDynamic Batching을 활용한 모델 추론 가속 및 배포 자동화

🎯 Production SLA / 성능 목표1단계 벡터 검색 지연 시간: < 15ms (HNSW index pre-filtered)2단계 Triton 모델 추론 지연 시간: < 35ms (Dynamic batching queued)코어 게이트웨이 최종 p99 목표 지연 시간: < 120ms

