Coupang-Scale Low-Latency Multi-Stage Product Discovery Agent대규모 트래픽 대응 초저지연 멀티 스테이지 상품 검색 에이전트Resume Headline🇺🇸 Two-stage semantic search framework utilizing distributed processing (Spark/Ray), partitioned vector DBs (Qdrant/Milvus), and Triton Inference Server to hit a <250ms p99 SLA on petabyte catalogs.🇰🇷 대용량 데이터 처리(Spark/Ray)와 분산 벡터 DB(Qdrant/Milvus)를 Triton 및 AsyncIO로 엮어 p99 <250ms 초저지연을 달성한 엔터프라이즈급 2단계 검색 아키텍처.🏗️ System Architecture / 시스템 구조Plaintext [ Catalogs / SQL / BigQuery ]
               │
               ▼ (Apache Airflow ETL)
 ┌────────────────────────────────────────────────────────┐
 │ 1. DATA LIFECYCLE (Spark ETL & Ray Distributed Embed)  │
 └────────────────────────────────────────────────────────┘
               │
        ┌──────┴──────┐
        ▼             ▼
 [ Vector Upsert ] [ Model Weights ] (via Kubeflow)
        │             │
        ▼             ▼
 ┌────────────────────────────────────────────────────────┐
 │ 2. PRODUCTION RUNTIME (Kubernetes)                     │
 │   ┌──────────────┐     ┌──────────────┐                │
 │   │ Triton Server│ ──► │ Async Gateway│ ◄──► [User]    │
 │   └──────────────┘     └──────────────┘                │
 │          ▲                    ▲                        │
 │          └─────► [Qdrant] ────┘                        │
 └────────────────────────────────────────────────────────┘
🚀 Core Architecture (STAR Summary)Situation (문제 배경): 수억 개의 상품 정보와 실시간 비즈니스 필터(예: 로켓배송 여부, 가격 제한)가 복합된 대규모 검색 요청 시, 기존 키워드 검색이나 거대 모델(LLM) 직결 방식은 심각한 지연 시간(Latency)과 과도한 리소스를 유발함.Task (목표): 대용량 인프라 환경에서 실시간 하이브리드 필터링과 고정밀 재정렬을 수행하는 비동기 2단계(Two-Stage) 검색 엔진 클라우드 설계를 통해 엄격한 기업용 서비스 기준(<250ms p99 SLA)을 충족하는 것.Action (해결 방법):인프라 추상화 (infrastructure/): 의존성 역전 원칙에 따라 인터페이스 지향 아키텍처를 수립, Qdrant(gRPC 기반)와 Milvus 플러그인을 결합하여 단일 노드 메모리 병목 극복.2단계 최적화 (core/): 1단계(High Recall)에서 HNSW 인덱스 기반 관계형 속성 사전 필터링을 거쳐 검색 후보군을 압축한 뒤, 2단계(High Precision)에서 Triton Inference Server와 비동기 통신을 수행하여 PyTorch Cross-Encoder 모델로 고정밀 재정렬 점수를 계산.비동기 게이트웨이 (main.py): Python AsyncIO 기반의 비동기 네트워크 입출력 및 서킷 브레이커 방어 전략(50ms 타임아웃)을 설계하여 장애 전파를 차단.파이프라인 연동: 대용량 분산 처리를 위한 Apache Spark/Ray와 지속적 배포를 위한 Airflow/Kubeflow 구조 설계.Result (성과 및 가치): 분산 서빙 레이어 격리를 통해 메모리 한계와 동시성 락(Lock) 문제를 해결하였으며, 트래픽 급증(Promotion Event)에도 수평 확장(Scale-out)이 용이한 Staff ML Engineer 수준의 엔터프라이즈 모범 사례 입증.📂 Project Structure / 디렉토리 구조Plaintext├── 📂 orchestration/     # Airflow ETL Pipeline DAG
├── 📂 pipelines/         # Spark Batch Processing & Ray Distributed Embedding
├── 📂 core/              # AsyncIO Gateway & Triton High-Throughput Client
├── 📂 infrastructure/    # DB Abstraction Interface (Qdrant & Milvus Impl)
└── 📂 deployment/        # Kubernetes / Kubeflow Manifests & Triton Models config.pbtxt
🛠️ Technology Stack & Purpose / 핵심 기술 및 활용 목적CategoryComponentPurpose / 활용 목적ML & SearchPyTorch, Cross-Encoder고정밀 문맥 매칭 및 검색 결과 실시간 재정렬 연산Vector EngineQdrant, Milvus컴퓨트-스토리지 분리 구조 기반 대규모 벡터 인덱싱Data PlatformApache Spark, Ray페타바이트급 데이터 정제(ETL) 및 분산 GPU 임베딩 생성Serving/MLOpsTriton, Kubernetes, AirflowDynamic Batching을 활용한 모델 추론 가속 및 배포 자동화🎯 Production SLA / 성능 목표1단계 벡터 검색 지연 시간: < 15ms (HNSW index pre-filtered)2단계 Triton 모델 추론 지연 시간: < 35ms (Dynamic batching queued)코어 게이트웨이 최종 p99 목표 지연 시간: < 120ms
