# Coupang-Scale Low-Latency Multi-Stage Product Discovery Agent
### 쿠팡 대규모 트래픽 대응 초저지연 멀티 스테이지 상품 검색 에이전트

An enterprise-grade blueprint and proof-of-concept architecture for high-throughput, asymmetric multi-stage semantic product search. This repository models how to handle petabyte-scale e-commerce catalogs under strict transaction and latency boundaries (<250ms p99 SLA) using a decoupled data lifecycle and state-of-the-art MLOps tools.

본 리포지토리는 고처리량 및 비대칭형 멀티 스테이지 시맨틱 상품 검색을 구현한 엔터프라이즈급 아키텍처 청사진입니다. 분리된 데이터 라이프사이클과 최신 MLOps 도구를 활용하여, 엄격한 트랜잭션 및 지연 시간 제약 조건(<250ms p99 SLA) 하에서 페타바이트 규모의 이커머스 카탈로그를 처리하는 방법을 모델링합니다.

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