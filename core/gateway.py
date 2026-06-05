# core/gateway.py
import asyncio
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from core.triton_client import HighThroughputTritonClient
from infrastructure.qdrant_impl import QdrantVectorStore # 필요에 따라 MilvusVectorStore로 교체 가능

app = FastAPI(title="Coupang-Scale Low-Latency Product Discovery Gateway")

class SearchResponse(BaseModel):
    status: str
    query: str
    latency_ms: float
    results: list[int]

class HighThroughputAgentGateway:
    def __init__(self, triton_url: str = "localhost:8001"):
        self.triton_client = HighThroughputTritonClient(triton_url=triton_url)
        # 프로덕션 스토어 의존성 주입
        self.vector_store = QdrantVectorStore(host="localhost", port=6333)

    async def pipeline_search_execution(self, query: str, user_id: str, top_k: int = 5) -> list[int]:
        # Stage-1: 고속 시맨틱 벡터 검색 수행 (Targeting Candidate 수집)
        # 하드웨어 슬롯 부하를 방지하기 위해 1차 후보군은 50개로 제한
        candidate_ids = await self.vector_store.stage_one_search(query, limit=50)
        
        if not candidate_ids:
            return []

        # Stage-2: Triton Inference Server 기반 고정밀 Cross-Encoder 모델 랭킹 스코어링
        triton_scores = await self.triton_client.compute_reranking_scores(query, candidate_ids)

        # Stage-3: [진짜 프로덕션의 핵심] 딥러닝 문맥 점수 + 쿠팡 광고(Ad) 스코어 가중치 결합 알고리즘
        final_ranked_items = []
        for idx, cid in enumerate(candidate_ids):
            base_ml_score = triton_scores[idx] if idx < len(triton_scores) else 0.0
            
            # [비즈니스 로직 적용]: 가상의 실시간 비즈니스 데이터 마트 연동 모사
            # 실제 운영 환경이라면 Redis 등 카탈로그 캐시에서 데이터를 1ms 이내로 가져옴
            is_ad_sponsored = (cid % 7 == 0)  # 예시: 7의 배수 ID 상품은 광고 입찰 상품으로 정의
            ad_boost_multiplier = 0.25 if is_ad_sponsored else 0.0
            
            # 최종 정렬 스코어 산식 (Multi-objective Optimization)
            final_score = base_ml_score + ad_boost_multiplier
            final_ranked_items.append((cid, final_score))

        # 복합 비즈니스 점수 기준으로 전체 재정렬(Descending Rank)
        final_ranked_items.sort(key=lambda x: x[1], reverse=True)
        
        # 최종 비즈니스 탑 K만 필터링하여 유저에게 반환
        return [item[0] for item in final_ranked_items[:top_k]]

# 전역 게이트웨이 인스턴스화
gateway = HighThroughputAgentGateway()

@app.post("/v1/predict/search", response_model=SearchResponse)
async def real_time_search_serving(
    query: str = Query(..., description="User search keyword"),
    user_id: str = Query(..., description="Unique client user hash identifier")
):
    start_time = asyncio.get_event_loop().time()
    try:
        # 비동기 완전 격리 파이프라인 호출
        top_products = await gateway.pipeline_search_execution(query, user_id)
        end_time = asyncio.get_event_loop().time()
        
        return SearchResponse(
            status="success",
            query=query,
            latency_ms=round((end_time - start_time) * 1000, 2),
            results=top_products
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Search Runtime Exception: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    # 수천 커런시 환경 대응을 위한 단일 프로세서 비동기 이벤트 루프 가동
    uvicorn.run("gateway:app", host="0.0.0.0", port=8000, workers=4)
