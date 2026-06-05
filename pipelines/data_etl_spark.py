# pipelines/data_etl_spark.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def run_petabyte_scale_catalog_etl():
    # 분산 셔플 오버헤드와 메모리 누수를 막기 위한 Spark 최적화 세션 빌드
    spark = SparkSession.builder \
        .appName("CoupangScale-Clickstream-Feature-Aggregator") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.driver.memory", "16g") \
        .config("spark.executor.memory", "16g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

    try:
        # 1. 데이터 레이크 파티션으로부터 원천 로그 적재
        raw_clickstream_df = spark.read.parquet("hdfs:///analytics/raw_logs/clickstream/*")
        raw_product_catalog_df = spark.read.parquet("hdfs:///analytics/raw_logs/catalog/*")

        # 2. [진짜 피처 엔지니어링] 상품 단위별 총 노출수, 클릭수, 실제 구매 건수 분산 Aggregation
        aggregated_features_df = raw_clickstream_df.groupBy("product_id") \
            .agg(
                F.count(F.when(F.col("event_type") == "impression", 1)).alias("total_impressions"),
                F.count(F.when(F.col("event_type") == "click", 1)).alias("total_clicks"),
                F.count(F.when(F.col("event_type") == "purchase", 1)).alias("total_purchases")
            )

        # 3. 0 나누기 오류(Division by Zero)를 방어하는 파생 변수 피처 생성 로직
        processed_features_df = aggregated_features_df \
            .withColumn(
                "ctr", 
                F.when(F.col("total_impressions") > 0, F.col("total_clicks") / F.col("total_impressions")).otherwise(0.0)
            ) \
            .withColumn(
                "conversion_rate", 
                F.when(F.col("total_clicks") > 0, F.col("total_purchases") / F.col("total_clicks")).otherwise(0.0)
            )

        # 4. 마스터 상품 카탈로그 테이블에 가공 피처 Left Join 결합 (데이터 스큐 현상 방지 fill 처리)
        final_production_catalog = raw_product_catalog_df.join(
            processed_features_df, 
            on="product_id", 
            how="left"
        ).na.fill({"ctr": 0.0, "conversion_rate": 0.0, "total_purchases": 0, "total_clicks": 0, "total_impressions": 0})

        # 5. Ray 분산 노드들이 병렬로 고속 패치해 갈 수 있도록 파티셔닝 영구 저장
        final_production_catalog.write \
            .mode("overwrite") \
            .partitionBy("category_group") \
            .parquet("hdfs:///analytics/production_features/catalog_gold_features/")
            
        print("Production Spark Feature Processing Pipeline Completed Successfully.")
            
    except Exception as e:
        print(f"Distributed Spark Job Failed: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    run_petabyte_scale_catalog_etl()
