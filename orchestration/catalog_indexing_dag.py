# orchestration/catalog_indexing_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

default_args = {
    "owner": "ml-platform-team",
    "depends_on_past": False,
    "start_date": datetime(2026, 6, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG("coupang_agent_indexing_pipeline", default_args=default_args, schedule_interval="@daily", catchup=False) as dag:
    """
    Enforces atomic scheduling execution parameters for data synchronization.
    """
    
    execute_spark_etl = KubernetesPodOperator(
        namespace="ml-pipelines",
        image="coupang-registry.internal/spark-etl-job:latest",
        cmds=["python", "/app/pipelines/data_etl_spark.py"],
        name="spark-catalog-etl-worker",
        task_id="run_spark_etl",
        is_delete_operator_pod=True
    )

    execute_distributed_embeddings = KubernetesPodOperator(
        namespace="ml-pipelines",
        image="coupang-registry.internal/ray-embedding-worker:latest",
        cmds=["python", "-c", "import ray; print('Distributed compute task activated')"],
        name="ray-vector-generation-worker",
        task_id="run_ray_embeddings",
        is_delete_operator_pod=True
    )

    execute_spark_etl >> execute_distributed_embeddings