# pipelines/distributed_embed_ray.py
import ray
import torch
import mlflow
from transformers import AutoTokenizer, AutoModel
from typing import Dict, List, Any

# Connect to a multi-node distributed Ray Cluster
ray.init(ignore_reinit_error=True)

@ray.remote(num_gpus=1)
class DistributedEmbeddingWorker:
    """
    Scales deep learning embedding workflows linearly across hundreds of compute nodes.
    Uses native PyTorch model execution tracked inside MLflow run boundaries.
    """
    def __init__(self, model_name: str = "hf-internal-mirror/text-embedding-3-small"):
        mlflow.set_tracking_uri("http://mlflow-tracking-server.coupang.net:5000")
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModel.from_pretrained(model_name).to(self.device)
        self.model.eval()

    def generate_batch_embeddings(self, texts: List[str]) -> List[List[float]]:
        inputs = self.tokenizer(texts, padding=True, truncation=True, return_tensors="pt").to(self.device)
        with torch.no_grad():
            with mlflow.start_run(nested=True):
                outputs = self.model(**inputs)
                # Max pooling transformation logic
                embeddings = outputs.last_hidden_state.mean(dim=1).cpu().numpy().tolist()
                mlflow.log_metric("inference_batch_size", len(texts))
        return embeddings