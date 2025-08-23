from typing import Dict, Any
from sentence_transformers import SentenceTransformer
from conductor.client.worker.worker_task import WorkerTask
from conductor.client.configuration.configuration import Configuration
from conductor.client.automator.task_handler import TaskHandler
import os
import numpy as np

embedding_model1 = None

os.environ['CONDUCTOR_SERVER_URL'] = "https://developer.orkescloud.com/api"
os.environ['CONDUCTOR_AUTH_KEY'] = "fqote939e1d6-7f2c-11f0-b60b-c227118a1889"
os.environ['CONDUCTOR_AUTH_SECRET'] = "8HWd9tOvoZgHge5wBDN3W4iznV1VtiK0K7Tv07T2xavlAnKn"

def get_embedding_model(model_name: str = 'all-MiniLM-L6-v2'):
    global embedding_model1
    if embedding_model1 is None:
        embedding_model1 = SentenceTransformer(model_name)
    return embedding_model1

@WorkerTask(task_definition_name='generate_jd_embeddings')
def generate_jd_embeddings_task(task_input) -> Dict[str, Any]:
    try:
        print(task_input.input_data)
        text = task_input.input_data['text']
        model_name = task_input.input_data['model_name'] if 'model_name' in task_input.input_data else 'all-MiniLM-L6-v2'

        model = get_embedding_model(model_name)
        embeddings = model.encode(text, convert_to_tensor=True)
        
        # Convert tensor to list for JSON serialization
        embeddings_list = embeddings.cpu().numpy().tolist()
        return {
            "embeddings": embeddings_list,
            "status": "success",
            "dimension": len(embeddings_list)
        }
    except Exception as e:
        return {
            "embeddings": [],
            "status": "error",
            "error_message": str(e)
        }
        
api_config = Configuration()
task_handler = TaskHandler(configuration=api_config)
task_handler.start_processes()