import pdfplumber
import json
import re
import torch
import numpy as np
from typing import Dict, Any, List
from google import genai
from google.genai import types
from sentence_transformers import SentenceTransformer, util
from conductor.client.worker.worker_task import WorkerTask
from conductor.client.worker.worker import Worker
from conductor.client.configuration.configuration import Configuration
from conductor.client.orkes_clients import OrkesClients
from conductor.client.automator.task_handler import TaskHandler
import os

os.environ['CONDUCTOR_SERVER_URL'] = "https://developer.orkescloud.com/api"
os.environ['CONDUCTOR_AUTH_KEY'] = "fqote939e1d6-7f2c-11f0-b60b-c227118a1889"
os.environ['CONDUCTOR_AUTH_SECRET'] = "8HWd9tOvoZgHge5wBDN3W4iznV1VtiK0K7Tv07T2xavlAnKn"

@WorkerTask(task_definition_name='calculate_similarity')
def calculate_similarity_task(task_input) -> Dict[str, Any]:
    try:
        jd_embeddings = task_input.input_data['jd_embeddings']
        resume_embeddings = task_input.input_data['resume_embeddings']
        
        # print(jd_embeddings)
        # print(resume_embeddings)

        # Convert lists back to tensors
        jd_tensor = torch.tensor(jd_embeddings)
        resume_tensor = torch.tensor(resume_embeddings)
        
        # Calculate cosine similarity
        cosine_score = util.cos_sim(jd_tensor, resume_tensor).item()
        percentage = (cosine_score + 1) * 50
        
        return {
            "cosine_score": cosine_score,
            "percentage": f"{percentage:.2f}%",
            "status": "success"
        }
    except Exception as e:
        return {
            "cosine_score": 0,
            "percentage": "0.00%",
            "status": "error",
            "error_message": str(e)
        }
        
api_config = Configuration()
task_handler = TaskHandler(configuration=api_config)
task_handler.start_processes()