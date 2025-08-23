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

        
@WorkerTask(task_definition_name="generate_resume_json")
def generate_resume_json_task(task_input) -> Dict[str, Any]:
    try:
        resume_text = task_input.input_data['resume_text']
        api_key = task_input.input_data['api_key']
        client = genai.Client(api_key=api_key)
        model = "gemini-2.0-flash-lite"
        
        contents = [
            types.Content(
                role="user",
                parts=[
                    types.Part.from_text(
                        text=(
                            "Analyze the following resume text and extract the key details. "
                            "Return the structured information strictly in JSON format with EXACTLY "
                            "these fields and no others:\n"
                            "- education\n"
                            "- skills\n"
                            "- experience\n"
                            "- projects\n\n"
                            "The response must follow this exact schema:\n"
                            "{\n"
                            "  \"education\": [...],\n"
                            "  \"skills\": [...],\n"
                            "  \"experience\": [...],\n"
                            "  \"projects\": [...]\n"
                            "}\n\n"
                            "(Note: Return only the JSON part nothing else). "
                            f"{resume_text}"
                        )
                    ),
                ],
            ),
        ]
        
        result_text = ""
        for chunk in client.models.generate_content_stream(
            model=model,
            contents=contents,
        ):
            result_text += chunk.text
        
        try:
            json_data = json.loads(result_text)
        except json.JSONDecodeError:
            m = re.search(r'(\{.*\})', result_text, re.DOTALL)
            if m:
                json_data = json.loads(m.group(1))
            else:
                raise ValueError("Could not parse JSON from model output")
        
        return {
            "json_data": json_data,
            "status": "success"
        }
    except Exception as e:
        return {
            "json_data": {},
            "status": "error",
            "error_message": str(e)
        }
        
api_config = Configuration()
task_handler = TaskHandler(configuration=api_config)
task_handler.start_processes()