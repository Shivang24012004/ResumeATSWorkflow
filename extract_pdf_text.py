import pdfplumber
import json
import re
import tempfile
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
import requests

os.environ['CONDUCTOR_SERVER_URL'] = "https://developer.orkescloud.com/api"
os.environ['CONDUCTOR_AUTH_KEY'] = "fqote939e1d6-7f2c-11f0-b60b-c227118a1889"
os.environ['CONDUCTOR_AUTH_SECRET'] = "8HWd9tOvoZgHge5wBDN3W4iznV1VtiK0K7Tv07T2xavlAnKn"

@WorkerTask(task_definition_name="extract_pdf_text")
def extract_text_from_pdf(task_input):

    try:
        
        url = task_input.input_data['pdf_path']
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.pdf')
        temp_path = temp_file.name
        temp_file.close()  # Close to allow pdfplumber to open it
        
        # Download the file to a temporary location
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise exception for 4XX/5XX responses
        
        # Write content to temporary file
        with open(temp_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        # Extract text from PDF
        text = ""
        with pdfplumber.open(temp_path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
        
        # Clean up temporary file
        if os.path.exists(temp_path):
            os.unlink(temp_path)
            
        return text
    
    except requests.exceptions.RequestException as e:
        print(f"Error downloading PDF from URL: {str(e)}")
        return ""
    except Exception as e:
        print(f"Error extracting PDF: {str(e)}")
        # Clean up temporary file in case of error
        if 'temp_path' in locals() and os.path.exists(temp_path):
            os.unlink(temp_path)
        return ""
    
api_config = Configuration()
task_handler = TaskHandler(configuration=api_config)
task_handler.start_processes()