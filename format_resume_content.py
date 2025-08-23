from typing import Dict, Any
from conductor.client.worker.worker_task import WorkerTask
from conductor.client.configuration.configuration import Configuration
from conductor.client.automator.task_handler import TaskHandler
import os

os.environ['CONDUCTOR_SERVER_URL'] = "https://developer.orkescloud.com/api"
os.environ['CONDUCTOR_AUTH_KEY'] = "fqote939e1d6-7f2c-11f0-b60b-c227118a1889"
os.environ['CONDUCTOR_AUTH_SECRET'] = "8HWd9tOvoZgHge5wBDN3W4iznV1VtiK0K7Tv07T2xavlAnKn"

@WorkerTask(task_definition_name="format_resume_content")
def format_resume_content_task(task_input) -> Dict[str, Any]:
    try:
        resume_json = task_input.input_data['resume_json']
        def json_to_plain_string(json_item):
            if isinstance(json_item, list):
                return ", ".join(json_to_plain_string(item) for item in json_item)
            elif isinstance(json_item, dict):
                parts = []
                for key, value in json_item.items():
                    parts.append(f"{key}: {json_to_plain_string(value)}")
                return " | ".join(parts)
            else:
                return str(json_item)
        
        skills_string = json_to_plain_string(resume_json.get('skills', []))
        experience_string = json_to_plain_string(resume_json.get('experience', []))
        projects_string = json_to_plain_string(resume_json.get('projects', []))
        
        formatted_content = f"Skills: {skills_string}\nExperience: {experience_string}\nProjects: {projects_string}"
        
        return {
            "formatted_content": formatted_content,
            "status": "success"
        }
    except Exception as e:
        return {
            "formatted_content": "",
            "status": "error",
            "error_message": str(e)
        }
        
api_config = Configuration()
task_handler = TaskHandler(configuration=api_config)
task_handler.start_processes()