import requests
import json
from typing import Dict, List,  Any
import json5
import re

class OllamaClient:
    """Client for interacting with Ollama API."""

    def __init__(self, base_url: str = "http://localhost:11434"):
        self.base_url = base_url
        self.timeout = 120
        self.default_model = "gemma:latest"
#        self.default_model = "mistral"
#        self.default_model = "mixtral:8x7b"

    def check_connection(self) -> bool:
        """Check if Ollama is running and accessible."""
        try:
            response = requests.get(f"{self.base_url}/api/tags", timeout=5)
            return response.status_code == 200
        except Exception:
            return False

    def get_available_models(self) -> List[str]:
        """Get list of available models."""
        try:
            response = requests.get(f"{self.base_url}/api/tags", timeout=5)
            if response.status_code == 200:
                data = response.json()
                models = [model['name'] for model in data.get('models', [])]
                return models
        except Exception:
            pass
        return []

    def generate_response(self, prompt: str, model: str = None) -> Dict[str, Any]:
        """Generate response from Ollama model."""
        model = model or self.default_model

        payload = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 0.1,  # Lower temperature for more consistent outputs
                "top_p": 0.9,
                "num_predict": 1000,  # Limit response length
            }
        }

        try:
            response = requests.post(
                f"{self.base_url}/api/generate",
                json=payload,
                timeout=self.timeout
            )

            if response.status_code == 200:
                return {
                    "success": True,
                    "response": response.json().get("response", ""),
                    "model": model,
                    "prompt_eval_count": response.json().get("prompt_eval_count", 0),
                    "eval_count": response.json().get("eval_count", 0),
                }
            else:
                return {
                    "success": False,
                    "error": f"HTTP {response.status_code}: {response.text}",
                    "model": model
                }

        except requests.Timeout:
            return {
                "success": False,
                "error": "Request timed out. The model might be taking too long to respond.",
                "model": model
            }
        except Exception as e:
            return {
                "success": False,
                "error": f"Connection error: {str(e)}",
                "model": model
            }

    def generate_structured_response(self, prompt: str, model: str = None) -> Dict[str, Any]:
        """Generate structured JSON response from Ollama model."""

        structured_prompt = f"""{prompt}

IMPORTANT: 
- Respond ONLY with a valid JSON object.
- DO NOT include explanations, comments, markdown (```), or triple quotes.
- DO NOT OMIT any columns or use ellipsis (...) or phrases like "omitted for brevity".
- Include ALL requested fields for ALL columns.
- Use ONLY double quotes (") for JSON keys and string values.
- The response must be directly parseable as JSON.
"""

        result = self.generate_response(structured_prompt, model)

        if result["success"]:
            try:
                response_text = result["response"].strip()

                # Remove triple quotes anywhere
                response_text = response_text.replace('"""', '').replace("'''", '')

                # Remove markdown code fences if present
                if "```json" in response_text:
                    response_text = response_text.split("```json")[1].split("```")[0]
                elif "```" in response_text:
                    response_text = response_text.split("```")[1].split("```")[0]

                response_text = response_text.strip()

                # Further clean with helper
                response_text = self._clean_json_response(response_text)

                # Parse with json5 for tolerance
                parsed_json = json5.loads(response_text)

                result["parsed_response"] = parsed_json
                result["is_json"] = True

            except Exception as e:
                result["json_error"] = str(e)
                result["is_json"] = False
                result["raw_response"] = result["response"]
                print("Raw response:", repr(result["raw_response"]))
                print("Cleaned response:", repr(response_text))

        return result

    def _clean_json_response(self, response: str) -> str:
        """Clean up JSON response to make it safely parsable."""
        response = response.strip()

        # Remove code fences again just in case
        response = re.sub(r'```(?:json)?', '', response)
        response = response.strip()

        # Remove all triple quotes again
        response = response.replace('"""', '').replace("'''", '')

        # Extract JSON between first { and last }
        start = response.find('{')
        end = response.rfind('}')
        if start != -1 and end != -1:
            response = response[start:end + 1]

        return response
    def test_model_response(self, model: str = None) -> Dict[str, Any]:
        """Test model with a simple prompt."""
        test_prompt = "Hello! Please respond with a simple greeting."
        return self.generate_response(test_prompt, model)

    def get_model_info(self, model: str = None) -> Dict[str, Any]:
        """Get information about a specific model."""
        model = model or self.default_model

        try:
            response = requests.post(
                f"{self.base_url}/api/show",
                json={"name": model},
                timeout=10
            )

            if response.status_code == 200:
                return {
                    "success": True,
                    "model_info": response.json()
                }
            else:
                return {
                    "success": False,
                    "error": f"Model '{model}' not found or not accessible"
                }

        except Exception as e:
            return {
                "success": False,
                "error": f"Error getting model info: {str(e)}"
            }

    def validate_model_availability(self, model: str) -> bool:
        """Check if a specific model is available."""
        available_models = self.get_available_models()
        return model in available_models

    def estimate_token_count(self, text: str) -> int:
        """Rough estimation of token count (4 characters ≈ 1 token)."""
        return len(text) // 4