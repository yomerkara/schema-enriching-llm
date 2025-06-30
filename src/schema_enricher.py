from typing import Dict, List, Any
from ollama_client import OllamaClient
from math import ceil


class SchemaEnricher:
    """Handles AI-powered schema enhancement using Ollama."""

    def __init__(self, ollama_client: OllamaClient):
        self.client = ollama_client
        self.enhancement_templates = {
            "column_names": self._get_column_names_prompt,
            "business_descriptions": self._get_business_descriptions_prompt,
            "data_quality_assessment": self._get_data_quality_prompt,
            "transformation_suggestions": self._get_transformation_prompt,
        }

    def enhance_schema(self, schema: List[Dict[str, Any]],
                       enhancement_options: List[str],
                       chunk_size: int = 10) -> List[Dict[str, Any]]:
        """Enhance schema in chunks to avoid overly large completions."""

        options = [opt.lower().replace(" ", "_") for opt in enhancement_options]
        all_enhanced_columns = []

        total_chunks = ceil(len(schema) / chunk_size)

        for i in range(total_chunks):
            chunk = schema[i * chunk_size : (i + 1) * chunk_size]
            prompt = self._build_enhancement_prompt(chunk, options)

            print(f"Processing chunk {i + 1}/{total_chunks} with {len(chunk)} columns...")

            result = self.client.generate_structured_response(prompt)

            if not result["success"]:
                raise Exception(f"AI enhancement failed for chunk {i+1}: {result.get('error', 'Unknown error')}")

            if not result.get("is_json"):
                raise Exception(f"AI returned invalid JSON for chunk {i+1}: {result.get('json_error', 'Parse error')}")

            enhanced_columns = result["parsed_response"].get("enhanced_columns", [])

            if len(enhanced_columns) != len(chunk):
                raise Exception(f"Chunk {i+1}: Expected {len(chunk)} columns, but AI returned {len(enhanced_columns)}")

            # Merge the chunk response
            enhanced_chunk = self._process_ai_response(
                {"enhanced_columns": enhanced_columns},
                chunk
            )
            all_enhanced_columns.extend(enhanced_chunk)

        # Optional: run overall assessment once on full schema
        if "data_quality_assessment" in options:
            prompt = self._build_enhancement_prompt(schema, ["data_quality_assessment"])
            result = self.client.generate_structured_response(prompt)

            if result["success"] and result.get("is_json"):
                overall = result["parsed_response"].get("overall_assessment")
                if overall:
                    all_enhanced_columns.append({
                        "is_overall_assessment": True,
                        "data_quality_score": overall.get("data_quality_score", 0.5),
                        "main_concerns": overall.get("main_concerns", []),
                        "recommendations": overall.get("recommendations", [])
                    })

        return all_enhanced_columns

    def _build_enhancement_prompt(self, schema: List[Dict[str, Any]], options: List[str]) -> str:
        """Build comprehensive prompt for schema enhancement."""

        prompt = """You are an expert data engineer analyzing a CSV schema. Please enhance the schema based on the requested options.

ORIGINAL SCHEMA:
"""

        # Add schema information
        for i, col in enumerate(schema, 1):
            prompt += f"\n{i}. Column: '{col['column_name']}'"
            prompt += f"\n   - Data Type: {col['data_type']}"
            prompt += f"\n   - Completeness: {col['completeness_pct']}%"
            prompt += f"\n   - Unique Values: {col['unique_count']}"
            prompt += f"\n   - Sample Values: {col['sample_values'][:3]}"

            # Add type-specific information
            if col['data_type'] in ['integer', 'float']:
                if 'min_value' in col:
                    prompt += f"\n   - Range: {col.get('min_value')} to {col.get('max_value')}"
            elif col['data_type'] == 'string':
                if 'avg_length' in col:
                    prompt += f"\n   - Avg Length: {col.get('avg_length')} chars"

            prompt += "\n"

        # Add enhancement instructions
        prompt += "\nENHANCEMENT REQUESTS:\n"

        enhancement_descriptions = {
            'column_names': "Suggest better, more descriptive column names",
            'business_descriptions': "Provide business-friendly descriptions of what each column represents",
            'data_quality_assessment': "Assess data quality and identify potential issues",
            'transformation_suggestions': "Suggest common transformations or cleaning steps"
        }

        for option in options:
            if option in enhancement_descriptions:
                prompt += f"- {enhancement_descriptions[option]}\n"

        # Add JSON format requirements
        prompt += """
+ - You must return exactly the same number of items in "enhanced_columns" as provided in the ORIGINAL SCHEMA.
+ - If 10 columns are provided, return 10 entries under "enhanced_columns" — no more, no less.
IMPORTANT INSTRUCTIONS:
- Respond **ONLY** with a single valid JSON object.
- Do **NOT** include any explanations, comments, or extra text before or after the JSON.
- Do **NOT** wrap the JSON in markdown code blocks (```json ...```).
- Do **NOT** include any triple quotes or other decorations.
- The JSON must be parseable directly without any modifications.

Here is the expected JSON format exactly:
{
  "enhanced_columns": [
    {
      "original_name": "original_column_name",
      "suggested_name": "better_column_name",
      "business_description": "Clear business description",
      "data_quality_notes": "Quality assessment and concerns",
      "transformation_suggestions": ["suggestion1", "suggestion2"],
      "confidence_score": 0.85
    }
  ],
  "overall_assessment": {
    "data_quality_score": 0.75,
    "main_concerns": ["concern1", "concern2"],
    "recommendations": ["recommendation1", "recommendation2"]
  }
}

Rules:
- Provide suggestions for ALL columns in the schema
- Keep suggested names concise but descriptive (snake_case format)
- Business descriptions should be 1-2 sentences maximum
- Confidence score should be between 0.0 and 1.0
- Only include transformation suggestions if specifically requested
Please adhere strictly to these rules.
"""

        return prompt

    def _process_ai_response(self, ai_response: Dict[str, Any],
                             original_schema: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process and validate AI response."""

        if "enhanced_columns" not in ai_response:
            raise Exception("AI response missing 'enhanced_columns' field")

        enhanced_columns = ai_response["enhanced_columns"]

        if len(enhanced_columns) != len(original_schema):
            raise Exception(f"AI returned {len(enhanced_columns)} columns, expected {len(original_schema)}")

        # Merge original schema with AI enhancements
        result = []

        for i, (original, enhanced) in enumerate(zip(original_schema, enhanced_columns)):
            merged_column = original.copy()  # Start with original data

            # Add AI enhancements
            merged_column.update({
                'suggested_name': enhanced.get('suggested_name', original['column_name']),
                'business_description': enhanced.get('business_description', ''),
                'data_quality_notes': enhanced.get('data_quality_notes', ''),
                'transformation_suggestions': enhanced.get('transformation_suggestions', []),
                'confidence_score': enhanced.get('confidence_score', 0.5),
                'enhanced': True
            })

            result.append(merged_column)

        # Add overall assessment if available
        if "overall_assessment" in ai_response:
            result.append({
                'is_overall_assessment': True,
                'data_quality_score': ai_response["overall_assessment"].get('data_quality_score', 0.5),
                'main_concerns': ai_response["overall_assessment"].get('main_concerns', []),
                'recommendations': ai_response["overall_assessment"].get('recommendations', [])
            })

        return result

    def _get_column_names_prompt(self, schema: List[Dict[str, Any]]) -> str:
        """Generate prompt for column name suggestions."""
        return "Suggest better, more descriptive column names that follow naming conventions."

    def _get_business_descriptions_prompt(self, schema: List[Dict[str, Any]]) -> str:
        """Generate prompt for business descriptions."""
        return "Provide clear business descriptions explaining what each column represents."

    def _get_data_quality_prompt(self, schema: List[Dict[str, Any]]) -> str:
        """Generate prompt for data quality assessment."""
        return "Assess data quality issues like high null rates, inconsistent formats, or suspicious patterns."

    def _get_transformation_prompt(self, schema: List[Dict[str, Any]]) -> str:
        """Generate prompt for transformation suggestions."""
        return "Suggest common data transformations, cleaning steps, or standardization needs."

    def validate_column_names(self, suggested_names: List[str]) -> Dict[str, Any]:
        """Validate suggested column names for conflicts and conventions."""
        validation_result = {
            'valid': True,
            'warnings': [],
            'errors': []
        }

        # Check for duplicates
        if len(suggested_names) != len(set(suggested_names)):
            validation_result['errors'].append("Duplicate column names detected")
            validation_result['valid'] = False

        # Check naming conventions
        for name in suggested_names:
            if not name.replace('_', '').replace('-', '').isalnum():
                validation_result['warnings'].append(f"Column '{name}' contains special characters")

            if name.startswith('_') or name.endswith('_'):
                validation_result['warnings'].append(f"Column '{name}' has leading/trailing underscores")

            if len(name) > 50:
                validation_result['warnings'].append(f"Column '{name}' is very long ({len(name)} chars)")

        return validation_result

    def generate_dbt_schema_yml(self, enhanced_schema: List[Dict[str, Any]],
                                table_name: str) -> str:
        """Generate dbt schema.yml content from enhanced schema."""

        yml_content = f"""version: 2

models:
  - name: {table_name}
    description: "Auto-generated model from CSV schema enrichment"
    columns:
"""

        for col in enhanced_schema:
            if col.get('is_overall_assessment'):
                continue

            yml_content += f"      - name: {col.get('suggested_name', col['column_name'])}\n"
            yml_content += f"        description: \"{col.get('business_description', 'No description available')}\"\n"
            yml_content += f"        data_type: {col['data_type']}\n"

            # Add tests based on data analysis
            tests = []
            if col['null_count'] == 0:
                tests.append("not_null")
            if col['unique_count'] == col['total_count']:
                tests.append("unique")

            if tests:
                yml_content += "        tests:\n"
                for test in tests:
                    yml_content += f"          - {test}\n"

            yml_content += "\n"

        return yml_content

    def generate_sql_model(self, enhanced_schema: List[Dict[str, Any]],
                           source_table: str, target_table: str) -> str:
        """Generate dbt SQL model from enhanced schema."""

        sql_content = f"""-- Auto-generated dbt model from CSV schema enrichment
-- Source: {source_table}

SELECT
"""

        column_lines = []
        for col in enhanced_schema:
            if col.get('is_overall_assessment'):
                continue

            original_name = col['column_name']
            suggested_name = col.get('suggested_name', original_name)

            if original_name != suggested_name:
                column_lines.append(f"    {original_name} AS {suggested_name}")
            else:
                column_lines.append(f"    {original_name}")

        sql_content += ",\n".join(column_lines)
        sql_content += f"\nFROM {{{{ source('raw', '{source_table}') }}}}"

        return sql_content
