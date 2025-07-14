from typing import Dict, List, Any
from ollama_client import OllamaClient
from math import ceil
import json


class EnhancedSchemaEnricher:
    """Advanced AI-powered schema enricher with business context awareness."""

    def __init__(self, ollama_client: OllamaClient):
        self.client = ollama_client
        self.industry_contexts = {
            "Financial Services": {
                "common_patterns": ["account", "transaction", "balance", "credit", "debit"],
                "compliance_requirements": ["PCI DSS", "SOX", "GDPR", "PII Protection"],
                "key_metrics": ["transaction_volume", "account_balance", "risk_score"]
            },
            "Healthcare": {
                "common_patterns": ["patient", "diagnosis", "procedure", "provider", "claim"],
                "compliance_requirements": ["HIPAA", "HITECH", "FDA", "PHI Protection"],
                "key_metrics": ["patient_outcomes", "readmission_rate", "length_of_stay"]
            },
            "Retail/E-commerce": {
                "common_patterns": ["customer", "order", "product", "inventory", "sales"],
                "compliance_requirements": ["PCI DSS", "GDPR", "CCPA", "Consumer Protection"],
                "key_metrics": ["conversion_rate", "customer_lifetime_value", "inventory_turnover"]
            },
            "Online Travel Agency (OTA)": {
                "common_patterns": ["booking", "reservation", "accommodation", "property", "guest", "stay",
                                    "cancellation", "rate", "availability"],
                "compliance_requirements": ["GDPR", "PCI DSS", "Data Localization Laws", "Consumer Protection",
                                            "Tourism Regulations"],
                "key_metrics": ["booking_conversion", "cancellation_rate", "average_daily_rate", "occupancy_rate",
                                "customer_satisfaction", "net_promoter_score"]
            }
        }

    def enhance_schema(self, schema: List[Dict[str, Any]],
                       enhancement_options: List[str],
                       project_context: Dict[str, Any],
                       chunk_size: int = 8) -> List[Dict[str, Any]]:
        """Enhanced schema enrichment with business context."""

        industry = project_context.get('industry', 'General')
        options = [opt.lower().replace(" ", "_").replace("-", "_") for opt in enhancement_options]

        all_enhanced_columns = []
        total_chunks = ceil(len(schema) / chunk_size)

        for i in range(total_chunks):
            chunk = schema[i * chunk_size: (i + 1) * chunk_size]
            prompt = self._build_comprehensive_enhancement_prompt(
                chunk, options, project_context, industry
            )

            print(f"Processing chunk {i + 1}/{total_chunks} with {len(chunk)} columns...")

            result = self.client.generate_structured_response(prompt)

            if not result["success"]:
                raise Exception(f"AI enhancement failed for chunk {i + 1}: {result.get('error', 'Unknown error')}")

            if not result.get("is_json"):
                raise Exception(
                    f"AI returned invalid JSON for chunk {i + 1}: {result.get('json_error', 'Parse error')}")

            enhanced_columns = result["parsed_response"].get("enhanced_columns", [])

            if len(enhanced_columns) != len(chunk):
                raise Exception(
                    f"Chunk {i + 1}: Expected {len(chunk)} columns, but AI returned {len(enhanced_columns)}")

            # Process and merge the chunk
            enhanced_chunk = self._process_ai_response(
                {"enhanced_columns": enhanced_columns},
                chunk,
                industry
            )
            all_enhanced_columns.extend(enhanced_chunk)

        return all_enhanced_columns

    def _build_comprehensive_enhancement_prompt(self, schema: List[Dict[str, Any]],
                                                options: List[str],
                                                project_context: Dict[str, Any],
                                                industry: str) -> str:
        """Build industry-aware enhancement prompt."""

        prompt = f"""You are an expert data engineer and business analyst specializing in {industry} data systems. 
You are helping with a migration project from {project_context.get('source', 'Legacy System')} to {project_context.get('target', 'Modern Platform')}.

BUSINESS CONTEXT:
- Industry: {industry}
- Project: {project_context.get('name', 'Data Migration')}
- Source System: {project_context.get('source', 'Unknown')}
- Target Platform: {project_context.get('target', 'Unknown')}

CURRENT SCHEMA ANALYSIS:
"""

        # Add detailed schema information
        for i, col in enumerate(schema, 1):
            prompt += f"\n{i}. Column: '{col['column_name']}'"
            prompt += f"\n   - Data Type: {col['data_type']}"
            prompt += f"\n   - Completeness: {col['completeness_pct']}%"
            prompt += f"\n   - Unique Values: {col['unique_count']} out of {col['total_count']}"
            prompt += f"\n   - Sample Values: {col['sample_values'][:3]}"

            # Add enhanced metadata if available
            if col.get('potential_pii'):
                prompt += f"\n   - ⚠️ Potential PII Detected"
            if col.get('potential_business_key'):
                prompt += f"\n   - 🔑 Potential Business Key"
            if col.get('data_pattern'):
                prompt += f"\n   - Pattern: {col['data_pattern']}"

            # Add type-specific information
            if col['data_type'] in ['integer', 'float']:
                if 'min_value' in col and 'max_value' in col:
                    prompt += f"\n   - Range: {col['min_value']} to {col['max_value']}"
            elif col['data_type'] == 'string':
                if 'avg_length' in col:
                    prompt += f"\n   - Avg Length: {col['avg_length']} chars"

            prompt += "\n"

        # Add industry-specific context
        if industry in self.industry_contexts:
            context = self.industry_contexts[industry]
            prompt += f"\n{industry.upper()} INDUSTRY CONTEXT:"
            prompt += f"\n- Common Patterns: {', '.join(context['common_patterns'])}"
            prompt += f"\n- Compliance Requirements: {', '.join(context['compliance_requirements'])}"
            prompt += f"\n- Key Business Metrics: {', '.join(context['key_metrics'])}"

        # Add enhancement instructions
        prompt += "\n\nENHANCEMENT OBJECTIVES:\n"

        enhancement_descriptions = {
            'business_friendly_column_names': f"Create clear, business-friendly column names following {industry} conventions",
            'industry_specific_descriptions': f"Provide {industry}-specific business descriptions with domain expertise",
            'data_governance_&_compliance': f"Identify compliance requirements and governance needs for {industry}",
            'data_quality_rules': "Define comprehensive data quality validation rules",
            'transformation_suggestions': "Suggest practical data transformations for migration",
            'business_kpi_identification': f"Identify potential business KPIs and metrics relevant to {industry}"
        }

        for option in options:
            if option in enhancement_descriptions:
                prompt += f"• {enhancement_descriptions[option]}\n"

        # Add JSON format requirements with industry context
        prompt += f"""
RESPONSE FORMAT - CRITICAL INSTRUCTIONS:
You must return exactly {len(schema)} column objects in the "enhanced_columns" array.

Return ONLY this JSON structure with NO additional text, explanations, or formatting:

{{
  "enhanced_columns": [
    {{
      "original_name": "exact_original_column_name",
      "suggested_name": "modern_column_name",
      "business_description": "{industry} business description in 1-2 sentences",
      "industry_context": "How this field is used in {industry} operations",
      "compliance_notes": "Relevant compliance considerations",
      "data_quality_rules": ["rule1", "rule2"],
      "transformation_suggestions": ["suggestion1", "suggestion2"],
      "business_importance": "High/Medium/Low",
      "potential_kpis": ["kpi1", "kpi2"],
      "modernization_notes": "Migration improvement suggestions",
      "confidence_score": 0.85
    }}
  ]
}}

CRITICAL REQUIREMENTS:
- Return exactly {len(schema)} objects in enhanced_columns array
- Use only valid JSON syntax with double quotes
- No markdown formatting, no code blocks, no explanations
- Each object must have ALL the fields shown above
- Keep descriptions concise but meaningful for {industry} domain
- Focus on {project_context.get('source', 'legacy')} to {project_context.get('target', 'modern')} migration
- Response must be parseable JSON immediately without any cleaning"""

        return prompt

    def enhance_schema(self, schema: List[Dict[str, Any]],
                       enhancement_options: List[str],
                       project_context: Dict[str, Any],
                       chunk_size: int = 6) -> List[Dict[str, Any]]:  # Reduced chunk size for reliability
        """Enhanced schema enrichment with improved error handling."""

        industry = project_context.get('industry', 'General')
        options = [opt.lower().replace(" ", "_").replace("-", "_") for opt in enhancement_options]

        all_enhanced_columns = []
        total_chunks = ceil(len(schema) / chunk_size)

        for i in range(total_chunks):
            chunk = schema[i * chunk_size: (i + 1) * chunk_size]

            # Try multiple times with different approaches
            max_retries = 3
            success = False

            for retry in range(max_retries):
                try:
                    print(f"Processing chunk {i + 1}/{total_chunks} with {len(chunk)} columns (attempt {retry + 1})...")

                    if retry == 0:
                        # Standard prompt
                        prompt = self._build_comprehensive_enhancement_prompt(
                            chunk, options, project_context, industry
                        )
                    elif retry == 1:
                        # Simplified prompt for better JSON compliance
                        prompt = self._build_simplified_enhancement_prompt(
                            chunk, project_context, industry
                        )
                    else:
                        # Most basic prompt
                        prompt = self._build_basic_enhancement_prompt(
                            chunk, project_context, industry
                        )

                    result = self.client.generate_structured_response(prompt)

                    if not result["success"]:
                        print(f"AI API failed: {result.get('error', 'Unknown error')}")
                        continue

                    if not result.get("is_json"):
                        print(f"JSON parsing failed: {result.get('json_error', 'Parse error')}")
                        print(f"Raw response: {result.get('response', '')[:200]}...")
                        continue

                    enhanced_columns = result["parsed_response"].get("enhanced_columns", [])

                    if len(enhanced_columns) != len(chunk):
                        print(f"Column count mismatch: Expected {len(chunk)}, got {len(enhanced_columns)}")
                        continue

                    # Process and merge the chunk
                    enhanced_chunk = self._process_ai_response(
                        {"enhanced_columns": enhanced_columns},
                        chunk,
                        industry
                    )
                    all_enhanced_columns.extend(enhanced_chunk)
                    success = True
                    break

                except Exception as e:
                    print(f"Attempt {retry + 1} failed: {str(e)}")
                    if retry == max_retries - 1:
                        # Fallback: create basic enhancements manually
                        print("Using fallback enhancement...")
                        enhanced_chunk = self._create_fallback_enhancement(chunk, industry)
                        all_enhanced_columns.extend(enhanced_chunk)
                        success = True

            if not success:
                raise Exception(f"Failed to enhance chunk {i + 1} after {max_retries} attempts")

        return all_enhanced_columns

    def _build_simplified_enhancement_prompt(self, schema: List[Dict[str, Any]],
                                             project_context: Dict[str, Any],
                                             industry: str) -> str:
        """Build simplified prompt for better JSON reliability."""

        prompt = f"""You are a {industry} data expert. Enhance these {len(schema)} columns for migration.

COLUMNS TO ENHANCE:
"""

        for i, col in enumerate(schema, 1):
            prompt += f"{i}. {col['column_name']} ({col['data_type']}) - Sample: {col['sample_values'][:2]}\n"

        prompt += f"""
Return ONLY this JSON (no other text):

{{
  "enhanced_columns": ["""

        for i, col in enumerate(schema):
            prompt += f"""
    {{
      "original_name": "{col['column_name']}",
      "suggested_name": "suggest_better_name",
      "business_description": "{industry} business meaning",
      "industry_context": "{industry} usage context",
      "compliance_notes": "compliance requirements",
      "data_quality_rules": ["rule1"],
      "transformation_suggestions": ["suggestion1"],
      "business_importance": "High",
      "potential_kpis": ["kpi1"],
      "modernization_notes": "improvement notes",
      "confidence_score": 0.8
    }}{"," if i < len(schema) - 1 else ""}"""

        prompt += """
  ]
}"""

        return prompt

    def _build_basic_enhancement_prompt(self, schema: List[Dict[str, Any]],
                                        project_context: Dict[str, Any],
                                        industry: str) -> str:
        """Build most basic prompt as fallback."""

        prompt = f"""Enhance these {len(schema)} columns for {industry}. Return only JSON:

{{
  "enhanced_columns": ["""

        for i, col in enumerate(schema):
            suggested_name = col['column_name'].lower().replace('_', '_')
            if 'id' in col['column_name'].lower():
                suggested_name = col['column_name'].lower().replace('_id_nbr', '_id').replace('_nbr', '_id')
            elif 'amt' in col['column_name'].lower():
                suggested_name = col['column_name'].lower().replace('_amt', '_amount')
            elif 'dt' in col['column_name'].lower():
                suggested_name = col['column_name'].lower().replace('_dt', '_date')
            elif 'cd' in col['column_name'].lower():
                suggested_name = col['column_name'].lower().replace('_cd', '_code')

            prompt += f"""
    {{
      "original_name": "{col['column_name']}",
      "suggested_name": "{suggested_name}",
      "business_description": "{industry} data field",
      "industry_context": "Used in {industry} operations",
      "compliance_notes": "Standard compliance",
      "data_quality_rules": ["not_null"],
      "transformation_suggestions": ["standardize"],
      "business_importance": "Medium",
      "potential_kpis": ["data_quality"],
      "modernization_notes": "Modernize naming",
      "confidence_score": 0.7
    }}{"," if i < len(schema) - 1 else ""}"""

        prompt += """
  ]
}"""

        return prompt

    def _create_fallback_enhancement(self, schema: List[Dict[str, Any]], industry: str) -> List[Dict[str, Any]]:
        """Create basic enhancements as fallback when AI fails."""

        enhanced_columns = []

        for col in schema:
            # Create basic enhancement using rule-based logic
            original_name = col['column_name']

            # Basic name improvements
            suggested_name = original_name.lower()
            suggested_name = suggested_name.replace('_nbr', '_id')
            suggested_name = suggested_name.replace('_amt', '_amount')
            suggested_name = suggested_name.replace('_dt', '_date')
            suggested_name = suggested_name.replace('_cd', '_code')
            suggested_name = suggested_name.replace('_flg', '_flag')
            suggested_name = suggested_name.replace('_pct', '_percentage')

            # Basic business context
            if 'cust' in original_name.lower():
                business_desc = f"Customer-related {industry} data"
            elif 'order' in original_name.lower() or 'booking' in original_name.lower():
                business_desc = f"Order/booking information for {industry}"
            elif 'product' in original_name.lower() or 'item' in original_name.lower():
                business_desc = f"Product/service data for {industry}"
            elif 'amount' in original_name.lower() or 'amt' in original_name.lower():
                business_desc = f"Financial amount for {industry} transactions"
            elif 'date' in original_name.lower() or 'dt' in original_name.lower():
                business_desc = f"Date/time field for {industry} operations"
            else:
                business_desc = f"{industry} operational data field"

            enhanced_col = col.copy()
            enhanced_col.update({
                'suggested_name': suggested_name,
                'business_description': business_desc,
                'industry_context': f"Standard {industry} field usage",
                'compliance_notes': "Review for industry compliance requirements",
                'data_quality_rules': ["validate_completeness"],
                'transformation_suggestions': ["standardize_naming"],
                'business_importance': "Medium",
                'potential_kpis': [],
                'modernization_notes': "Consider modern naming conventions",
                'confidence_score': 0.6,
                'enhanced': True,
                'industry': industry,
                'fallback_used': True
            })

            # Add calculated insights
            enhanced_col['data_quality_score'] = self._calculate_data_quality_score(enhanced_col)
            enhanced_col['migration_complexity'] = self._assess_migration_complexity(enhanced_col)

            enhanced_columns.append(enhanced_col)

        return enhanced_columns

    def _process_ai_response(self, ai_response: Dict[str, Any],
                             original_schema: List[Dict[str, Any]],
                             industry: str) -> List[Dict[str, Any]]:
        """Process and validate AI response with industry context."""

        if "enhanced_columns" not in ai_response:
            raise Exception("AI response missing 'enhanced_columns' field")

        enhanced_columns = ai_response["enhanced_columns"]

        if len(enhanced_columns) != len(original_schema):
            raise Exception(f"AI returned {len(enhanced_columns)} columns, expected {len(original_schema)}")

        result = []

        for i, (original, enhanced) in enumerate(zip(original_schema, enhanced_columns)):
            merged_column = original.copy()

            # Add AI enhancements
            merged_column.update({
                'suggested_name': enhanced.get('suggested_name', original['column_name']),
                'business_description': enhanced.get('business_description', ''),
                'industry_context': enhanced.get('industry_context', ''),
                'compliance_notes': enhanced.get('compliance_notes', ''),
                'data_quality_rules': enhanced.get('data_quality_rules', []),
                'transformation_suggestions': enhanced.get('transformation_suggestions', []),
                'business_importance': enhanced.get('business_importance', 'Medium'),
                'potential_kpis': enhanced.get('potential_kpis', []),
                'modernization_notes': enhanced.get('modernization_notes', ''),
                'confidence_score': enhanced.get('confidence_score', 0.5),
                'enhanced': True,
                'industry': industry
            })

            # Add calculated insights
            merged_column['data_quality_score'] = self._calculate_data_quality_score(merged_column)
            merged_column['migration_complexity'] = self._assess_migration_complexity(merged_column)

            result.append(merged_column)

        return result

    def _calculate_data_quality_score(self, column: Dict[str, Any]) -> float:
        """Calculate a comprehensive data quality score."""
        score = 1.0

        # Completeness factor
        completeness = column.get('completeness_pct', 0) / 100
        score *= (0.3 + 0.7 * completeness)  # 30% base, 70% based on completeness

        # Uniqueness factor (for potential keys)
        if column.get('potential_business_key'):
            uniqueness = column.get('unique_count', 0) / max(column.get('total_count', 1), 1)
            score *= (0.5 + 0.5 * uniqueness)

        # PII handling factor
        if column.get('potential_pii') and not column.get('compliance_notes'):
            score *= 0.7  # Reduce score if PII not properly addressed

        return round(score, 2)

    def _assess_migration_complexity(self, column: Dict[str, Any]) -> str:
        """Assess migration complexity for each column."""
        complexity_factors = 0

        # Name change required
        if column['column_name'] != column.get('suggested_name', column['column_name']):
            complexity_factors += 1

        # Data type transformation needed
        if column.get('transformation_suggestions'):
            complexity_factors += len(column['transformation_suggestions'])

        # PII handling required
        if column.get('potential_pii'):
            complexity_factors += 2

        # Compliance requirements
        if column.get('compliance_notes'):
            complexity_factors += 1

        if complexity_factors == 0:
            return "Low"
        elif complexity_factors <= 2:
            return "Medium"
        else:
            return "High"

    def generate_migration_checklist(self, enhanced_schema: List[Dict[str, Any]]) -> List[str]:
        """Generate a migration checklist based on enhanced schema."""
        checklist = []

        # PII handling
        pii_columns = [col for col in enhanced_schema if col.get('potential_pii')]
        if pii_columns:
            checklist.append(f"🔒 Implement PII protection for {len(pii_columns)} columns")

        # High complexity migrations
        high_complexity = [col for col in enhanced_schema if col.get('migration_complexity') == 'High']
        if high_complexity:
            checklist.append(f"⚠️ Plan detailed migration strategy for {len(high_complexity)} complex columns")

        # Data quality rules
        columns_with_rules = [col for col in enhanced_schema if col.get('data_quality_rules')]
        if columns_with_rules:
            checklist.append(f"✅ Implement data quality rules for {len(columns_with_rules)} columns")

        # Business KPIs
        kpi_columns = [col for col in enhanced_schema if col.get('potential_kpis')]
        if kpi_columns:
            checklist.append(f"📊 Set up KPI tracking for {len(kpi_columns)} business metrics")

        return checklist

    def validate_naming_conventions(self, enhanced_schema: List[Dict[str, Any]],
                                    target_platform: str) -> Dict[str, Any]:
        """Validate naming conventions for target platform."""
        validation_result = {
            'valid': True,
            'warnings': [],
            'errors': [],
            'suggestions': []
        }

        platform_rules = {
            'Snowflake': {
                'max_length': 255,
                'case_sensitive': False,
                'reserved_words': ['SELECT', 'FROM', 'WHERE', 'GROUP', 'ORDER']
            },
            'BigQuery': {
                'max_length': 300,
                'case_sensitive': True,
                'reserved_words': ['SELECT', 'FROM', 'WHERE', 'GROUP', 'ORDER']
            }
        }

        rules = platform_rules.get(target_platform, platform_rules['Snowflake'])
        suggested_names = [col.get('suggested_name', col['column_name']) for col in enhanced_schema]

        # Check for duplicates
        if len(suggested_names) != len(set([name.upper() for name in suggested_names])):
            validation_result['errors'].append("Duplicate column names detected (case-insensitive)")
            validation_result['valid'] = False

        # Platform-specific validation
        for col in enhanced_schema:
            name = col.get('suggested_name', col['column_name'])

            if len(name) > rules['max_length']:
                validation_result['warnings'].append(f"Column '{name}' exceeds {target_platform} length limit")

            if name.upper() in rules['reserved_words']:
                validation_result['errors'].append(f"Column '{name}' is a {target_platform} reserved word")
                validation_result['valid'] = False

        return validation_result