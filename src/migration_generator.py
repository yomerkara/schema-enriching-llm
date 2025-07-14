from typing import Dict, List, Any
import pandas as pd
from datetime import datetime
import yaml


class MigrationGenerator:
    """Generates migration artifacts and documentation."""

    def __init__(self, project_context: Dict[str, Any]):
        self.project_context = project_context
        self.target_platform = project_context.get('target', 'Snowflake')
        self.source_system = project_context.get('source', 'Legacy System')
        self.industry = project_context.get('industry', 'General')

    def _safe_get_criticality(self, column: Dict[str, Any]) -> str:
        """Safely get business criticality with fallback."""
        criticality = column.get('business_criticality')
        return str(criticality) if criticality else 'Medium'

    def _safe_startswith(self, value: Any, prefix: str) -> bool:
        """Safely check if value starts with prefix."""
        return str(value or '').startswith(prefix)

    def generate_all_assets(self, enhanced_schema: List[Dict[str, Any]],
                            source_data: pd.DataFrame = None) -> Dict[str, str]:
        """Generate comprehensive migration assets."""

        assets = {}

        # Generate dbt model
        assets['dbt_model'] = self.generate_dbt_model(enhanced_schema)

        # Generate schema YAML
        assets['schema_yml'] = self.generate_schema_yml(enhanced_schema)

        # Generate data quality tests
        assets['quality_tests'] = self.generate_quality_tests(enhanced_schema)

        # Generate business documentation
        assets['documentation'] = self.generate_business_documentation(enhanced_schema)

        # Generate migration script
        assets['migration_script'] = self.generate_migration_script(enhanced_schema)

        # Generate data lineage
        assets['lineage_documentation'] = self.generate_lineage_docs(enhanced_schema)

        return assets

    def generate_dbt_model(self, enhanced_schema: List[Dict[str, Any]]) -> str:
        """Generate dbt SQL model with transformations."""

        table_name = self.project_context.get('name', 'migration_table').lower()
        source_table = f"raw_{table_name}"

        sql = f"""/*
 * dbt Model: {table_name}
 * Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
 * Source: {self.source_system}
 * Target: {self.target_platform}
 * Industry: {self.industry}
 */

{{{{ config(
    materialized='table',
    tags=['{self.industry.lower()}', 'migration', 'enhanced'],
    description='Enhanced {table_name} with business-friendly column names and transformations'
) }}}}

WITH source_data AS (
    SELECT *
    FROM {{{{ source('raw_data', '{source_table}') }}}}
),

enhanced_data AS (
    SELECT
"""

        # Generate column transformations
        column_lines = []
        for col in enhanced_schema:
            if col.get('is_overall_assessment'):
                continue

            original_name = col['column_name']
            suggested_name = col.get('suggested_name', original_name)
            transformations = col.get('transformation_suggestions', [])

            # Build transformation logic
            if transformations:
                transformation_logic = self._build_transformation_logic(
                    original_name, transformations, col['data_type']
                )
                column_lines.append(f"        {transformation_logic} AS {suggested_name}")
            else:
                if original_name != suggested_name:
                    column_lines.append(f"        {original_name} AS {suggested_name}")
                else:
                    column_lines.append(f"        {original_name}")

            # Add comments for business context
            if col.get('business_description'):
                column_lines[-1] += f"  -- {col['business_description'][:50]}..."

        sql += ",\n".join(column_lines)

        # Add data quality enhancements
        sql += f"""
    FROM source_data
    WHERE 1=1
        -- Add data quality filters
"""

        # Add quality filters based on analysis
        for col in enhanced_schema:
            if col.get('is_overall_assessment'):
                continue

            completeness = col.get('completeness_pct', 100)
            business_criticality = col.get('business_criticality', '') or ''
            if completeness < 95 and business_criticality.startswith('High'):
                sql += f"        AND {col['column_name']} IS NOT NULL  -- Critical field validation\n"

        sql += """
),

final AS (
    SELECT *,
        CURRENT_TIMESTAMP() AS dbt_created_at,
        'dbt_migration' AS data_source_system
    FROM enhanced_data
)

SELECT * FROM final"""

        return sql

    def generate_schema_yml(self, enhanced_schema: List[Dict[str, Any]]) -> str:
        """Generate comprehensive dbt schema.yml."""

        table_name = self.project_context.get('name', 'migration_table').lower()

        schema_dict = {
            'version': 2,
            'models': [{
                'name': table_name,
                'description': f'Enhanced {self.industry} data migrated from {self.source_system}',
                'meta': {
                    'industry': self.industry,
                    'source_system': self.source_system,
                    'migration_date': datetime.now().isoformat(),
                    'data_steward': f'{self.industry} Data Team'
                },
                'columns': []
            }]
        }

        # Add column definitions
        for col in enhanced_schema:
            if col.get('is_overall_assessment'):
                continue

            # Safely extract values and convert to native Python types
            suggested_name = col.get('suggested_name', col['column_name'])
            description = col.get('business_description', 'No description available')
            data_type = col['data_type']
            original_name = col['column_name']

            # Safe conversion of business criticality
            business_criticality = col.get('business_criticality', 'Medium')
            if business_criticality is None:
                business_criticality = 'Medium'

            # Safe conversion of data quality score
            data_quality_score = col.get('data_quality_score', 0.5)
            try:
                if hasattr(data_quality_score, 'item'):  # numpy scalar
                    data_quality_score = float(data_quality_score.item())
                else:
                    data_quality_score = float(data_quality_score)
            except (ValueError, TypeError):
                data_quality_score = 0.5

            column_def = {
                'name': suggested_name,
                'description': description,
                'data_type': data_type,
                'meta': {
                    'original_name': original_name,
                    'business_criticality': str(business_criticality),
                    'industry_context': col.get('industry_context', ''),
                    'data_quality_score': round(data_quality_score, 2)
                }
            }

            # Add tests based on analysis
            tests = []

            # Completeness tests
            completeness = col.get('completeness_pct', 0)
            if completeness > 95:
                tests.append('not_null')

            # Uniqueness tests
            if col.get('potential_business_key'):
                tests.append('unique')

            # Custom data quality tests
            quality_rules = col.get('data_quality_rules', [])
            for rule in quality_rules:
                if isinstance(rule, str):  # Ensure rule is a string
                    if 'email' in rule.lower():
                        tests.append({
                            'dbt_utils.expression_is_true': {
                                'expression': f"regexp_like({suggested_name}, '^[^@]+@[^@]+\\\\.[^@]+$')"
                            }
                        })
                    elif 'positive' in rule.lower():
                        tests.append({
                            'dbt_utils.expression_is_true': {
                                'expression': f"{suggested_name} >= 0"
                            }
                        })

            if tests:
                column_def['tests'] = tests

            # Add compliance tags
            if col.get('potential_pii'):
                column_def['tags'] = ['pii', 'sensitive']

            schema_dict['models'][0]['columns'].append(column_def)

        return yaml.dump(schema_dict, default_flow_style=False, sort_keys=False)

    def generate_quality_tests(self, enhanced_schema: List[Dict[str, Any]]) -> str:
        """Generate comprehensive data quality tests."""

        table_name = self.project_context.get('name', 'migration_table').lower()

        sql = f"""/*
 * Data Quality Tests for {table_name}
 * Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
 * Industry: {self.industry}
 */

-- ========================================
-- COMPLETENESS TESTS
-- ========================================

"""

        # Completeness tests
        critical_columns = [col for col in enhanced_schema
                            if self._safe_startswith(col.get('business_criticality'), 'High')]

        if critical_columns:
            sql += f"-- Critical columns completeness check\n"
            sql += f"SELECT 'Critical Columns Completeness' AS test_name,\n"
            sql += f"       COUNT(*) AS total_records,\n"

            for col in critical_columns:
                if col.get('is_overall_assessment'):
                    continue
                suggested_name = col.get('suggested_name', col['column_name'])
                sql += f"       SUM(CASE WHEN {suggested_name} IS NULL THEN 1 ELSE 0 END) AS {suggested_name}_nulls,\n"
                sql += f"       ROUND(100.0 * SUM(CASE WHEN {suggested_name} IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS {suggested_name}_completeness_pct,\n"

            sql = sql.rstrip(',\n') + f"\nFROM {{{{ ref('{table_name}') }}}}\n\n"

        # Business rule tests
        sql += """-- ========================================
-- BUSINESS RULE TESTS
-- ========================================

"""

        for col in enhanced_schema:
            if col.get('is_overall_assessment'):
                continue

            suggested_name = col.get('suggested_name', col['column_name'])
            business_rules = col.get('suggested_business_rules', [])

            if business_rules:
                sql += f"-- {suggested_name} business rules\n"

                for rule in business_rules:
                    if 'non-negative' in rule.lower() and col['data_type'] in ['integer', 'float']:
                        sql += f"""SELECT '{suggested_name}_non_negative_check' AS test_name,
       COUNT(*) AS total_records,
       SUM(CASE WHEN {suggested_name} < 0 THEN 1 ELSE 0 END) AS negative_values,
       CASE WHEN SUM(CASE WHEN {suggested_name} < 0 THEN 1 ELSE 0 END) = 0 
            THEN 'PASS' ELSE 'FAIL' END AS test_result
FROM {{{{ ref('{table_name}') }}}}
WHERE {suggested_name} IS NOT NULL;

"""
                    elif 'email' in rule.lower():
                        sql += f"""SELECT '{suggested_name}_email_format_check' AS test_name,
       COUNT(*) AS total_records,
       SUM(CASE WHEN NOT regexp_like({suggested_name}, '^[^@]+@[^@]+\\.[^@]+$') THEN 1 ELSE 0 END) AS invalid_emails,
       CASE WHEN SUM(CASE WHEN NOT regexp_like({suggested_name}, '^[^@]+@[^@]+\\.[^@]+$') THEN 1 ELSE 0 END) = 0 
            THEN 'PASS' ELSE 'FAIL' END AS test_result
FROM {{{{ ref('{table_name}') }}}}
WHERE {suggested_name} IS NOT NULL;

"""

        # Compliance tests
        sql += """-- ========================================
-- COMPLIANCE TESTS
-- ========================================

"""

        pii_columns = [col for col in enhanced_schema if col.get('potential_pii')]
        if pii_columns:
            sql += "-- PII Data Audit\n"
            sql += "SELECT 'PII_Data_Audit' AS test_name,\n"
            sql += "       COUNT(*) AS total_records,\n"

            for col in pii_columns:
                if col.get('is_overall_assessment'):
                    continue
                suggested_name = col.get('suggested_name', col['column_name'])
                sql += f"       COUNT(DISTINCT {suggested_name}) AS {suggested_name}_unique_values,\n"

            sql = sql.rstrip(',\n') + f"\nFROM {{{{ ref('{table_name}') }}}};\n\n"

        return sql

    def generate_business_documentation(self, enhanced_schema: List[Dict[str, Any]]) -> str:
        """Generate comprehensive business documentation."""

        table_name = self.project_context.get('name', 'migration_table')

        doc = f"""# {table_name} - Business Data Dictionary

**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  
**Industry:** {self.industry}  
**Source System:** {self.source_system}  
**Target Platform:** {self.target_platform}  

## Overview

This document provides business-friendly documentation for the {table_name} dataset as part of the data migration from {self.source_system} to {self.target_platform}.

## Business Context

This dataset contains {self.industry} data that supports critical business operations including:

"""

        # Add business context based on columns
        business_areas = set()
        for col in enhanced_schema:
            if col.get('is_overall_assessment'):
                continue
            context = col.get('industry_context', '')
            if context:
                business_areas.add(context.split('.')[0] if '.' in context else context)

        for area in sorted(business_areas):
            doc += f"- {area}\n"

        doc += f"""
## Data Quality Summary

"""

        # Add data quality summary
        total_columns = len([col for col in enhanced_schema if not col.get('is_overall_assessment')])
        high_quality = len([col for col in enhanced_schema
                            if col.get('data_quality_score', 0) > 0.8 and not col.get('is_overall_assessment')])
        pii_columns = len([col for col in enhanced_schema
                           if col.get('potential_pii') and not col.get('is_overall_assessment')])

        doc += f"""- **Total Columns:** {total_columns}
- **High Quality Columns:** {high_quality} ({round(100 * high_quality / total_columns, 1)}%)
- **PII Fields:** {pii_columns}
- **Business Critical Fields:** {len([col for col in enhanced_schema if self._safe_startswith(col.get('business_criticality'), 'High')])}

## Column Reference

| Column Name | Business Description | Data Type | Business Criticality | Compliance Notes |
|-------------|---------------------|-----------|-------------------|------------------|
"""

        for col in enhanced_schema:
            if col.get('is_overall_assessment'):
                continue

            suggested_name = col.get('suggested_name', col['column_name'])
            description = col.get('business_description', 'No description')[:50] + '...'
            data_type = col['data_type']
            criticality = col.get('business_criticality', 'Medium')
            compliance = ', '.join(col.get('compliance_implications', [])[:2])
            if len(col.get('compliance_implications', [])) > 2:
                compliance += '...'

            doc += f"| `{suggested_name}` | {description} | {data_type} | {criticality} | {compliance} |\n"

        doc += f"""
## Migration Notes

### Transformation Summary
"""

        transformed_columns = [col for col in enhanced_schema
                               if col.get('transformation_suggestions') and not col.get('is_overall_assessment')]

        if transformed_columns:
            doc += f"\n{len(transformed_columns)} columns require transformation during migration:\n\n"
            for col in transformed_columns:
                doc += f"- **{col.get('suggested_name')}**: {', '.join(col['transformation_suggestions'][:2])}\n"
        else:
            doc += "\nNo complex transformations required for this migration.\n"

        doc += f"""
### Compliance Requirements

This dataset is subject to the following compliance requirements:

"""

        all_compliance = set()
        for col in enhanced_schema:
            if col.get('is_overall_assessment'):
                continue
            all_compliance.update(col.get('compliance_implications', []))

        for req in sorted(all_compliance):
            doc += f"- {req}\n"

        doc += f"""
## Business Glossary

### Key Terms and Definitions
"""

        # Group columns by business domain
        domains = {}
        for col in enhanced_schema:
            if col.get('is_overall_assessment'):
                continue
            context = col.get('industry_context', 'General')
            domain = context.split(' ')[0] if context else 'General'
            if domain not in domains:
                domains[domain] = []
            domains[domain].append(col)

        for domain, columns in domains.items():
            doc += f"\n#### {domain}\n\n"
            for col in columns[:5]:  # Limit to top 5 per domain
                doc += f"**{col.get('suggested_name')}**: {col.get('business_description', 'No description')}\n\n"

        return doc

    def generate_migration_script(self, enhanced_schema: List[Dict[str, Any]]) -> str:
        """Generate platform-specific migration script."""

        if self.target_platform == 'Snowflake':
            return self._generate_snowflake_ddl(enhanced_schema)
        elif self.target_platform == 'BigQuery':
            return self._generate_bigquery_ddl(enhanced_schema)
        else:
            return self._generate_generic_ddl(enhanced_schema)

    def _generate_snowflake_ddl(self, enhanced_schema: List[Dict[str, Any]]) -> str:
        """Generate Snowflake-specific DDL."""

        table_name = self.project_context.get('name', 'MIGRATION_TABLE').upper()

        ddl = f"""-- Snowflake DDL for {table_name}
-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

CREATE OR REPLACE TABLE {table_name} (
"""

        column_definitions = []
        for col in enhanced_schema:
            if col.get('is_overall_assessment'):
                continue

            suggested_name = col.get('suggested_name', col['column_name']).upper()
            data_type = self._map_to_snowflake_type(col['data_type'])

            # Add nullability based on business criticality
            business_criticality = col.get('business_criticality') or ''
            nullable = "NOT NULL" if business_criticality.startswith('High') else "NULL"

            comment = col.get('business_description', '')[:100]

            column_def = f"    {suggested_name:<30} {data_type:<20} {nullable}"
            if comment:
                column_def += f" COMMENT '{comment}'"

            column_definitions.append(column_def)

        ddl += ",\n".join(column_definitions)

        ddl += f"""
)
COMMENT = 'Enhanced {self.industry} data migrated from {self.source_system}'
;

-- Create indexes for business keys
"""

        # Add indexes for business keys
        for col in enhanced_schema:
            if col.get('potential_business_key') and not col.get('is_overall_assessment'):
                suggested_name = col.get('suggested_name', col['column_name']).upper()
                ddl += f"CREATE INDEX IF NOT EXISTS IDX_{table_name}_{suggested_name} ON {table_name}({suggested_name});\n"

        return ddl

    def _generate_bigquery_ddl(self, enhanced_schema: List[Dict[str, Any]]) -> str:
        """Generate BigQuery-specific DDL."""

        table_name = self.project_context.get('name', 'migration_table').lower()

        ddl = f"""-- BigQuery DDL for {table_name}
-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

CREATE OR REPLACE TABLE `project.dataset.{table_name}` (
"""

        column_definitions = []
        for col in enhanced_schema:
            if col.get('is_overall_assessment'):
                continue

            suggested_name = col.get('suggested_name', col['column_name']).lower()
            data_type = self._map_to_bigquery_type(col['data_type'])

            # Add mode based on business criticality
            mode = "REQUIRED" if self._safe_startswith(col.get('business_criticality'), 'High') else "NULLABLE"

            description = col.get('business_description', '')[:1024]  # BigQuery limit

            column_def = f"    {suggested_name} {data_type} OPTIONS(description='{description}')"
            column_definitions.append(column_def)

        ddl += ",\n".join(column_definitions)

        ddl += f"""
)
OPTIONS(
    description='Enhanced {self.industry} data migrated from {self.source_system}',
    labels=[('industry', '{self.industry.lower()}'), ('source', 'migration')]
)
;"""

        return ddl

    def _generate_generic_ddl(self, enhanced_schema: List[Dict[str, Any]]) -> str:
        """Generate generic SQL DDL."""

        table_name = self.project_context.get('name', 'migration_table')

        ddl = f"""-- Generic DDL for {table_name}
-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
-- Target Platform: {self.target_platform}

CREATE TABLE {table_name} (
"""

        column_definitions = []
        for col in enhanced_schema:
            if col.get('is_overall_assessment'):
                continue

            suggested_name = col.get('suggested_name', col['column_name'])
            data_type = self._map_to_generic_type(col['data_type'])

            column_definitions.append(f"    {suggested_name} {data_type}")

        ddl += ",\n".join(column_definitions)
        ddl += "\n);"

        return ddl

    def generate_lineage_docs(self, enhanced_schema: List[Dict[str, Any]]) -> str:
        """Generate data lineage documentation."""

        table_name = self.project_context.get('name', 'migration_table')

        lineage = f"""# Data Lineage Documentation

## Source to Target Mapping

**Migration Project:** {table_name}  
**Source:** {self.source_system}  
**Target:** {self.target_platform}  
**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Field Mappings

| Source Field | Target Field | Transformation | Business Rationale |
|--------------|--------------|----------------|-------------------|
"""

        for col in enhanced_schema:
            if col.get('is_overall_assessment'):
                continue

            original_name = col['column_name']
            suggested_name = col.get('suggested_name', original_name)
            transformations = col.get('transformation_suggestions', [])

            transformation_desc = transformations[0] if transformations else "Direct mapping"
            business_rationale = col.get('business_description', 'Standard field')[:50]

            lineage += f"| `{original_name}` | `{suggested_name}` | {transformation_desc} | {business_rationale} |\n"

        lineage += f"""
## Transformation Rules

### Applied Transformations
"""

        transformed_fields = [col for col in enhanced_schema
                              if col.get('transformation_suggestions') and not col.get('is_overall_assessment')]

        for col in transformed_fields:
            lineage += f"""
#### {col.get('suggested_name', col['column_name'])}
- **Source:** {col['column_name']}
- **Transformations:** {', '.join(col['transformation_suggestions'])}
- **Business Justification:** {col.get('business_description', 'N/A')}
"""

        return lineage

    def _build_transformation_logic(self, column_name: str, transformations: List[str], data_type: str) -> str:
        """Build SQL transformation logic based on suggestions."""

        base_column = column_name

        for transformation in transformations:
            if 'uppercase' in transformation.lower():
                base_column = f"UPPER({base_column})"
            elif 'lowercase' in transformation.lower():
                base_column = f"LOWER({base_column})"
            elif 'trim' in transformation.lower():
                base_column = f"TRIM({base_column})"
            elif 'standardize phone' in transformation.lower():
                base_column = f"REGEXP_REPLACE({base_column}, '[^0-9]', '')"
            elif 'format currency' in transformation.lower():
                base_column = f"ROUND({base_column}, 2)"
            elif 'null to zero' in transformation.lower() and data_type in ['integer', 'float']:
                base_column = f"COALESCE({base_column}, 0)"
            elif 'extract date' in transformation.lower():
                base_column = f"DATE({base_column})"

        return base_column

    def _map_to_snowflake_type(self, generic_type: str) -> str:
        """Map generic data types to Snowflake types."""
        mapping = {
            'string': 'VARCHAR(255)',
            'integer': 'NUMBER(38,0)',
            'float': 'NUMBER(38,2)',
            'boolean': 'BOOLEAN',
            'date': 'DATE',
            'datetime': 'TIMESTAMP_NTZ',
            'timestamp': 'TIMESTAMP_NTZ'
        }
        return mapping.get(generic_type, 'VARCHAR(255)')

    def _map_to_bigquery_type(self, generic_type: str) -> str:
        """Map generic data types to BigQuery types."""
        mapping = {
            'string': 'STRING',
            'integer': 'INT64',
            'float': 'FLOAT64',
            'boolean': 'BOOL',
            'date': 'DATE',
            'datetime': 'DATETIME',
            'timestamp': 'TIMESTAMP'
        }
        return mapping.get(generic_type, 'STRING')

    def _map_to_generic_type(self, generic_type: str) -> str:
        """Map to generic SQL types."""
        mapping = {
            'string': 'VARCHAR(255)',
            'integer': 'INTEGER',
            'float': 'DECIMAL(10,2)',
            'boolean': 'BOOLEAN',
            'date': 'DATE',
            'datetime': 'TIMESTAMP',
            'timestamp': 'TIMESTAMP'
        }
        return mapping.get(generic_type, 'VARCHAR(255)')

    def generate_project_summary(self, enhanced_schema: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate comprehensive project summary metrics."""

        total_columns = len([col for col in enhanced_schema if not col.get('is_overall_assessment')])

        # Calculate various metrics
        summary = {
            'project_info': self.project_context,
            'schema_metrics': {
                'total_columns': total_columns,
                'enhanced_columns': len([col for col in enhanced_schema if col.get('enhanced')]),
                'pii_columns': len([col for col in enhanced_schema if col.get('potential_pii')]),
                'business_keys': len([col for col in enhanced_schema if col.get('potential_business_key')]),
                'high_quality_columns': len([col for col in enhanced_schema if col.get('data_quality_score', 0) > 0.8])
            },
            'business_impact': {
                'high_criticality_fields': len(
                    [col for col in enhanced_schema if self._safe_startswith(col.get('business_criticality'), 'High')]),
                'compliance_requirements': len(
                    set().union(*[col.get('compliance_implications', []) for col in enhanced_schema])),
                'transformation_complexity': len(
                    [col for col in enhanced_schema if col.get('transformation_suggestions')])
            },
            'migration_readiness': self._assess_migration_readiness(enhanced_schema)
        }

        return summary

    def _assess_migration_readiness(self, enhanced_schema: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Assess overall migration readiness."""

        total_columns = len([col for col in enhanced_schema if not col.get('is_overall_assessment')])

        # Calculate readiness scores
        data_quality_score = sum([col.get('data_quality_score', 0) for col in enhanced_schema if
                                  not col.get('is_overall_assessment')]) / total_columns

        high_complexity_count = len([col for col in enhanced_schema if col.get('migration_complexity') == 'High'])
        complexity_score = 1.0 - (high_complexity_count / total_columns)

        documentation_score = len([col for col in enhanced_schema if col.get('business_description') and not col.get(
            'is_overall_assessment')]) / total_columns

        overall_readiness = (data_quality_score + complexity_score + documentation_score) / 3

        return {
            'overall_score': round(overall_readiness, 2),
            'data_quality_score': round(data_quality_score, 2),
            'complexity_score': round(complexity_score, 2),
            'documentation_score': round(documentation_score, 2),
            'readiness_level': 'High' if overall_readiness > 0.8 else 'Medium' if overall_readiness > 0.6 else 'Low',
            'recommendations': self._generate_readiness_recommendations(overall_readiness, enhanced_schema)
        }

    def _generate_readiness_recommendations(self, readiness_score: float, enhanced_schema: List[Dict[str, Any]]) -> \
    List[str]:
        """Generate recommendations to improve migration readiness."""
        recommendations = []

        if readiness_score < 0.8:
            # Data quality recommendations
            low_quality_columns = [col for col in enhanced_schema if col.get('data_quality_score', 0) < 0.7]
            if low_quality_columns:
                recommendations.append(f"Improve data quality for {len(low_quality_columns)} columns before migration")

            # Complexity recommendations
            high_complexity_columns = [col for col in enhanced_schema if col.get('migration_complexity') == 'High']
            if high_complexity_columns:
                recommendations.append(
                    f"Develop detailed migration plan for {len(high_complexity_columns)} complex transformations")

            # Documentation recommendations
            undocumented_columns = [col for col in enhanced_schema if not col.get('business_description')]
            if undocumented_columns:
                recommendations.append(f"Add business documentation for {len(undocumented_columns)} columns")

        return recommendations