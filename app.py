import streamlit as st
import pandas as pd
import json
from pathlib import Path
import sys
import sqlite3
import io
from datetime import datetime
import yaml

# Add src directory to path for imports
sys.path.append(str(Path(__file__).parent / "src"))

from src.multi_format_processor import MultiFormatProcessor
from src.ollama_client import OllamaClient
from src.schema_enricher import EnhancedSchemaEnricher
from src.migration_generator import MigrationGenerator
from src.business_context_engine import BusinessContextEngine

# Page configuration
st.set_page_config(
    page_title="Data Migration Accelerator",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'source_data' not in st.session_state:
    st.session_state.source_data = None
if 'original_schema' not in st.session_state:
    st.session_state.original_schema = None
if 'enriched_schema' not in st.session_state:
    st.session_state.enriched_schema = None
if 'migration_artifacts' not in st.session_state:
    st.session_state.migration_artifacts = None
if 'ollama_connected' not in st.session_state:
    st.session_state.ollama_connected = False
if 'selected_industry' not in st.session_state:
    st.session_state.selected_industry = "General"
if 'project_context' not in st.session_state:
    st.session_state.project_context = {}


def main():
    st.title("🚀 Data Migration Accelerator")
    st.markdown("*AI-powered schema discovery and migration automation for data engineers*")

    # Sidebar for configuration
    with st.sidebar:
        st.header("⚙️ Configuration")

        # Project Context
        st.subheader("📋 Project Context")
        project_name = st.text_input("Migration Project Name", value="Legacy_to_Modern_Migration")
        source_system = st.text_input("Source System", value="Legacy Oracle DB")
        target_system = st.selectbox("Target Platform",
                                     ["Snowflake", "BigQuery", "Databricks", "Redshift", "PostgreSQL"])

        # Industry Context for Better AI Understanding
        industry = st.selectbox("Industry Domain", [
            "General", "Financial Services", "Healthcare", "Retail/E-commerce",
            "Online Travel Agency (OTA)", "Manufacturing", "Telecommunications", "Government"
        ])
        st.session_state.selected_industry = industry

        st.subheader("📊 Data Generation Settings")
        sample_size = st.selectbox("Sample Data Size", [
            1000, 5000, 10000, 25000, 50000
        ], index=2)  # Default to 10000

        # Store in project context
        st.session_state.project_context['sample_size'] = sample_size

        # Store project context
        st.session_state.project_context = {
            "name": project_name,
            "source": source_system,
            "target": target_system,
            "industry": industry,
            "created_date": datetime.now().isoformat()
        }

        # Ollama connection
        st.subheader("🤖 AI Engine Status")
        ollama_client = OllamaClient()

        if st.button("🔄 Check AI Connection"):
            with st.spinner("Checking Ollama connection..."):
                st.session_state.ollama_connected = ollama_client.check_connection()

        if st.session_state.ollama_connected:
            st.success("✅ AI Engine Connected")
            available_models = ollama_client.get_available_models()
            if available_models:
                selected_model = st.selectbox("AI Model", available_models)
        else:
            st.error("❌ AI Engine Disconnected")
            st.info("Start Ollama: `ollama serve`")

    # Main tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📁 Data Discovery",
        "🔍 Schema Analysis",
        "✨ AI Enhancement",
        "🛠️ Migration Assets",
        "📊 Project Summary"
    ])

    with tab1:
        st.header("Multi-Source Data Discovery")

        # Data source selection
        source_type = st.selectbox("Select Data Source Type", [
            "CSV File Upload",
            "Database Connection",
            "JSON Schema",
            "Sample Data Generator"
        ])

        processor = MultiFormatProcessor()

        if source_type == "CSV File Upload":
            uploaded_file = st.file_uploader("Upload CSV File", type=['csv'])

            if uploaded_file is not None:
                try:
                    with st.spinner("🔄 Processing CSV file..."):
                        result = processor.process_csv(uploaded_file)
                        st.session_state.source_data = result['data']
                        st.session_state.original_schema = result['schema']

                    st.success("✅ CSV processed successfully!")
                    display_data_preview(result['data'])

                except Exception as e:
                    st.error(f"❌ Error processing file: {str(e)}")

        elif source_type == "Database Connection":
            st.info("🔧 Database connector - Feature coming in next iteration")

            # For POC, let's create a sample database schema
            # Add OTA-specific sample generation
            if st.button("🏨 Generate Sample OTA/Booking Database Schema"):
                with st.spinner("Generating sample OTA database schema..."):
                    result = processor.generate_sample_ota_schema(sample_size)
                    st.session_state.source_data = result['data']
                    st.session_state.original_schema = result['schema']

                st.success("✅ Sample OTA booking schema generated!")
                display_data_preview(result['data'])

        elif source_type == "JSON Schema":
            st.info("📝 Upload or paste JSON schema")
            json_input = st.text_area("JSON Schema", height=200,
                                      placeholder='{"users": {"id": "integer", "name": "string", "email": "string"}}')

            if json_input and st.button("Process JSON Schema"):
                try:
                    result = processor.process_json_schema(json_input)
                    st.session_state.source_data = result['data']
                    st.session_state.original_schema = result['schema']
                    st.success("✅ JSON schema processed!")
                    display_data_preview(result['data'])
                except Exception as e:
                    st.error(f"❌ Error processing JSON: {str(e)}")

        elif source_type == "Sample Data Generator":
            sample_type = st.selectbox("Sample Dataset Type", [
                "Financial Transactions",
                "Customer Records",
                "Product Catalog",
                "Healthcare Records",
                "Manufacturing Data",
                "OTA Booking Data",
                "Hotel Property Data",
                "Travel Search Data"
            ])

            if st.button(f"🎲 Generate {sample_type}"):
                with st.spinner(f"Generating {sample_type.lower()}..."):
                    result = processor.generate_sample_data(sample_type, sample_size)
                    st.session_state.source_data = result['data']
                    st.session_state.original_schema = result['schema']

                st.success(f"✅ {sample_type} generated!")
                display_data_preview(result['data'])

    with tab2:
        if st.session_state.original_schema is not None:
            st.header("📊 Schema Analysis & Data Profiling")

            # Display enhanced schema table
            display_enhanced_schema_analysis()

            # Data quality dashboard
            display_data_quality_dashboard()

        else:
            st.info("👆 Please discover data source first in the Data Discovery tab")

    with tab3:
        if st.session_state.original_schema is not None and st.session_state.ollama_connected:
            st.header("🧠 AI-Powered Schema Enhancement")

            # Enhancement options with industry context
            col1, col2 = st.columns([2, 1])

            with col1:
                st.subheader(f"🎯 {industry}-Specific Enhancements")
                enhancement_options = st.multiselect(
                    "Select Enhancement Types:",
                    [
                        "Business-Friendly Column Names",
                        "Industry-Specific Descriptions",
                        "Data Governance & Compliance",
                        "Data Quality Rules",
                        "Transformation Suggestions",
                        "Business KPI Identification"
                    ],
                    default=["Business-Friendly Column Names", "Industry-Specific Descriptions"]
                )

            with col2:
                st.subheader("🚀 AI Processing")
                if st.button("✨ Enhance with AI", type="primary", use_container_width=True):
                    if enhancement_options:
                        enhance_schema_with_ai(enhancement_options, ollama_client)
                    else:
                        st.warning("⚠️ Please select at least one enhancement option")

            # Display enhancement results
            if st.session_state.enriched_schema is not None:
                display_ai_enhancement_results()

        elif not st.session_state.ollama_connected:
            st.warning("🤖 AI Engine connection required for schema enhancement")
        else:
            st.info("👆 Please discover data source first")

    with tab4:
        if st.session_state.enriched_schema is not None:
            st.header("🛠️ Migration Asset Generation")

            # Generate migration artifacts
            if st.button("🏗️ Generate Migration Assets", type="primary"):
                generate_migration_assets()

            # Display generated assets
            if st.session_state.migration_artifacts:
                display_migration_artifacts()

        else:
            st.info("👆 Please complete AI enhancement first")

    with tab5:
        if st.session_state.project_context:
            st.header("📊 Migration Project Summary")
            display_project_summary()
        else:
            st.info("👆 Complete the previous steps to see project summary")


def display_data_preview(data):
    """Display data preview with metrics."""
    if isinstance(data, pd.DataFrame):
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("📏 Rows", f"{len(data):,}")
        with col2:
            st.metric("📊 Columns", len(data.columns))
        with col3:
            memory_usage = data.memory_usage(deep=True).sum() / 1024 / 1024
            st.metric("💾 Memory", f"{memory_usage:.1f} MB")
        with col4:
            completeness = (1 - data.isnull().sum().sum() / (len(data) * len(data.columns))) * 100
            st.metric("✅ Completeness", f"{completeness:.1f}%")

        st.subheader("📋 Data Preview")
        st.dataframe(data.head(10), use_container_width=True)


def display_enhanced_schema_analysis():
    """Display enhanced schema analysis."""
    schema_df = pd.DataFrame(st.session_state.original_schema)

    # Enhanced schema display with better formatting
    st.subheader("📋 Schema Overview")

    # Format schema for display
    display_columns = [
        'column_name', 'data_type', 'completeness_pct',
        'unique_count', 'sample_values'
    ]

    formatted_df = schema_df[display_columns].copy()
    formatted_df['completeness_pct'] = formatted_df['completeness_pct'].round(1)

    st.dataframe(
        formatted_df,
        use_container_width=True,
        column_config={
            "column_name": st.column_config.TextColumn("Column", width=150),
            "data_type": st.column_config.TextColumn("Type", width=100),
            "completeness_pct": st.column_config.ProgressColumn(
                "Completeness %", min_value=0, max_value=100
            ),
            "unique_count": st.column_config.NumberColumn("Unique Values"),
            "sample_values": st.column_config.ListColumn("Sample Values")
        }
    )


def display_data_quality_dashboard():
    """Display data quality insights."""
    st.subheader("🔍 Data Quality Assessment")

    schema_df = pd.DataFrame(st.session_state.original_schema)

    # Quality metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        high_quality_cols = len(schema_df[schema_df['completeness_pct'] >= 95])
        st.metric("🟢 High Quality Columns", high_quality_cols)

    with col2:
        medium_quality_cols = len(schema_df[
                                      (schema_df['completeness_pct'] >= 70) &
                                      (schema_df['completeness_pct'] < 95)
                                      ])
        st.metric("🟡 Medium Quality Columns", medium_quality_cols)

    with col3:
        low_quality_cols = len(schema_df[schema_df['completeness_pct'] < 70])
        st.metric("🔴 Low Quality Columns", low_quality_cols)

    with col4:
        avg_completeness = schema_df['completeness_pct'].mean()
        st.metric("📊 Avg Completeness", f"{avg_completeness:.1f}%")


def enhance_schema_with_ai(enhancement_options, ollama_client):
    """Enhance schema using AI."""
    with st.spinner("🧠 AI is analyzing your schema..."):
        try:
            enricher = EnhancedSchemaEnricher(ollama_client)
            context_engine = BusinessContextEngine(st.session_state.selected_industry)

            # Add business context to the schema
            contextualized_schema = context_engine.add_business_context(
                st.session_state.original_schema
            )

            # Enhance with AI
            enhanced_schema = enricher.enhance_schema(
                contextualized_schema,
                enhancement_options,
                st.session_state.project_context
            )

            st.session_state.enriched_schema = enhanced_schema
            st.success("✅ Schema enhanced successfully with AI insights!")
            st.rerun()

        except Exception as e:
            st.error(f"❌ AI enhancement failed: {str(e)}")


def display_ai_enhancement_results():
    """Display AI enhancement results."""
    st.subheader("🎯 AI Enhancement Results")

    # Create comparison view
    for i, enhanced_col in enumerate(st.session_state.enriched_schema):
        if enhanced_col.get('is_overall_assessment'):
            continue

        with st.expander(
                f"📊 {enhanced_col['column_name']} → {enhanced_col.get('suggested_name', enhanced_col['column_name'])}",
                expanded=False):
            col1, col2 = st.columns(2)

            with col1:
                st.write("**🔍 Original Analysis:**")
                st.write(f"**Name:** `{enhanced_col['column_name']}`")
                st.write(f"**Type:** `{enhanced_col['data_type']}`")
                st.write(f"**Completeness:** {enhanced_col['completeness_pct']}%")

            with col2:
                st.write("**✨ AI Enhancement:**")
                st.write(f"**Suggested Name:** `{enhanced_col.get('suggested_name', 'N/A')}`")
                st.write(f"**Business Description:** {enhanced_col.get('business_description', 'N/A')}")

                if enhanced_col.get('compliance_notes'):
                    st.write(f"**🛡️ Compliance:** {enhanced_col['compliance_notes']}")

                if enhanced_col.get('transformation_suggestions'):
                    st.write("**🔄 Transformations:**")
                    for suggestion in enhanced_col['transformation_suggestions']:
                        st.write(f"• {suggestion}")


def generate_migration_assets():
    """Generate migration artifacts."""
    with st.spinner("🏗️ Generating migration assets..."):
        try:
            generator = MigrationGenerator(st.session_state.project_context)

            assets = generator.generate_all_assets(
                st.session_state.enriched_schema,
                st.session_state.source_data
            )

            st.session_state.migration_artifacts = assets
            st.success("✅ Migration assets generated successfully!")
            st.rerun()

        except Exception as e:
            st.error(f"❌ Asset generation failed: {str(e)}")


def display_migration_artifacts():
    """Display generated migration artifacts."""
    st.subheader("📦 Generated Migration Assets")

    artifacts = st.session_state.migration_artifacts

    # Tabs for different artifacts
    art_tab1, art_tab2, art_tab3, art_tab4 = st.tabs([
        "🏗️ dbt Models", "📋 Schema YML", "🧪 Data Quality Tests", "📚 Documentation"
    ])

    with art_tab1:
        st.subheader("dbt SQL Model")
        if 'dbt_model' in artifacts:
            st.code(artifacts['dbt_model'], language='sql')
            st.download_button(
                "💾 Download dbt Model",
                artifacts['dbt_model'],
                file_name=f"{st.session_state.project_context['name']}_model.sql",
                mime="text/sql"
            )

    with art_tab2:
        st.subheader("dbt Schema YAML")
        if 'schema_yml' in artifacts:
            st.code(artifacts['schema_yml'], language='yaml')
            st.download_button(
                "💾 Download Schema YML",
                artifacts['schema_yml'],
                file_name="schema.yml",
                mime="text/yaml"
            )

    with art_tab3:
        st.subheader("Data Quality Tests")
        if 'quality_tests' in artifacts:
            st.code(artifacts['quality_tests'], language='sql')
            st.download_button(
                "💾 Download Quality Tests",
                artifacts['quality_tests'],
                file_name="quality_tests.sql",
                mime="text/sql"
            )

    with art_tab4:
        st.subheader("Business Glossary")
        if 'documentation' in artifacts:
            st.markdown(artifacts['documentation'])
            st.download_button(
                "💾 Download Documentation",
                artifacts['documentation'],
                file_name="business_glossary.md",
                mime="text/markdown"
            )


def display_project_summary():
    """Display project summary and metrics."""
    context = st.session_state.project_context

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📋 Project Details")
        st.write(f"**Project:** {context['name']}")
        st.write(f"**Source:** {context['source']}")
        st.write(f"**Target:** {context['target']}")
        st.write(f"**Industry:** {context['industry']}")
        st.write(f"**Created:** {context['created_date'][:10]}")

    with col2:
        st.subheader("📊 Migration Metrics")
        if st.session_state.original_schema:
            st.metric("📊 Tables Analyzed", 1)
            st.metric("📋 Columns Processed", len(st.session_state.original_schema))

            if st.session_state.enriched_schema:
                enhanced_count = len([c for c in st.session_state.enriched_schema
                                      if c.get('enhanced')])
                st.metric("✨ AI Enhanced Columns", enhanced_count)

            if st.session_state.migration_artifacts:
                asset_count = len(st.session_state.migration_artifacts)
                st.metric("🛠️ Generated Assets", asset_count)


if __name__ == "__main__":
    main()