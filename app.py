import streamlit as st
import pandas as pd
import json
from pathlib import Path
import sys

# Add src directory to path for imports
sys.path.append(str(Path(__file__).parent / "src"))

from src.csv_processor import CSVProcessor
from src.ollama_client import OllamaClient
from src.schema_enricher import SchemaEnricher

# Page configuration
st.set_page_config(
    page_title="CSV Schema Enricher",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'csv_data' not in st.session_state:
    st.session_state.csv_data = None
if 'original_schema' not in st.session_state:
    st.session_state.original_schema = None
if 'enriched_schema' not in st.session_state:
    st.session_state.enriched_schema = None
if 'ollama_connected' not in st.session_state:
    st.session_state.ollama_connected = False


def main():
    st.title("🔍 CSV Schema Enricher")
    st.markdown("*Automatically analyze CSV files and enhance schema with AI-powered insights*")

    # Sidebar for configuration and status
    with st.sidebar:
        st.header("Configuration")

        # Ollama connection status
        st.subheader("🤖 Ollama Status")
        ollama_client = OllamaClient()

        if st.button("Check Connection"):
            with st.spinner("Checking Ollama connection..."):
                st.session_state.ollama_connected = ollama_client.check_connection()

        if st.session_state.ollama_connected:
            st.success("✅ Connected to Ollama")
            available_models = ollama_client.get_available_models()

            if available_models:
                selected_model = st.selectbox(
                    "Select Model",
                    available_models,
                    index=0 if 'gemma:latest' in available_models[0] else 0
                )
            else:
                st.warning("No models found. Make sure Mistral is installed.")
        else:
            st.error("❌ Ollama not connected")
            st.info("Make sure Ollama is running:\n```bash\nollama serve\n```")

    # Main content area
    tab1, tab2, tab3 = st.tabs(["📁 Upload & Analyze", "🔍 Schema Details", "✨ AI Enhancement"])

    with tab1:
        st.header("Upload CSV File")

        uploaded_file = st.file_uploader(
            "Choose a CSV file",
            type=['csv'],
            help="Upload a CSV file to analyze its schema"
        )

        if uploaded_file is not None:
            try:
                # Process CSV file
                processor = CSVProcessor()

                with st.spinner("Processing CSV file..."):
                    csv_data = processor.process_file(uploaded_file)
                    st.session_state.csv_data = csv_data
                    st.session_state.original_schema = processor.infer_schema(csv_data)

                st.success("✅ CSV file processed successfully!")

                # Display basic file info
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Rows", len(csv_data))
                with col2:
                    st.metric("Columns", len(csv_data.columns))
                with col3:
                    st.metric("File Size", f"{uploaded_file.size / 1024:.1f} KB")

                # Data preview
                st.subheader("Data Preview")
                st.dataframe(
                    csv_data.head(10),
                    use_container_width=True,
                    height=300
                )

            except Exception as e:
                st.error(f"Error processing file: {str(e)}")

    with tab2:
        if st.session_state.original_schema is not None:
            st.header("Inferred Schema")

            # Display schema as a table
            schema_df = pd.DataFrame(st.session_state.original_schema)

            # Format the schema display
            st.dataframe(
                schema_df,
                use_container_width=True,
                column_config={
                    "column_name": st.column_config.TextColumn(
                        "Column Name",
                        help="Original column name from CSV"
                    ),
                    "data_type": st.column_config.TextColumn(
                        "Data Type",
                        help="Inferred data type"
                    ),
                    "null_count": st.column_config.NumberColumn(
                        "Null Count",
                        help="Number of null/empty values"
                    ),
                    "unique_count": st.column_config.NumberColumn(
                        "Unique Values",
                        help="Number of unique values"
                    ),
                    "sample_values": st.column_config.ListColumn(
                        "Sample Values",
                        help="Sample values from the column"
                    )
                }
            )

            # Data quality insights
            st.subheader("Data Quality Insights")

            total_columns = len(schema_df)
            high_null_columns = len(schema_df[schema_df['null_count'] > len(st.session_state.csv_data) * 0.5])

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Columns", total_columns)
            with col2:
                st.metric("High Null Columns", high_null_columns)
            with col3:
                completeness = ((len(st.session_state.csv_data) * total_columns - schema_df['null_count'].sum()) /
                                (len(st.session_state.csv_data) * total_columns) * 100)
                st.metric("Data Completeness", f"{completeness:.1f}%")

        else:
            st.info("Please upload a CSV file first to see schema details.")

    with tab3:
        if st.session_state.original_schema is not None:
            st.header("AI-Powered Schema Enhancement")

            if not st.session_state.ollama_connected:
                st.warning("⚠️ Ollama connection required for AI enhancement. Please check connection in sidebar.")
            else:
                col1, col2 = st.columns([1, 1])

                with col1:
                    st.subheader("Original Schema")
                    original_df = pd.DataFrame(st.session_state.original_schema)
                    st.dataframe(original_df[['column_name', 'data_type']], use_container_width=True)

                with col2:
                    st.subheader("Enhanced Schema")
                    if st.session_state.enriched_schema is not None:
                        enriched_df = pd.DataFrame(st.session_state.enriched_schema)
                        st.dataframe(enriched_df, use_container_width=True)
                    else:
                        st.info("Click 'Enhance Schema' to get AI suggestions")

                # Enhancement controls
                st.subheader("Enhancement Options")

                col1, col2 = st.columns([2, 1])
                with col1:
                    enhancement_options = st.multiselect(
                        "What to enhance:",
                        ["Column Names", "Business Descriptions", "Data Quality Assessment",
                         "Transformation Suggestions"],
                        default=["Column Names", "Business Descriptions"]
                    )

                with col2:
                    if st.button("✨ Enhance Schema", type="primary", use_container_width=True):
                        if enhancement_options:
                            with st.spinner("Enhancing schema with AI..."):
                                enricher = SchemaEnricher(ollama_client)
                                try:
                                    enhanced_schema = enricher.enhance_schema(
                                        st.session_state.original_schema,
                                        enhancement_options
                                    )
                                    if isinstance(enhanced_schema, dict) and 'raw_response' in enhanced_schema:
                                        with st.expander("📄 Raw LLM Response", expanded=False):
                                            st.code(enhanced_schema["raw_response"], language="json")

                                        if not enhanced_schema.get("is_json", False):
                                            st.error(f"⚠️ JSON parsing failed: {enhanced_schema.get('json_error')}")

                                    st.session_state.enriched_schema = enhanced_schema
                                    st.success("✅ Schema enhanced successfully!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Enhancement failed: {str(e)}")
                        else:
                            st.warning("Please select at least one enhancement option.")

                # Display enhancement results
                if st.session_state.enriched_schema is not None:
                    st.subheader("Enhancement Results")

                    for i, (original, enhanced) in enumerate(zip(
                            st.session_state.original_schema,
                            st.session_state.enriched_schema
                    )):
                        with st.expander(f"Column: {original['column_name']}", expanded=False):
                            col1, col2 = st.columns(2)

                            with col1:
                                st.write("**Original:**")
                                st.write(f"Name: `{original['column_name']}`")
                                st.write(f"Type: `{original['data_type']}`")

                            with col2:
                                st.write("**Enhanced:**")
                                st.write(f"Suggested Name: `{enhanced.get('suggested_name', 'N/A')}`")
                                st.write(f"Description: {enhanced.get('business_description', 'N/A')}")
                                if enhanced.get('data_quality_notes'):
                                    st.write(f"Quality Notes: {enhanced['data_quality_notes']}")
        else:
            st.info("Please upload and analyze a CSV file first.")

    # Footer
    st.markdown("---")
    st.markdown("*Built with Streamlit and powered by Ollama + Mistral*")


if __name__ == "__main__":
    main()