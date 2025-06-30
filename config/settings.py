"""Configuration settings for CSV Schema Enricher."""

import os
import re
from typing import Dict, Any

# Ollama Configuration
OLLAMA_CONFIG = {
    "base_url": os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"),
    "default_model": os.getenv("OLLAMA_MODEL", "gemma:latest"),
    "timeout": int(os.getenv("OLLAMA_TIMEOUT", "30")),
    "temperature": float(os.getenv("OLLAMA_TEMPERATURE", "0.1")),
    "max_tokens": min(2048, int(os.getenv("OLLAMA_MAX_TOKENS", "1000"))),
    "top_p": float(os.getenv("OLLAMA_TOP_P", "0.9")),
    "frequency_penalty": float(os.getenv("OLLAMA_FREQ_PENALTY", "0.0")),
    "presence_penalty": float(os.getenv("OLLAMA_PRES_PENALTY", "0.0")),
}

# CSV Processing Configuration
CSV_CONFIG = {
    "max_file_size_mb": int(os.getenv("MAX_FILE_SIZE_MB", "100")),
    "max_rows_preview": int(os.getenv("MAX_ROWS_PREVIEW", "1000")),
    "supported_encodings": ["utf-8", "latin-1", "cp1252", "iso-8859-1"],
    "date_patterns": [
        r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
        r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
        r'\d{2}-\d{2}-\d{4}',  # MM-DD-YYYY
        r'\d{4}/\d{2}/\d{2}',  # YYYY/MM/DD
    ],
    "compiled_date_patterns": []  # to be filled below
}

# Compile regex patterns (performance boost if reused frequently)
CSV_CONFIG["compiled_date_patterns"] = [re.compile(p) for p in CSV_CONFIG["date_patterns"]]

# Streamlit UI Configuration
STREAMLIT_CONFIG = {
    "page_title": "CSV Schema Enricher",
    "page_icon": "📊",
    "layout": "wide",
    "initial_sidebar_state": "expanded"
}

# Schema Enhancement Configuration
ENHANCEMENT_CONFIG = {
    "default_confidence_threshold": 0.7,
    "max_sample_values": 5,
    "max_description_length": 200,
    "supported_enhancements": [
        "Column Names",
        "Business Descriptions",
        "Data Quality Assessment",
        "Transformation Suggestions"
    ],
    "max_columns_per_chunk": int(os.getenv("MAX_COLUMNS_PER_CHUNK", "10"))  # used for chunking if schema is too large
}

# Data Quality Thresholds
DATA_QUALITY_THRESHOLDS = {
    "high_null_percentage": 50,      # % of nulls to flag as high
    "low_uniqueness_threshold": 0.1, # uniqueness ratio to flag
    "string_length_outlier_factor": 3,
    "completeness_good": 95,         # % completeness considered good
    "completeness_poor": 70          # % completeness considered poor
}

# Debug & Validation Settings
DEBUG_CONFIG = {
    "validate_ai_json": os.getenv("VALIDATE_AI_JSON", "true").lower() == "true",
    "log_ai_prompts": os.getenv("LOG_AI_PROMPTS", "false").lower() == "true"
}


def get_config() -> Dict[str, Any]:
    """Get complete configuration dictionary."""
    config = {
        "ollama": OLLAMA_CONFIG,
        "csv": CSV_CONFIG,
        "streamlit": STREAMLIT_CONFIG,
        "enhancement": ENHANCEMENT_CONFIG,
        "data_quality": DATA_QUALITY_THRESHOLDS,
        "debug": DEBUG_CONFIG
    }

    # Add friendly name for UI
    config["ollama"]["friendly_model_name"] = config["ollama"]["default_model"].replace(":", " ").title()

    return config


def log_config() -> None:
    """Print the current configuration (for debugging)."""
    import pprint
    pprint.pprint(get_config())
