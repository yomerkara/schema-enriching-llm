import pandas as pd
import numpy as np
import chardet
import re
from typing import Dict, List, Any, Optional
from datetime import datetime
import io


class CSVProcessor:
    """Handles CSV file processing and schema inference."""

    def __init__(self):
        self.supported_encodings = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
        self.date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
            r'\d{2}-\d{2}-\d{4}',  # MM-DD-YYYY
            r'\d{4}/\d{2}/\d{2}',  # YYYY/MM/DD
        ]

    def detect_encoding(self, file_bytes: bytes) -> str:
        """Detect file encoding using chardet."""
        try:
            result = chardet.detect(file_bytes)
            encoding = result['encoding']

            # Fallback to common encodings if detection fails
            if not encoding or result['confidence'] < 0.7:
                encoding = 'utf-8'

            return encoding
        except Exception:
            return 'utf-8'

    def process_file(self, uploaded_file) -> pd.DataFrame:
        """Process uploaded CSV file and return DataFrame."""
        # Read file bytes
        file_bytes = uploaded_file.read()
        uploaded_file.seek(0)  # Reset file pointer

        # Detect encoding
        encoding = self.detect_encoding(file_bytes)

        # Try different parsing strategies
        parsing_strategies = [
            {'sep': ',', 'encoding': encoding},
            {'sep': ';', 'encoding': encoding},
            {'sep': '\t', 'encoding': encoding},
            {'sep': ',', 'encoding': 'utf-8'},
            {'sep': ',', 'encoding': 'latin1'},
        ]

        for strategy in parsing_strategies:
            try:
                # Reset file pointer
                uploaded_file.seek(0)

                df = pd.read_csv(
                    uploaded_file,
                    **strategy,
                    on_bad_lines='skip',
                    low_memory=False
                )

                # Validate DataFrame
                if len(df.columns) > 1 and len(df) > 0:
                    # Clean column names
                    df.columns = [self._clean_column_name(col) for col in df.columns]
                    return df

            except Exception as e:
                continue

        raise ValueError("Could not parse CSV file with any supported format")

    def _clean_column_name(self, column_name: str) -> str:
        """Clean and standardize column names."""
        # Convert to string and strip whitespace
        name = str(column_name).strip()

        # Remove quotes
        name = name.strip('"\'')

        # Replace spaces and special characters with underscores
        name = re.sub(r'[^\w\s]', '_', name)
        name = re.sub(r'\s+', '_', name)

        # Remove multiple underscores
        name = re.sub(r'_+', '_', name)

        # Remove leading/trailing underscores
        name = name.strip('_')

        # Ensure name is not empty
        if not name:
            name = 'unnamed_column'

        return name

    def infer_schema(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Infer schema from DataFrame."""
        schema = []

        for column in df.columns:
            column_info = self._analyze_column(df, column)
            schema.append(column_info)

        return schema

    def _analyze_column(self, df: pd.DataFrame, column: str) -> Dict[str, Any]:
        """Analyze individual column and return metadata."""
        series = df[column]

        # Basic statistics
        total_count = len(series)
        null_count = series.isnull().sum()
        unique_count = series.nunique()

        # Get non-null values for type inference
        non_null_values = series.dropna()

        # Infer data type
        inferred_type = self._infer_data_type(non_null_values)

        # Get sample values (first 5 unique non-null values)
        sample_values = [str(val) for val in non_null_values.unique()[:5]]

        # Additional analysis based on type
        analysis = {
            'column_name': column,
            'data_type': inferred_type,
            'total_count': total_count,
            'null_count': null_count,
            'unique_count': unique_count,
            'sample_values': sample_values,
            'completeness_pct': round((total_count - null_count) / total_count * 100, 2),
        }

        # Type-specific analysis
        if inferred_type in ['integer', 'float']:
            analysis.update(self._analyze_numeric_column(non_null_values))
        elif inferred_type == 'string':
            analysis.update(self._analyze_string_column(non_null_values))
        elif inferred_type == 'date':
            analysis.update(self._analyze_date_column(non_null_values))
        elif inferred_type == 'datetime':
            analysis.update(self._analyze_date_column(non_null_values))
        elif inferred_type == 'boolean':
            analysis.update(self._analyze_boolean_column(non_null_values))

        return analysis

    def _infer_data_type(self, series: pd.Series) -> str:
        """Infer data type from series values."""
        if len(series) == 0:
            return 'string'

        # Convert to string for pattern matching
        str_series = series.astype(str)

        # Check for boolean patterns
        if self._is_boolean_column(str_series):
            return 'boolean'

        if self._is_date_column(str_series):
            return 'date'

        # Check for numeric patterns
        numeric_pattern = r'^-?\d+\.?\d*$'
        numeric_matches = str_series.str.match(numeric_pattern).sum()

        if numeric_matches == len(str_series):
            # Check if all values are integers
            if str_series.str.contains('\.').sum() == 0:
                return 'integer'
            else:
                return 'float'

        # Default to string
        return 'string'

    def _is_boolean_column(self, str_series: pd.Series) -> bool:
        """Check if column contains boolean-like values."""
        boolean_values = {
            'true', 'false', 'yes', 'no', 'y', 'n',
            '1', '0', 'on', 'off', 'enabled', 'disabled'
        }

        unique_values = set(str_series.str.lower().unique())
        return len(unique_values) <= 2 and unique_values.issubset(boolean_values)

    def _is_date_column(self, str_series: pd.Series) -> bool:
        """Check if column contains date-like values."""
        match_count = 0

        for val in str_series:
            for pattern in self.date_patterns:
                if re.match(pattern, val):
                    match_count += 1
                    break

        return match_count / len(str_series) > 0.7

    def _analyze_numeric_column(self, series: pd.Series) -> Dict[str, Any]:
        """Analyze numeric column."""
        numeric_series = pd.to_numeric(series, errors='coerce').dropna()

        if numeric_series.empty:
            return {}

        return {
            'min_value': numeric_series.min(),
            'max_value': numeric_series.max(),
            'mean_value': round(numeric_series.mean(), 2),
            'median_value': numeric_series.median(),
            'std_dev': round(numeric_series.std(), 2),
        }

    def _analyze_string_column(self, series: pd.Series) -> Dict[str, Any]:
        """Analyze string column."""
        str_lengths = series.astype(str).str.len()

        return {
            'min_length': str_lengths.min(),
            'max_length': str_lengths.max(),
            'avg_length': round(str_lengths.mean(), 2),
            'most_common': series.mode().iloc[0] if len(series.mode()) > 0 else None,
        }

    def _analyze_date_column(self, series: pd.Series) -> Dict[str, Any]:
        """Analyze date column."""
        try:
            # Try to parse dates
            date_series = pd.to_datetime(series, errors='coerce', infer_datetime_format=True)
            date_series = date_series.dropna()

            if len(date_series) > 0:
                return {
                    'min_date': date_series.min(),
                    'max_date': date_series.max(),
                    'date_range_days': (date_series.max() - date_series.min()).days,
                }
        except Exception:
            pass

        return {}

    def _analyze_boolean_column(self, series: pd.Series) -> Dict[str, Any]:
        """Analyze boolean column."""
        value_counts = series.value_counts()

        return {
            'true_count': value_counts.get('true', 0) + value_counts.get('1', 0) + value_counts.get('yes', 0),
            'false_count': value_counts.get('false', 0) + value_counts.get('0', 0) + value_counts.get('no', 0),
            'unique_values': value_counts.index.tolist(),
        }
