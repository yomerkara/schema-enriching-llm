import pandas as pd
import numpy as np
import json
import sqlite3
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import random
import string
from faker import Faker


class MultiFormatProcessor:
    """Enhanced processor supporting multiple data source types."""

    def __init__(self):
        self.faker = Faker()
        self.supported_formats = [
            'csv', 'excel', 'json', 'parquet', 'database', 'api_schema'
        ]

    def process_csv(self, uploaded_file) -> Dict[str, Any]:
        """Process CSV file with enhanced analysis."""
        from csv_processor import CSVProcessor

        processor = CSVProcessor()
        data = processor.process_file(uploaded_file)
        schema = processor.infer_schema(data)

        # Add enhanced metadata
        enhanced_schema = self._enhance_basic_schema(schema, data)

        return {
            'data': data,
            'schema': enhanced_schema,
            'source_type': 'csv',
            'metadata': {
                'file_name': uploaded_file.name,
                'file_size': uploaded_file.size,
                'processed_at': datetime.now().isoformat()
            }
        }

    def generate_sample_ecommerce_schema(self,size: int = 10000) -> Dict[str, Any]:
        """Generate realistic e-commerce database schema for POC."""

        # Create sample tables that represent typical legacy e-commerce system
        tables_data = {
            'customers': self._generate_customer_data(size),
            'orders': self._generate_order_data(size),
            'products': self._generate_product_data(size // 2),
            'order_items': self._generate_order_items_data(size)
        }

        # For POC, we'll use the customers table as main example
        data = tables_data['customers']
        schema = self._infer_schema_from_sample_data(data, 'customers')

        return {
            'data': data,
            'schema': schema,
            'source_type': 'database',
            'all_tables': tables_data,
            'metadata': {
                'database_type': 'Legacy Oracle',
                'schema_name': 'ECOMM_PROD',
                'table_count': len(tables_data),
                'generated_at': datetime.now().isoformat()
            }
        }

    def process_json_schema(self, json_input: str) -> Dict[str, Any]:
        """Process JSON schema definition OR actual JSON data."""
        try:
            parsed_json = json.loads(json_input)

            if isinstance(parsed_json, list):
                # Handle actual JSON data (array of objects)
                if len(parsed_json) > 0:
                    data = pd.DataFrame(parsed_json)
                    schema = self._infer_schema_from_sample_data(data, 'json_data')

                    return {
                        'data': data,
                        'schema': schema,
                        'source_type': 'json_data',
                        'metadata': {
                            'record_count': len(parsed_json),
                            'source_format': 'JSON Array',
                            'processed_at': datetime.now().isoformat()
                        }
                    }
                else:
                    raise ValueError("Empty JSON array provided")

            elif isinstance(parsed_json, dict):
                # Check if it's a schema definition or single object
                if self._is_schema_definition(parsed_json):
                    # Handle schema definition
                    data = self._generate_data_from_json_schema(parsed_json)
                    schema = self._infer_schema_from_sample_data(data, 'json_schema')

                    return {
                        'data': data,
                        'schema': schema,
                        'source_type': 'json_schema',
                        'metadata': {
                            'schema_definition': parsed_json,
                            'processed_at': datetime.now().isoformat()
                        }
                    }
                else:
                    # Handle single JSON object
                    data = pd.DataFrame([parsed_json])
                    schema = self._infer_schema_from_sample_data(data, 'json_object')

                    return {
                        'data': data,
                        'schema': schema,
                        'source_type': 'json_object',
                        'metadata': {
                            'record_count': 1,
                            'processed_at': datetime.now().isoformat()
                        }
                    }
            else:
                raise ValueError("Unsupported JSON format")

        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON: {str(e)}")

    def _is_schema_definition(self, json_obj: dict) -> bool:
        """Determine if JSON object is a schema definition or actual data."""
        # Schema definitions typically have simple string values indicating types
        schema_types = ['string', 'integer', 'float', 'boolean', 'date', 'datetime']

        values = list(json_obj.values())
        # If most values are type definitions, it's likely a schema
        type_matches = sum(1 for v in values if isinstance(v, str) and v.lower() in schema_types)

        return type_matches > len(values) * 0.7  # 70% threshold

    def generate_sample_data(self, sample_type: str, size: int = 10000) -> Dict[str, Any]:
        """Generate industry-specific sample datasets."""

        generators = {
            'Financial Transactions': self._generate_financial_data,
            'Customer Records': self._generate_customer_data,
            'Product Catalog': self._generate_product_data,
            'Healthcare Records': self._generate_healthcare_data,
            'Manufacturing Data': self._generate_manufacturing_data,
            # ADD THESE 3 NEW LINES:
            'OTA Booking Data': self._generate_ota_booking_data,
            'Hotel Property Data': self._generate_hotel_property_data,
            'Travel Search Data': self._generate_travel_search_data
        }

        if sample_type not in generators:
            raise ValueError(f"Unsupported sample type: {sample_type}")

        data = generators[sample_type](size)
        schema = self._infer_schema_from_sample_data(data, sample_type.lower().replace(' ', '_'))

        return {
            'data': data,
            'schema': schema,
            'source_type': 'sample_data',
            'metadata': {
                'sample_type': sample_type,
                'record_count': len(data),
                'generated_at': datetime.now().isoformat()
            }
        }

    def _enhance_basic_schema(self, schema: List[Dict], data: pd.DataFrame) -> List[Dict]:
        """Add enhanced metadata to basic schema."""
        enhanced = []

        for col_info in schema:
            enhanced_col = col_info.copy()

            # Add business context hints
            enhanced_col['potential_pii'] = self._detect_pii(
                col_info['column_name'],
                col_info.get('sample_values', [])
            )

            enhanced_col['potential_business_key'] = self._detect_business_key(
                col_info['column_name'],
                col_info.get('unique_count', 0),
                len(data)
            )

            enhanced_col['data_pattern'] = self._detect_data_patterns(
                col_info.get('sample_values', [])
            )

            enhanced.append(enhanced_col)

        return enhanced

    def _generate_customer_data(self, n: int) -> pd.DataFrame:
        """Generate realistic customer data with legacy naming conventions."""
        data = []

        for i in range(n):
            # Use cryptic legacy column names that need enhancement
            record = {
                'CUST_ID_NBR': f'C{i + 1:06d}',
                'CUST_FNAME': self.faker.first_name(),
                'CUST_LNAME': self.faker.last_name(),
                'CUST_EMAIL_ADDR': self.faker.email(),
                'CUST_PHONE_NBR': self.faker.phone_number(),
                'CUST_ADDR_LINE1': self.faker.street_address(),
                'CUST_CITY_NM': self.faker.city(),
                'CUST_STATE_CD': self.faker.state_abbr(),
                'CUST_ZIP_CD': self.faker.zipcode(),
                'CUST_COUNTRY_CD': 'US',
                'CUST_BIRTH_DT': self.faker.date_of_birth(minimum_age=18, maximum_age=80),
                'CUST_GENDER_CD': random.choice(['M', 'F', 'O']),
                'CUST_STATUS_CD': random.choice(['A', 'I', 'S']),  # Active, Inactive, Suspended
                'CUST_SEGMENT_CD': random.choice(['PREM', 'GOLD', 'SILV', 'BRNZ']),
                'CUST_REG_DT': self.faker.date_between(start_date='-5y', end_date='today'),
                'CUST_LAST_LOGIN_DT': self.faker.date_between(start_date='-30d', end_date='today'),
                'CUST_LIFETIME_VAL_AMT': round(random.uniform(100, 50000), 2),
                'CUST_RISK_SCORE_NBR': random.randint(1, 100),
                'CUST_PREF_CONTACT_CD': random.choice(['EMAIL', 'PHONE', 'MAIL']),
                'CUST_MARKETING_OPT_FLG': random.choice(['Y', 'N']),
            }
            data.append(record)

        return pd.DataFrame(data)

    def _generate_order_data(self, n: int) -> pd.DataFrame:
        """Generate order data with legacy conventions."""
        data = []

        for i in range(n):
            order_date = self.faker.date_between(start_date='-2y', end_date='today')
            record = {
                'ORDER_ID_NBR': f'ORD{i + 1:08d}',
                'CUST_ID_NBR': f'C{random.randint(1, 1000):06d}',
                'ORDER_DT': order_date,
                'ORDER_STATUS_CD': random.choice(['PEND', 'CONF', 'SHIP', 'DLVR', 'CANC']),
                'ORDER_TOTAL_AMT': round(random.uniform(10, 2000), 2),
                'ORDER_TAX_AMT': round(random.uniform(1, 200), 2),
                'ORDER_SHIP_AMT': round(random.uniform(0, 50), 2),
                'ORDER_DISCOUNT_AMT': round(random.uniform(0, 100), 2),
                'PAYMENT_METHOD_CD': random.choice(['CC', 'PP', 'BT', 'COD']),
                'SHIP_METHOD_CD': random.choice(['STD', 'EXP', 'OVN', 'PU']),
                'SHIP_ADDR_LINE1': self.faker.street_address(),
                'SHIP_CITY_NM': self.faker.city(),
                'SHIP_STATE_CD': self.faker.state_abbr(),
                'SHIP_ZIP_CD': self.faker.zipcode(),
                'ORDER_CHANNEL_CD': random.choice(['WEB', 'MOB', 'STORE', 'PHONE']),
            }
            data.append(record)

        return pd.DataFrame(data)

    def _generate_product_data(self, n: int) -> pd.DataFrame:
        """Generate product catalog data."""
        categories = ['ELEC', 'CLTH', 'HOME', 'BOOK', 'TOYS', 'SPRT']
        data = []

        for i in range(n):
            record = {
                'PROD_ID_NBR': f'P{i + 1:06d}',
                'PROD_SKU_CD': f'SKU{random.randint(100000, 999999)}',
                'PROD_NAME_TXT': self.faker.catch_phrase(),
                'PROD_DESC_TXT': self.faker.text(max_nb_chars=200),
                'PROD_CAT_CD': random.choice(categories),
                'PROD_SUBCAT_CD': f'{random.choice(categories)}{random.randint(10, 99)}',
                'PROD_BRAND_NM': self.faker.company(),
                'PROD_PRICE_AMT': round(random.uniform(5, 500), 2),
                'PROD_COST_AMT': round(random.uniform(2, 250), 2),
                'PROD_WEIGHT_NBR': round(random.uniform(0.1, 50), 2),
                'PROD_STATUS_CD': random.choice(['A', 'D', 'O']),  # Active, Discontinued, Out of Stock
                'PROD_INVENTORY_QTY': random.randint(0, 1000),
                'PROD_LAUNCH_DT': self.faker.date_between(start_date='-5y', end_date='today'),
                'PROD_RATING_NBR': round(random.uniform(1, 5), 1),
                'PROD_REVIEW_CNT': random.randint(0, 500),
                'PROD_VENDOR_ID_NBR': f'V{random.randint(1, 100):03d}',
            }
            data.append(record)

        return pd.DataFrame(data)

    def _generate_order_items_data(self, n: int) -> pd.DataFrame:
        """Generate order items data."""
        data = []

        for i in range(n):
            record = {
                'ORDER_ITEM_ID_NBR': f'OI{i + 1:08d}',
                'ORDER_ID_NBR': f'ORD{random.randint(1, 2500):08d}',
                'PROD_ID_NBR': f'P{random.randint(1, 500):06d}',
                'ITEM_QTY_NBR': random.randint(1, 10),
                'ITEM_UNIT_PRICE_AMT': round(random.uniform(5, 500), 2),
                'ITEM_TOTAL_AMT': 0,  # Will be calculated
                'ITEM_DISCOUNT_AMT': round(random.uniform(0, 50), 2),
                'ITEM_STATUS_CD': random.choice(['ORD', 'SHIP', 'DLVR', 'RETN']),
            }
            # Calculate total
            record['ITEM_TOTAL_AMT'] = round(
                record['ITEM_QTY_NBR'] * record['ITEM_UNIT_PRICE_AMT'] - record['ITEM_DISCOUNT_AMT'], 2
            )
            data.append(record)

        return pd.DataFrame(data)

    def _generate_financial_data(self, n: int) -> pd.DataFrame:
        """Generate financial transaction data."""
        data = []

        for i in range(n):
            record = {
                'TXN_ID_NBR': f'TXN{i + 1:010d}',
                'ACCT_NBR': f'{random.randint(1000000000, 9999999999)}',
                'TXN_DT': self.faker.date_between(start_date='-1y', end_date='today'),
                'TXN_TS': self.faker.date_time_between(start_date='-1y', end_date='now'),
                'TXN_TYPE_CD': random.choice(['DEP', 'WTH', 'TRF', 'FEE', 'INT', 'CHG']),
                'TXN_AMT': round(random.uniform(-5000, 10000), 2),
                'TXN_DESC_TXT': self.faker.sentence(nb_words=4),
                'TXN_CATEGORY_CD': random.choice(['FOOD', 'GAS', 'SHOP', 'BILL', 'ENTM', 'MISC']),
                'TXN_CHANNEL_CD': random.choice(['ATM', 'ONL', 'MOB', 'BRANCH', 'POS']),
                'TXN_STATUS_CD': random.choice(['COMP', 'PEND', 'FAIL', 'VOID']),
                'MERCHANT_NM': self.faker.company(),
                'MERCHANT_CAT_CD': f'MCC{random.randint(1000, 9999)}',
                'TXN_CURRENCY_CD': 'USD',
                'TXN_EXCHANGE_RT': 1.0,
                'FRAUD_SCORE_NBR': random.randint(0, 100),
                'TXN_LOCATION_CD': self.faker.state_abbr(),
            }
            data.append(record)

        return pd.DataFrame(data)

    def _generate_healthcare_data(self, n: int) -> pd.DataFrame:
        """Generate healthcare records (anonymized)."""
        data = []

        for i in range(n):
            record = {
                'PATIENT_ID_NBR': f'PT{i + 1:08d}',
                'VISIT_ID_NBR': f'V{i + 1:010d}',
                'VISIT_DT': self.faker.date_between(start_date='-2y', end_date='today'),
                'PATIENT_AGE_NBR': random.randint(1, 100),
                'PATIENT_GENDER_CD': random.choice(['M', 'F', 'O']),
                'DIAGNOSIS_CD': f'ICD{random.randint(10, 99)}.{random.randint(100, 999)}',
                'PROCEDURE_CD': f'CPT{random.randint(10000, 99999)}',
                'PROVIDER_ID_NBR': f'PR{random.randint(1, 1000):04d}',
                'DEPARTMENT_CD': random.choice(['ER', 'ICU', 'SURG', 'CARD', 'ONCO', 'PEDI']),
                'ADMISSION_TYPE_CD': random.choice(['EMER', 'ELEC', 'URGENT', 'OUTPT']),
                'LENGTH_OF_STAY_NBR': random.randint(0, 30),
                'DISCHARGE_STATUS_CD': random.choice(['HOME', 'TRANSFER', 'AMA', 'EXPIRED']),
                'TOTAL_CHARGES_AMT': round(random.uniform(100, 50000), 2),
                'INSURANCE_TYPE_CD': random.choice(['PRIV', 'MCARE', 'MCAID', 'SELF']),
                'SEVERITY_SCORE_NBR': random.randint(1, 10),
            }
            data.append(record)

        return pd.DataFrame(data)

    def _generate_manufacturing_data(self, n: int) -> pd.DataFrame:
        """Generate manufacturing/production data."""
        data = []

        for i in range(n):
            record = {
                'BATCH_ID_NBR': f'B{i + 1:08d}',
                'PROD_LINE_CD': random.choice(['LINE01', 'LINE02', 'LINE03', 'LINE04']),
                'SHIFT_CD': random.choice(['DAY', 'SWING', 'NIGHT']),
                'PROD_DT': self.faker.date_between(start_date='-6m', end_date='today'),
                'PART_NBR': f'PN{random.randint(100000, 999999)}',
                'QTY_PRODUCED_NBR': random.randint(50, 1000),
                'QTY_DEFECTIVE_NBR': random.randint(0, 50),
                'CYCLE_TIME_MIN': round(random.uniform(10, 120), 2),
                'TEMPERATURE_F': round(random.uniform(150, 300), 1),
                'PRESSURE_PSI': round(random.uniform(20, 100), 1),
                'HUMIDITY_PCT': round(random.uniform(30, 70), 1),
                'OPERATOR_ID_NBR': f'OP{random.randint(1, 100):03d}',
                'MACHINE_ID_NBR': f'M{random.randint(1, 50):03d}',
                'QUALITY_GRADE_CD': random.choice(['A', 'B', 'C', 'REJECT']),
                'DOWNTIME_MIN': random.randint(0, 60),
                'MATERIAL_COST_AMT': round(random.uniform(50, 500), 2),
            }
            data.append(record)

        return pd.DataFrame(data)

    def _generate_data_from_json_schema(self, schema_def: Dict) -> pd.DataFrame:
        """Generate sample data from JSON schema definition."""
        data = []

        # Simple JSON schema support for POC
        for i in range(100):
            record = {}
            for field, field_type in schema_def.items():
                if field_type == 'string':
                    record[field] = self.faker.word()
                elif field_type == 'integer':
                    record[field] = random.randint(1, 1000)
                elif field_type == 'float':
                    record[field] = round(random.uniform(1, 1000), 2)
                elif field_type == 'boolean':
                    record[field] = random.choice([True, False])
                elif field_type == 'date':
                    record[field] = self.faker.date()
                else:
                    record[field] = self.faker.word()
            data.append(record)

        return pd.DataFrame(data)

    def _infer_schema_from_sample_data(self, data: pd.DataFrame, table_name: str) -> List[Dict]:
        """Infer schema from generated sample data."""
        from csv_processor import CSVProcessor

        processor = CSVProcessor()
        schema = processor.infer_schema(data)

        # Add table context
        for col in schema:
            col['table_name'] = table_name
            col['source_system'] = 'Legacy System'

        return self._enhance_basic_schema(schema, data)

    def _detect_pii(self, column_name: str, sample_values: List[str]) -> bool:
        """Detect potential PII fields."""
        pii_keywords = [
            'email', 'phone', 'ssn', 'social', 'name', 'fname', 'lname',
            'address', 'addr', 'birth', 'dob', 'license', 'passport'
        ]

        column_lower = column_name.lower()
        return any(keyword in column_lower for keyword in pii_keywords)

    def _detect_business_key(self, column_name: str, unique_count: int, total_count: int) -> bool:
        """Detect potential business keys."""
        key_keywords = ['id', 'key', 'nbr', 'code', 'cd']
        uniqueness_ratio = unique_count / total_count if total_count > 0 else 0

        column_lower = column_name.lower()
        has_key_pattern = any(keyword in column_lower for keyword in key_keywords)
        is_highly_unique = uniqueness_ratio > 0.9

        return has_key_pattern and is_highly_unique

    def _detect_data_patterns(self, sample_values: List[str]) -> str:
        """Detect common data patterns."""
        if not sample_values:
            return 'unknown'

        # Convert to strings and check patterns
        str_values = [str(v) for v in sample_values[:10]]

        # Check for common patterns
        if all(v.upper() in ['Y', 'N', 'YES', 'NO', 'TRUE', 'FALSE', '1', '0'] for v in str_values):
            return 'boolean_flag'
        elif all(len(v) == len(str_values[0]) and v.isupper() for v in str_values):
            return 'fixed_code'
        elif all('@' in v for v in str_values):
            return 'email_address'
        elif all(v.replace('-', '').replace('(', '').replace(')', '').replace(' ', '').isdigit() for v in str_values):
            return 'phone_number'
        elif all(len(v.replace('-', '')) in [9, 11] and v.replace('-', '').isdigit() for v in str_values):
            return 'ssn_or_id'
        else:
            return 'general_text'

    def _generate_ota_booking_data(self, n: int) -> pd.DataFrame:
        """Generate realistic OTA booking data with legacy naming conventions."""
        data = []

        booking_statuses = ['CONF', 'PEND', 'CANC', 'NOSH', 'AMND']
        property_types = ['HOTEL', 'APART', 'B&B', 'HOSTEL', 'VILLA', 'RESORT']
        channels = ['DIRECT', 'OTA', 'GDS', 'AGENT', 'MOBILE', 'API']
        cancellation_policies = ['FREE', 'NONREF', 'PARTIAL', 'FLEXI']

        for i in range(n):
            checkin_date = self.faker.date_between(start_date='-6m', end_date='+6m')
            checkout_date = checkin_date + timedelta(days=random.randint(1, 14))
            booking_date = checkin_date - timedelta(days=random.randint(1, 90))

            record = {
                'BKNG_ID_NBR': f'BK{i + 1:010d}',
                'PROP_ID_NBR': f'PROP{random.randint(1, 10000):06d}',
                'GUEST_ID_NBR': f'G{random.randint(1, 50000):08d}',
                'BKNG_REF_CD': f'REF{random.randint(100000, 999999)}',
                'BKNG_DT': booking_date,
                'CHECKIN_DT': checkin_date,
                'CHECKOUT_DT': checkout_date,
                'NIGHTS_CNT': (checkout_date - checkin_date).days,
                'ADULTS_CNT': random.randint(1, 4),
                'CHILDREN_CNT': random.randint(0, 3),
                'ROOMS_CNT': random.randint(1, 3),
                'ROOM_TYPE_CD': random.choice(['STD', 'DLX', 'STE', 'FAM', 'TWIN', 'KING']),
                'BKNG_STATUS_CD': random.choice(booking_statuses),
                'CHANNEL_CD': random.choice(channels),
                'PROP_TYPE_CD': random.choice(property_types),
                'CURRENCY_CD': random.choice(['EUR', 'USD', 'GBP', 'AUD', 'CAD']),
                'TOTAL_AMT': round(random.uniform(50, 2000), 2),
                'COMMISSION_AMT': round(random.uniform(5, 300), 2),
                'COMMISSION_PCT': round(random.uniform(8, 25), 2),
                'CANC_POLICY_CD': random.choice(cancellation_policies),
                'GUEST_EMAIL_ADDR': self.faker.email(),
                'GUEST_PHONE_NBR': self.faker.phone_number(),
                'GUEST_COUNTRY_CD': self.faker.country_code(),
                'PAYMENT_METHOD_CD': random.choice(['CC', 'PAYPAL', 'BANK', 'CRYPTO']),
                'LOYALTY_MEMBER_FLG': random.choice(['Y', 'N']),
                'FRAUD_SCORE_NBR': random.randint(0, 100),
                # Add more fields as needed...
            }
            data.append(record)

        return pd.DataFrame(data)

    def _generate_hotel_property_data(self, n: int) -> pd.DataFrame:
        """Generate hotel property data for OTA systems."""
        data = []

        property_types = ['HOTEL', 'APART', 'B&B', 'HOSTEL', 'VILLA', 'RESORT']
        star_ratings = [1, 2, 3, 4, 5]

        for i in range(n):
            record = {
                'PROP_ID_NBR': f'PROP{i + 1:06d}',
                'PROP_NAME_TXT': f"{self.faker.company()} {random.choice(['Hotel', 'Resort', 'Inn'])}",
                'PROP_TYPE_CD': random.choice(property_types),
                'STAR_RATING_NBR': random.choice(star_ratings),
                'CITY_NM': self.faker.city(),
                'COUNTRY_CD': self.faker.country_code(),
                'LATITUDE_NBR': float(self.faker.latitude()),
                'LONGITUDE_NBR': float(self.faker.longitude()),
                'TOTAL_ROOMS_CNT': random.randint(10, 500),
                'COMMISSION_PCT': round(random.uniform(10, 25), 2),
                'GUEST_REVIEW_SCORE': round(random.uniform(6.0, 9.5), 1),
                'ACTIVE_STATUS_FLG': random.choice(['Y', 'N']),
                # Add more property fields...
            }
            data.append(record)

        return pd.DataFrame(data)

    def _generate_travel_search_data(self, n: int) -> pd.DataFrame:
        """Generate travel search and user behavior data."""
        data = []

        devices = ['DESKTOP', 'MOBILE', 'TABLET']
        search_types = ['CITY', 'PROPERTY', 'REGION', 'LANDMARK']

        for i in range(n):
            search_date = self.faker.date_between(start_date='-3m', end_date='now')
            checkin_date = search_date + timedelta(days=random.randint(1, 180))

            record = {
                'SEARCH_ID_NBR': f'SRCH{i + 1:010d}',
                'SESSION_ID_TXT': f'SESS{random.randint(100000000, 999999999)}',
                'SEARCH_TS': self.faker.date_time_between(start_date=search_date, end_date=search_date),
                'DESTINATION_TXT': self.faker.city(),
                'CHECKIN_DT': checkin_date,
                'CHECKOUT_DT': checkin_date + timedelta(days=random.randint(1, 14)),
                'DEVICE_TYPE_CD': random.choice(devices),
                'CONVERSION_FLG': random.choice(['Y', 'N']),
                'CLICKS_CNT': random.randint(0, 20),
                # Add more search behavior fields...
            }
            data.append(record)

        return pd.DataFrame(data)

    def generate_sample_ota_schema(self,size: int = 10000) -> Dict[str, Any]:
        """Generate realistic OTA database schema for POC."""

        # Create sample tables representing typical OTA system
        tables_data = {
            'bookings': self._generate_ota_booking_data(size),
            'properties': self._generate_hotel_property_data(size // 5),
            'searches': self._generate_travel_search_data(size),
            'guests': self._generate_customer_data(size)
        }

        # Use bookings table as main example
        data = tables_data['bookings']
        schema = self._infer_schema_from_sample_data(data, 'bookings')

        return {
            'data': data,
            'schema': schema,
            'source_type': 'ota_database',
            'all_tables': tables_data,
            'metadata': {
                'database_type': 'Legacy OTA System',
                'schema_name': 'OTA_PROD',
                'table_count': len(tables_data),
                'generated_at': datetime.now().isoformat(),
                'industry': 'Online Travel Agency'
            }
        }