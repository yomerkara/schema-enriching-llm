from typing import Dict, List, Any
import re


class BusinessContextEngine:
    """Provides industry-specific business context for schema analysis."""

    def __init__(self, industry: str = "General"):
        self.industry = industry
        self.business_glossaries = self._load_business_glossaries()
        self.compliance_frameworks = self._load_compliance_frameworks()
        self.naming_patterns = self._load_naming_patterns()

    def add_business_context(self, schema: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Add business context to schema based on industry knowledge."""
        enhanced_schema = []

        for column in schema:
            enhanced_column = column.copy()

            # Add business context
            enhanced_column['business_context'] = self._infer_business_context(column)
            enhanced_column['compliance_implications'] = self._identify_compliance_needs(column)
            enhanced_column['business_criticality'] = self._assess_business_criticality(column)
            enhanced_column['suggested_business_rules'] = self._suggest_business_rules(column)

            enhanced_schema.append(enhanced_column)

        return enhanced_schema

    def _load_business_glossaries(self) -> Dict[str, Dict]:
        """Load industry-specific business glossaries."""
        return {
            "Financial Services": {
                "account": "Financial account holder record",
                "transaction": "Financial transaction or payment",
                "balance": "Account balance or available funds",
                "credit": "Credit-related information or transactions",
                "debit": "Debit transactions or charges",
                "risk": "Risk assessment or credit risk scoring",
                "kyc": "Know Your Customer compliance data",
                "aml": "Anti-Money Laundering related fields",
                "routing": "Bank routing number or payment routing",
                "swift": "SWIFT code for international transfers"
            },
            "Healthcare": {
                "patient": "Patient demographic or medical information",
                "diagnosis": "Medical diagnosis codes (ICD-10)",
                "procedure": "Medical procedures (CPT codes)",
                "provider": "Healthcare provider information",
                "claim": "Insurance claim or billing information",
                "phi": "Protected Health Information",
                "hipaa": "HIPAA compliance related data",
                "medication": "Prescription or medication data",
                "allergy": "Patient allergy information",
                "vital": "Vital signs measurements"
            },
            "Retail/E-commerce": {
                "customer": "Customer profile and demographic data",
                "order": "Purchase order information",
                "product": "Product catalog and inventory data",
                "inventory": "Stock levels and warehouse data",
                "sales": "Sales performance and revenue data",
                "cart": "Shopping cart and session data",
                "payment": "Payment processing information",
                "shipping": "Shipping and fulfillment data",
                "return": "Return and refund processing",
                "loyalty": "Customer loyalty program data",
                "promotion": "Marketing campaigns and promotions"
            },
            "Online Travel Agency (OTA)": {
                "booking": "Reservation or booking transaction record",
                "reservation": "Hotel/accommodation reservation details",
                "accommodation": "Property or hotel listing information",
                "property": "Hotel, apartment, or rental property details",
                "guest": "Traveler or guest profile and preferences",
                "stay": "Actual stay period and check-in/out details",
                "cancellation": "Booking cancellation and refund information",
                "rate": "Room rates, pricing, and availability data",
                "availability": "Property availability and capacity management",
                "commission": "Partner commission and revenue sharing",
                "inventory": "Room inventory and allocation management",
                "channel": "Distribution channel (direct, OTA, GDS)",
                "payment": "Payment processing and fraud detection",
                "review": "Guest reviews and property ratings",
                "loyalty": "Guest loyalty program and points",
                "destination": "Travel destination and location data",
                "amenity": "Property amenities and facilities",
                "policy": "Cancellation, payment, and booking policies",
                "search": "Search queries and user behavior",
                "conversion": "Booking funnel and conversion tracking",
                "revenue": "Revenue management and pricing optimization",
                "competitor": "Competitive pricing and market analysis",
                "fraud": "Fraud detection and prevention data",
                "partner": "Hotel partner and supplier information",
                "contract": "Partner contracts and rate agreements"
            },
            "Manufacturing": {
                "production": "Manufacturing production data",
                "quality": "Quality control and assurance metrics",
                "equipment": "Manufacturing equipment and machinery",
                "batch": "Production batch tracking",
                "material": "Raw materials and supply chain",
                "operator": "Production line operator data",
                "downtime": "Equipment downtime tracking",
                "yield": "Production yield and efficiency",
                "safety": "Workplace safety incidents",
                "maintenance": "Equipment maintenance records"
            }
        }

    def _load_compliance_frameworks(self) -> Dict[str, List[str]]:
        """Load compliance frameworks by industry."""
        return {
            "Financial Services": [
                "PCI DSS - Payment Card Industry Data Security Standard",
                "SOX - Sarbanes-Oxley Act compliance",
                "GDPR - General Data Protection Regulation",
                "CCPA - California Consumer Privacy Act",
                "KYC - Know Your Customer requirements",
                "AML - Anti-Money Laundering regulations",
                "BASEL III - Banking regulatory framework"
            ],
            "Healthcare": [
                "HIPAA - Health Insurance Portability and Accountability Act",
                "HITECH - Health Information Technology for Economic and Clinical Health",
                "FDA 21 CFR Part 11 - Electronic records compliance",
                "GDPR - General Data Protection Regulation",
                "State privacy laws - Various state healthcare privacy requirements"
            ],
            "Retail/E-commerce": [
                "PCI DSS - Payment Card Industry Data Security Standard",
                "GDPR - General Data Protection Regulation",
                "CCPA - California Consumer Privacy Act",
                "COPPA - Children's Online Privacy Protection Act",
                "CAN-SPAM Act - Email marketing compliance",
                "FTC Act - Federal Trade Commission consumer protection"
            ],
            "Online Travel Agency (OTA)": [
                "GDPR - General Data Protection Regulation (critical for EU travelers)",
                "PCI DSS - Payment Card Industry Data Security Standard",
                "CCPA - California Consumer Privacy Act",
                "Data Localization Laws - Various country-specific requirements",
                "Consumer Protection Laws - Travel-specific regulations",
                "Tourism Industry Regulations - Local tourism board requirements",
                "Anti-Money Laundering (AML) - For high-value bookings",
                "Accessibility Laws - Website accessibility compliance",
                "Package Travel Directive - EU travel package regulations",
                "Price Transparency Laws - Display of total costs and fees",
                "Cooling-off Period Laws - Consumer right to cancel",
                "Force Majeure Regulations - COVID-19 and emergency cancellations"
            ],
            "General": [
                "GDPR - General Data Protection Regulation",
                "SOC 2 - Service Organization Control 2",
                "ISO 27001 - Information security management"
            ]
        }

    def _load_naming_patterns(self) -> Dict[str, Dict]:
        """Load common naming patterns by industry."""
        return {
            "Financial Services": {
                "account_patterns": ["acct", "account", "acc"],
                "transaction_patterns": ["txn", "trans", "transaction"],
                "amount_patterns": ["amt", "amount", "value", "val"],
                "date_patterns": ["dt", "date", "time", "ts"],
                "code_patterns": ["cd", "code", "type", "status"]
            },
            "Healthcare": {
                "patient_patterns": ["pt", "patient", "pat"],
                "medical_patterns": ["dx", "diagnosis", "proc", "procedure"],
                "provider_patterns": ["prov", "provider", "dr", "physician"],
                "date_patterns": ["dt", "date", "time", "dos"],
                "code_patterns": ["cd", "code", "icd", "cpt"]
            },
            "Retail/E-commerce": {
                "customer_patterns": ["cust", "customer", "client"],
                "product_patterns": ["prod", "product", "item", "sku"],
                "order_patterns": ["ord", "order", "purchase"],
                "quantity_patterns": ["qty", "quantity", "count", "nbr"],
                "price_patterns": ["price", "cost", "amt", "amount"]
            },
            "Online Travel Agency (OTA)": {
                "booking_patterns": ["bkng", "booking", "reservation", "res"],
                "property_patterns": ["prop", "property", "hotel", "accom"],
                "guest_patterns": ["guest", "traveler", "customer", "cust"],
                "rate_patterns": ["rate", "price", "tariff", "amt"],
                "date_patterns": ["dt", "date", "checkin", "checkout"],
                "status_patterns": ["status", "state", "cd", "flg"],
                "location_patterns": ["dest", "destination", "city", "country"],
                "revenue_patterns": ["commission", "revenue", "margin", "profit"]
            }
        }

    def _infer_business_context(self, column: Dict[str, Any]) -> str:
        """Infer business context from column information."""
        column_name = column['column_name'].lower()
        sample_values = column.get('sample_values', [])

        # Get industry-specific glossary
        glossary = self.business_glossaries.get(self.industry, {})

        # Check for direct matches
        for term, description in glossary.items():
            if term in column_name:
                return f"{description} (Industry: {self.industry})"

        # Check for common business patterns
        if any(pattern in column_name for pattern in ['id', 'key', 'nbr', 'number']):
            if any(pattern in column_name for pattern in ['cust', 'customer', 'client', 'guest']):
                return "Customer/Guest identifier for business operations"
            elif any(pattern in column_name for pattern in ['prod', 'product', 'item', 'prop', 'property']):
                return "Product/Property identifier for inventory management"
            elif any(pattern in column_name for pattern in ['ord', 'order', 'trans', 'bkng', 'booking']):
                return "Transaction/Booking identifier for order processing"
            else:
                return "Business identifier for operational tracking"

        elif any(pattern in column_name for pattern in ['amt', 'amount', 'price', 'cost', 'value', 'rate']):
            if self.industry == "Online Travel Agency (OTA)":
                return "Financial amount for booking transactions and revenue management"
            else:
                return "Financial amount for business calculations and reporting"

        elif any(pattern in column_name for pattern in ['dt', 'date', 'time', 'ts']):
            if self.industry == "Online Travel Agency (OTA)" and any(
                    pattern in column_name for pattern in ['checkin', 'checkout']):
                return "Travel date for booking and stay management"
            else:
                return "Date/time field for temporal business analysis"

        elif any(pattern in column_name for pattern in ['status', 'state', 'flag', 'ind']):
            return "Status indicator for business process tracking"

        elif any(pattern in column_name for pattern in ['name', 'desc', 'description']):
            return "Descriptive text field for business identification"

        # Industry-specific patterns
        elif self.industry == "Online Travel Agency (OTA)":
            if any(pattern in column_name for pattern in ['commission', 'margin']):
                return "Revenue sharing and partner commission data"
            elif any(pattern in column_name for pattern in ['cancel', 'refund']):
                return "Cancellation and refund processing information"
            elif any(pattern in column_name for pattern in ['review', 'rating', 'score']):
                return "Guest feedback and property quality metrics"
            elif any(pattern in column_name for pattern in ['search', 'click', 'conversion']):
                return "User behavior and booking funnel analytics"

        return f"Business data field relevant to {self.industry} operations"

    def _identify_compliance_needs(self, column: Dict[str, Any]) -> List[str]:
        """Identify compliance requirements for the column."""
        compliance_needs = []
        column_name = column['column_name'].lower()

        # PII Detection
        pii_patterns = [
            'name', 'fname', 'lname', 'email', 'phone', 'address', 'addr',
            'ssn', 'social', 'birth', 'dob', 'license', 'passport'
        ]

        if any(pattern in column_name for pattern in pii_patterns):
            compliance_needs.extend([
                "GDPR Article 6 - Lawful basis for processing personal data",
                "Data encryption at rest and in transit required",
                "Access logging and audit trail implementation"
            ])

        # Industry-specific compliance
        industry_compliance = self.compliance_frameworks.get(self.industry, [])

        if self.industry == "Financial Services":
            if any(pattern in column_name for pattern in ['account', 'balance', 'payment', 'card']):
                compliance_needs.extend([
                    "PCI DSS - Secure storage of cardholder data",
                    "SOX - Financial reporting accuracy requirements"
                ])

        elif self.industry == "Healthcare":
            if any(pattern in column_name for pattern in ['patient', 'medical', 'diagnosis', 'procedure']):
                compliance_needs.extend([
                    "HIPAA - Protected Health Information (PHI) safeguards",
                    "Minimum necessary standard for data access"
                ])

        elif self.industry == "Retail/E-commerce":
            if any(pattern in column_name for pattern in ['payment', 'card', 'billing']):
                compliance_needs.append("PCI DSS - Payment processing security")

        elif self.industry == "Online Travel Agency (OTA)":
            if any(pattern in column_name for pattern in ['guest', 'customer', 'traveler']):
                compliance_needs.extend([
                    "GDPR - EU traveler data protection requirements",
                    "Data localization - Country-specific data residency rules"
                ])
            if any(pattern in column_name for pattern in ['payment', 'card', 'billing']):
                compliance_needs.append("PCI DSS - Payment processing for travel bookings")
            if any(pattern in column_name for pattern in ['booking', 'reservation', 'cancellation']):
                compliance_needs.extend([
                    "Package Travel Directive - EU booking protection",
                    "Consumer protection laws - Booking terms transparency"
                ])

        return compliance_needs

    def _assess_business_criticality(self, column: Dict[str, Any]) -> str:
        """Assess business criticality of the column."""
        column_name = column['column_name'].lower()
        completeness = column.get('completeness_pct', 0)
        uniqueness_ratio = column.get('unique_count', 0) / max(column.get('total_count', 1), 1)

        # High criticality indicators
        if any(pattern in column_name for pattern in ['id', 'key', 'primary']):
            return "High - Primary business identifier"

        elif any(pattern in column_name for pattern in ['amount', 'price', 'cost', 'revenue', 'commission']):
            return "High - Financial/Revenue critical"

        elif column.get('potential_pii'):
            return "High - Personal data with compliance requirements"

        elif completeness > 95 and uniqueness_ratio > 0.8:
            return "High - Well-maintained business key"

        # Industry-specific high criticality
        elif self.industry == "Online Travel Agency (OTA)":
            if any(pattern in column_name for pattern in ['booking', 'reservation', 'guest']):
                return "High - Core booking business process"
            elif any(pattern in column_name for pattern in ['checkin', 'checkout']):
                return "High - Critical for stay management"

        # Medium criticality
        elif any(pattern in column_name for pattern in ['date', 'time', 'status', 'type']):
            return "Medium - Operational tracking field"

        elif completeness > 80:
            return "Medium - Well-populated business data"

        # Low criticality
        else:
            return "Low - Supporting or optional data"

    def _suggest_business_rules(self, column: Dict[str, Any]) -> List[str]:
        """Suggest business rules for the column."""
        rules = []
        column_name = column['column_name'].lower()
        data_type = column.get('data_type', '')
        completeness = column.get('completeness_pct', 0)

        # Completeness rules
        if completeness < 70:
            rules.append("Implement data completeness monitoring and alerts")

        # Type-specific rules
        if data_type in ['integer', 'float']:
            if 'amount' in column_name or 'price' in column_name or 'commission' in column_name:
                rules.append("Validate non-negative amounts for financial fields")
                rules.append("Implement currency precision rules (2 decimal places)")

            if 'quantity' in column_name or 'qty' in column_name or 'rooms' in column_name:
                rules.append("Validate positive quantities for inventory")

        elif data_type == 'string':
            if any(pattern in column_name for pattern in ['email', 'mail']):
                rules.append("Validate email format using regex pattern")
                rules.append("Implement duplicate email detection")

            elif any(pattern in column_name for pattern in ['phone', 'tel']):
                rules.append("Standardize phone number format")
                rules.append("Validate phone number patterns by region")

            elif any(pattern in column_name for pattern in ['status', 'state']):
                rules.append("Implement allowed values validation")
                rules.append("Create status transition rules")

        # Business key rules
        if column.get('potential_business_key'):
            rules.append("Implement uniqueness constraints")
            rules.append("Create referential integrity checks")

        # PII rules
        if column.get('potential_pii'):
            rules.append("Implement data masking for non-production environments")
            rules.append("Create access control and audit logging")

        # Industry-specific rules
        if self.industry == "Online Travel Agency (OTA)":
            if 'booking' in column_name and 'date' in column_name:
                rules.append("Validate booking date is not in the past")
            if 'checkin' in column_name or 'checkout' in column_name:
                rules.append("Validate check-out date is after check-in date")
            if 'cancellation' in column_name and 'date' in column_name:
                rules.append("Validate cancellation deadline against check-in date")
            if 'commission' in column_name:
                rules.append("Validate commission percentage is within contract limits")

        return rules

    def generate_business_glossary_entry(self, column: Dict[str, Any]) -> Dict[str, str]:
        """Generate a business glossary entry for the column."""
        return {
            'business_name': column.get('suggested_name', column['column_name']),
            'business_definition': column.get('business_description', ''),
            'business_context': column.get('business_context', ''),
            'data_steward': f"{self.industry} Data Team",
            'business_owner': f"{self.industry} Business Unit",
            'usage_guidelines': self._generate_usage_guidelines(column),
            'related_metrics': self._identify_related_metrics(column)
        }

    def _generate_usage_guidelines(self, column: Dict[str, Any]) -> str:
        """Generate usage guidelines for the column."""
        guidelines = []

        if column.get('potential_pii'):
            guidelines.append("Requires approval for access due to PII content")

        if column.get('business_criticality', '').startswith('High'):
            guidelines.append("Critical business field - changes require business approval")

        if column.get('compliance_implications'):
            guidelines.append("Subject to regulatory compliance requirements")

        return "; ".join(guidelines) if guidelines else "Standard business data usage applies"

    def _identify_related_metrics(self, column: Dict[str, Any]) -> List[str]:
        """Identify related business metrics."""
        column_name = column['column_name'].lower()

        if self.industry == "Online Travel Agency (OTA)":
            if 'booking' in column_name:
                return ["Booking Conversion Rate", "Cancellation Rate", "Revenue per Booking"]
            elif 'commission' in column_name or 'revenue' in column_name:
                return ["Average Commission Rate", "Partner Revenue", "Margin Analysis"]
            elif 'guest' in column_name or 'customer' in column_name:
                return ["Guest Satisfaction Score", "Repeat Booking Rate", "Customer Lifetime Value"]
            elif 'property' in column_name:
                return ["Property Performance Score", "Occupancy Rate", "Average Daily Rate"]
            elif 'search' in column_name:
                return ["Search Conversion Rate", "Click-through Rate", "Abandonment Rate"]

        elif 'revenue' in column_name or 'sales' in column_name:
            return ["Monthly Revenue", "Year-over-Year Growth", "Revenue per Customer"]

        elif 'customer' in column_name and 'id' in column_name:
            return ["Customer Count", "Customer Acquisition Rate", "Customer Retention"]

        elif 'order' in column_name:
            return ["Order Volume", "Average Order Value", "Order Conversion Rate"]

        elif 'product' in column_name:
            return ["Product Performance", "Inventory Turnover", "Product Profitability"]

        else:
            return []