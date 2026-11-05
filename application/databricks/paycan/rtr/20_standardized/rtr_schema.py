from pyspark.sql.types import StructType, ArrayType, StructField, StringType, DateType, TimestampType, IntegerType, DecimalType, LongType, BooleanType

class RTR:

    # define other constants
    DATABASE = "standardized_db"
    CURATED_DATABASE = "curated_db"

    # Constants to be used in the table definitions
    NULLABLE = True
    NON_NULLABLE = False

    # Data Schema
    EX_PAYMENT_PREVIOUS_INSTRUCTING_AGENTS_SCHEMA = StructType([
        StructField("agent_index", LongType(), False),
        StructField("bicfi", StringType(), True),
        StructField("clearing_system_identification_code", StringType(), True),
        StructField("clearing_system_identification_proprietary", StringType(), True),
        StructField("clearing_system_member_id", StringType(), True),
        StructField("legal_entity_identifier", StringType(), True),
        StructField("name", StringType(), True),
        StructField("postal_address", StructType([
            StructField("postal_code", StringType(), True),
            StructField("town_name", StringType(), True),
            StructField("country", StringType(), True),
        ]), True),
        StructField("other", StructType([
            StructField("identification", StringType(), True),
            StructField("scheme_name_code", StringType(), True),
            StructField("scheme_name_proprietary", StringType(), True),
            StructField("issuer", StringType(), True),
        ]), True),
    ])

    TECH_SCHEMA = StructType([
        StructField("technical_state", StringType(), True),
        StructField("technical_state_time", TimestampType(), True),
    ])

    TRANSACTION_STATE_SCHEMA = StructType([
        StructField("transaction_state", StringType(), True),
        StructField("transaction_state_time", TimestampType(), True),
        StructField("technical_states", ArrayType(TECH_SCHEMA), True),
    ])

    EX_PAYMENT_TRANSACTION_STATUSES_SCHEMA = StructType([
        StructField("status_code", StringType(), True),
        StructField("status_description", StringType(), True),
        StructField("status_reason_code", StringType(), True),
        StructField("status_reason_desc", StringType(), True),
        StructField("status_time", TimestampType(), True),
        StructField("transaction_states", ArrayType(TRANSACTION_STATE_SCHEMA), True),
    ])

    AGENT_SCHEMA = StructType([
        StructField("bicfi", StringType(), True),
        StructField("clearing_system_identification_code", StringType(), True),
        StructField("clearing_system_member_id", StringType(), True),
        StructField("legal_entity_identifier", StringType(), True),
        StructField("name", StringType(), True),
        StructField("postal_address", StructType([
            StructField("postal_code", StringType(), True),
            StructField("town_name", StringType(), True),
            StructField("country", StringType(), True),
        ]), True),
    ])

    EX_PAYMENT_SCHEMA_RAW = StructType([
        StructField("uetr", StringType(), False),
        StructField("message_identification", StringType(), False),
        StructField("end_to_end_identification", StringType(), False),
        StructField("transaction_identification", StringType(), True),
        StructField("clearing_and_settlement_assigned_reference", StringType(), True),
        StructField("competitive_service_id", StringType(), False),
        StructField("reconciliation_cycle_id", StringType(), True),
        StructField("exchange_assigned_reference", StringType(), False),
        StructField("sending_participant_id", StringType(), False),
        StructField("sending_participant_csp_id", StringType(), False),
        StructField("receiving_participant_id", StringType(), False),
        StructField("receiving_participant_csp_id", StringType(), True),
        StructField("payment_amount", DecimalType(15, 2), False),
        StructField("payment_currency_code", StringType(), False),
        StructField("creation_time", TimestampType(), False),

        StructField("debtor_agent", AGENT_SCHEMA, True),
        StructField("creditor_agent", AGENT_SCHEMA, True),

        StructField("previous_instructing_agents", ArrayType(EX_PAYMENT_PREVIOUS_INSTRUCTING_AGENTS_SCHEMA), True),
        StructField("transaction_statuses", ArrayType(EX_PAYMENT_TRANSACTION_STATUSES_SCHEMA), True),
    ])

    # EX_PAYMENT_SCHEMA = StructType([
    #     StructField("uetr", StringType(), False),
    #     StructField("message_identification", StringType(), False),
    #     StructField("end_to_end_identification", StringType(), False),
    #     StructField("transaction_identification", StringType(), True),
    #     StructField("clearing_and_settlement_assigned_reference", StringType(), True),
    #     StructField("competitive_service_id", StringType(), False),
    #     StructField("reconciliation_cycle_id", StringType(), True),
    #     StructField("exchange_assigned_reference", StringType(), False),
    #     StructField("sending_participant_id", StringType(), False),
    #     StructField("sending_participant_csp_id", StringType(), False),
    #     StructField("receiving_participant_id", StringType(), False),
    #     StructField("receiving_participant_csp_id", StringType(), True),
    #     StructField("payment_amount", DecimalType(15, 2), False),
    #     StructField("payment_currency_code", StringType(), False),
    #     StructField("creation_time", TimestampType(), False),

    #     StructField("debtor_agent", AGENT_SCHEMA, True),
    #     StructField("creditor_agent", AGENT_SCHEMA, True)
    # ])

    EX_PAYMENT_SCHEMA = StructType([
        StructField("uetr", StringType(), False),
        StructField("message_identification", StringType(), False),
        StructField("end_to_end_identification", StringType(), False),
        StructField("transaction_identification", StringType(), True),
        StructField("clearing_and_settlement_assigned_reference", StringType(), True),
        StructField("competitive_service_id", StringType(), False),
        StructField("reconciliation_cycle_id", StringType(), True),
        StructField("exchange_assigned_reference", StringType(), False),
        StructField("sending_participant_id", StringType(), False),
        StructField("sending_participant_csp_id", StringType(), False),
        StructField("receiving_participant_id", StringType(), False),
        StructField("receiving_participant_csp_id", StringType(), True),
        StructField("payment_amount", DecimalType(15, 2), False),
        StructField("payment_currency_code", StringType(), False),
        StructField("creation_time", TimestampType(), False),

        # debtor agent (flattened)
        StructField("debtor_agent_bicfi", StringType(), True),
        StructField("debtor_agent_clearing_system_identification_code", StringType(), True),
        StructField("debtor_agent_clearing_system_member_id", StringType(), True),
        StructField("debtor_agent_legal_entity_identifier", StringType(), True),
        StructField("debtor_agent_name", StringType(), True),
        StructField("debtor_agent_postal_address_postal_code", StringType(), True),
        StructField("debtor_agent_postal_address_town_name", StringType(), True),
        StructField("debtor_agent_postal_address_country", StringType(), True),

        # creditor agent (flattened)
        StructField("creditor_agent_bicfi", StringType(), True),
        StructField("creditor_agent_clearing_system_identification_code", StringType(), True),
        StructField("creditor_agent_clearing_system_member_id", StringType(), True),
        StructField("creditor_agent_legal_entity_identifier", StringType(), True),
        StructField("creditor_agent_name", StringType(), True),
        StructField("creditor_agent_postal_address_postal_code", StringType(), True),
        StructField("creditor_agent_postal_address_town_name", StringType(), True),
        StructField("creditor_agent_postal_address_country", StringType(), True),
    ])


    EX_CONNECTION_SERVICE_PROVIDER_SCHEMA = StructType([
        StructField("connection_service_provider_id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("financial_institution_indicator", StringType(), False),
        StructField("key_contacts", ArrayType(
            StructType([
                StructField("title", StringType(), True),
                StructField("name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("phone", StringType(), True),
            ])
        ), 
        False),
        StructField("effective_time", TimestampType(), False),
        StructField("expire_time", TimestampType(), False)
    ])

    EX_COMPETITIVE_SERVICE_SCHEMA = StructType([
        StructField("competitive_service_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("key_contacts", ArrayType(
            StructType([
                StructField("title", StringType(), True),
                StructField("name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("phone", StringType(), True)
            ])
        ), True),
        StructField("effective_time", TimestampType(), True),
        StructField("expire_time", TimestampType(), True)
    ])

    EX_PARTICIPANT_SCHEMA = StructType([
        StructField("participant_id", StringType(), False),  # NOT NULL (PK)
        StructField("name", StringType(), False),            # NOT NULL

        StructField(
            "addresses",
            ArrayType(
                StructType([
                    StructField("street_number", StringType(), True),
                    StructField("street_name", StringType(), True),
                    StructField("building_name", StringType(), True),
                    StructField("city", StringType(), True),
                    StructField("province", StringType(), True),
                    StructField("postal_code", StringType(), True),
                ])
            ),
            False  # NOT NULL
        ),

        StructField(
            "key_contacts",
            ArrayType(
                StructType([
                    StructField("title", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("email", StringType(), True),
                    StructField("phone", StringType(), True),
                ])
            ),
            False  # NOT NULL
        ),

        StructField("registration_date", DateType(), False),          # NOT NULL
        StructField("active", BooleanType(), False),                  # NOT NULL
        StructField("authorized_to_originate", BooleanType(), False), # NOT NULL

        StructField(
            "authorized_connection_service_providers",
            ArrayType(
                StructType([
                    StructField("connection_service_provider_id", StringType(), True),
                    StructField("name", StringType(), True),
                ])
            ),
            True  # NULL allowed
        ),

        StructField(
            "registered_competitive_services",
            ArrayType(
                StructType([
                    StructField("competitive_service_provider_id", StringType(), True),
                    StructField("name", StringType(), True),
                ])
            ),
            True  # NULL allowed
        ),

        # struct first, not array then struct - so gets flattened
        StructField(
            "default_routing_method",
            StructType([
                StructField("method", StringType(), True),
                StructField("default_connection_service_provider_id", StringType(), True),
            ]),
            True  # NULL allowed
        ),

        StructField(
            "routing_rules",
            ArrayType(
                StructType([
                    StructField("competitive_service_id", StringType(), True),
                    StructField("method", StringType(), True),
                    StructField("connection_service_provider_id", StringType(), True),
                ])
            ),
            True  # NULL allowed
        ),

        StructField(
            "transit_number_routing_rules",
            ArrayType(
                StructType([
                    StructField("transit_number", StringType(), True),
                    StructField("connection_service_provider_id", StringType(), True),
                ])
            ),
            True  # NULL allowed
        ),

        StructField("effective_time", TimestampType(), False),  # NOT NULL
        StructField("expire_time", TimestampType(), False),     # NOT NULL
    ])

    CS_ACCOUNT_SCHEMA = StructType([
        StructField("account_id", IntegerType(), nullable=False, metadata={}),
        StructField("account_tier_type", StringType(), nullable=True, metadata={}),
        StructField("participant_number", StringType(), nullable=True, metadata={}),
        StructField("account_number", StringType(), nullable=True, metadata={}),
        StructField("account_type", StringType(), nullable=True, metadata={}),
        StructField("currency", StringType(), nullable=True, metadata={}),
        StructField("inactive_reason", StringType(), nullable=True, metadata={}),
        StructField("effective_time", TimestampType(), nullable=True, metadata={}),
        StructField("expire_time", TimestampType(), nullable=True, metadata={}),
    ])

    CS_TRANSACTION_SCHEMA = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("transaction_type", StringType(), True),
        StructField("cns_ref_id", StringType(), True),
        StructField("end_to_end_reference", StringType(), True),
        StructField("uetr", StringType(), True),
        StructField("cycle_id", StringType(), True),
        StructField("cycle_date", DateType(), True),
        StructField("status_timestamp", TimestampType(), True),
        StructField("transaction_status", StringType(), True),
        StructField("reject_reason_code", StringType(), True),
        StructField("transaction_amount", DecimalType(15,2), True),
        StructField("exchange_code", StringType(), True),
        StructField("exchange_name", StringType(), True),

        StructField("sender", StructType([
            StructField("position_account_number", StringType(), True),
            StructField("position_account_type", StringType(), True),
            StructField("account_balance_after_transaction", DecimalType(15,2), True),
            StructField("reserve_balance_after_transaction", DecimalType(15,2), True),
            StructField("participant_number", StringType(), True),
            StructField("participant_short_name_en", StringType(), True),
            StructField("participant_short_name_fr", StringType(), True),
            StructField("participant_name_en", StringType(), True),
            StructField("participant_name_fr", StringType(), True),
            StructField("participant_irn", StringType(), True)
        ]), True),

        StructField("sender_settlement_agent", StructType([
            StructField("settlement_account_number", StringType(), True),
            StructField("settlement_account_type", StringType(), True),
            StructField("account_balance_after_transaction", DecimalType(15,2), True),
            StructField("reserve_balance_after_transaction", DecimalType(15,2), True),
            StructField("participant_number", StringType(), True),
            StructField("participant_short_name_en", StringType(), True),
            StructField("participant_short_name_fr", StringType(), True),
            StructField("participant_name_en", StringType(), True),
            StructField("participant_name_fr", StringType(), True),
            StructField("participant_irn", StringType(), True)
        ]), True),

        StructField("receiver", StructType([
            StructField("position_account_number", StringType(), True),
            StructField("position_account_type", StringType(), True),
            StructField("account_balance_after_transaction", DecimalType(15,2), True),
            StructField("reserve_balance_after_transaction", DecimalType(15,2), True),
            StructField("participant_number", StringType(), True),
            StructField("participant_short_name_en", StringType(), True),
            StructField("participant_short_name_fr", StringType(), True),
            StructField("participant_name_en", StringType(), True),
            StructField("participant_name_fr", StringType(), True),
            StructField("participant_irn", StringType(), True)
        ]), True),

        StructField("receiver_settlement_agent", StructType([
            StructField("settlement_account_number", StringType(), True),
            StructField("settlement_account_type", StringType(), True),
            StructField("account_balance_after_transaction", DecimalType(15,2), True),
            StructField("reserve_balance_after_transaction", DecimalType(15,2), True),
            StructField("participant_number", StringType(), True),
            StructField("participant_short_name_en", StringType(), True),
            StructField("participant_short_name_fr", StringType(), True),
            StructField("participant_name_en", StringType(), True),
            StructField("participant_name_fr", StringType(), True),
            StructField("participant_irn", StringType(), True)
        ]), True),

        StructField("commit", StructType([
            StructField("commit_id", StringType(), True),
            StructField("cycle_id", StringType(), True),
            StructField("cycle_date", DateType(), True),
            StructField("status_code", StringType(), True),
            StructField("reject_reason_code", StringType(), True),
            StructField("status_timestamp", TimestampType(), True),

            StructField("sender", StructType([
                StructField("account_balance_after_transaction", DecimalType(15,2), True),
                StructField("reserve_balance_after_transaction", DecimalType(15,2), True),
            ]), True),

            StructField("sender_settlement_agent", StructType([
                StructField("account_balance_after_transaction", DecimalType(15,2), True),
                StructField("reserve_balance_after_transaction", DecimalType(15,2), True),
            ]), True),

            StructField("receiver", StructType([
                StructField("account_balance_after_transaction", DecimalType(15,2), True),
                StructField("reserve_balance_after_transaction", DecimalType(15,2), True),
            ]), True),

            StructField("receiver_settlement_agent", StructType([
                StructField("account_balance_after_transaction", DecimalType(15,2), True),
                StructField("reserve_balance_after_transaction", DecimalType(15,2), True),
            ]), True),
        ]), True),

        StructField("final_status_code", StringType(), True)
    ])

    CS_PARTICIPANT_SCHEMA = StructType([
        StructField("participant_id", LongType(), False),   # bigint NOT NULL
        StructField("participant_number", StringType(), True),
        StructField("short_name_en", StringType(), True),
        StructField("short_name_fr", StringType(), True),
        StructField("name_en", StringType(), True),
        StructField("name_fr", StringType(), True),
        StructField("irn", StringType(), True),
        StructField("status", StringType(), True),
        StructField("inactive_reason", StringType(), True),
        StructField("participant_type", StringType(), True),
        StructField("settlement_agent_flag", BooleanType(), True),
        StructField("peer_to_peer_flag", BooleanType(), True),
        StructField("effective_time", StringType(), True), # epoch micros/nanos
        StructField("expire_time", StringType(), True), # epoch micros/nanos
    ])

    CS_NET_DEBIT_CAP_SCHEMA = StructType([
        StructField("id", LongType(), False),                 # bigint NOT NULL
        StructField("account_tier_type", StringType(), True),
        StructField("account_number", StringType(), True),
        StructField("net_debit_cap", DecimalType(15, 2), True),
        StructField("effective_time", TimestampType(), True),
        StructField("expire_time", TimestampType(), True),
    ])

    CS_WATERMARK_SCHEMA = StructType([
        StructField("watermark_id", LongType(), False),
        StructField("account_tier_type", StringType(), True),
        StructField("account_number", StringType(), True),
        StructField("watermark_type", StringType(), True),
        StructField("watermark_value", DecimalType(20, 2), True),
        StructField("watermark_threshold", StringType(), True),
        StructField("effective_time", StringType(), True),
        StructField("expire_time", StringType(), True),
    ])

    CS_EXCHANGE_SCHEMA = StructType([
        StructField("id", LongType(), False),                 # bigint NOT NULL
        StructField("exchange_code", StringType(), True),
        StructField("exchange_name_en", StringType(), True),
        StructField("exchange_name_fr", StringType(), True),
        StructField("description_en", StringType(), True),
        StructField("description_fr", StringType(), True),
        StructField("validate_uetr_flag", BooleanType(), True),
        StructField("ora_delay_time", StringType(), True),
        StructField("ora_max_retry", StringType(), True),
        StructField("effective_time", TimestampType(), True),
        StructField("expire_time", TimestampType(), True),
    ])

    # CS_REJECT_REASON_CODE_SCHEMA = StructType([
    #     StructField("reject_reason_code", StringType(), False),  # NOT NULL
    #     StructField("description_en", StringType(), True),
    #     StructField("description_fr", StringType(), True),
    #     StructField("effective_time", TimestampType(), True),
    #     StructField("expire_time", TimestampType(), True),
    # ])

    CS_REJECT_REASON_CODE_SCHEMA = StructType([
        StructField("reject_code", StringType(), True),
        StructField("reject_code_name", StringType(), True),
        StructField("reject_code_description_en", StringType(), True),
        StructField("reject_code_description_fr", StringType(), True),
    ])

    # Dataset definition
    DATASETS = {
        "cs": {
            "name": "cs",
            "raw_path": "raw/external/payments_canada/rtr/cs",
            "files": [
                {"name":"account","file_name": "account_cs_{DATE}.json", "schema": CS_ACCOUNT_SCHEMA, "table_name": "standardized_db.tbl_paycan_rtr_account_cs", "table_update_mode":"overwrite"},
                {"name":"transaction","file_name": "transaction_cs_{DATE}.json", "schema": CS_TRANSACTION_SCHEMA, "table_name": "standardized_db.tbl_paycan_rtr_transaction_cs", "table_update_mode":"replace"},
                {"name":"participant","file_name": "participant_cs_{DATE}.json", "schema": CS_PARTICIPANT_SCHEMA, "table_name": "standardized_db.tbl_paycan_rtr_participant_cs", "table_update_mode":"overwrite"}, 
                {"name":"net_debit_cap","file_name": "net_debit_cap_cs_{DATE}.json", "schema": CS_NET_DEBIT_CAP_SCHEMA, "table_name": "standardized_db.tbl_paycan_rtr_net_debit_cap_cs", "table_update_mode":"overwrite"}, 
                {"name":"watermark","file_name": "watermark_cs_{DATE}.json", "schema": CS_WATERMARK_SCHEMA, "table_name": "standardized_db.tbl_paycan_rtr_watermark_cs", "table_update_mode":"overwrite"}, 
                {"name":"exchange","file_name": "exchange_cs_{DATE}.json", "schema": CS_EXCHANGE_SCHEMA, "table_name": "standardized_db.tbl_paycan_rtr_exchange_cs", "table_update_mode":"overwrite"}, 
                {"name":"reject_reason_code","file_name": "reject_reason_codes_cs_{DATE}.json", "schema": CS_REJECT_REASON_CODE_SCHEMA, "table_name": "standardized_db.tbl_paycan_rtr_reject_reason_code_cs", "table_update_mode":"overwrite"}
            ]
        },
        "exchange": {
            "name": "exchange",
            "raw_path": "raw/external/payments_canada/rtr/exchange",
            "files": [
                {"name":"competitive_service","file_name": "competitive_service_ex_{DATE}.json", "schema": EX_COMPETITIVE_SERVICE_SCHEMA, "table_name": "standardized_db.tbl_paycan_rtr_competitive_service_ex", "table_update_mode":"overwrite"},
                {"name":"connection_service_provider","file_name": "connection_service_provider_ex_{DATE}.json", "schema": EX_CONNECTION_SERVICE_PROVIDER_SCHEMA, "table_name": "standardized_db.tbl_paycan_rtr_connection_service_provider_ex", "table_update_mode":"overwrite"},
                {"name":"participant","file_name": "participant_ex_{DATE}.json", "schema": EX_PARTICIPANT_SCHEMA, "table_name": "standardized_db.tbl_paycan_rtr_participant_ex", "table_update_mode":"overwrite"},
                {"name":"payment","file_name": "payment_ex_{DATE}.json", "schema": [EX_PAYMENT_SCHEMA_RAW, EX_PAYMENT_SCHEMA, EX_PAYMENT_PREVIOUS_INSTRUCTING_AGENTS_SCHEMA, EX_PAYMENT_TRANSACTION_STATUSES_SCHEMA], "table_name": ["standardized_db.tbl_paycan_rtr_payment_ex", "standardized_db.tbl_paycan_rtr_payment_previous_instructing_agents_ex", "standardized_db.tbl_paycan_rtr_payment_transaction_statuses_ex"], "table_update_mode":"replace"}
            ]
        }
    }

    def __init__(self):
        pass
