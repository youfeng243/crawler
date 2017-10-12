SITE_RECORD_ID = "_site_record_id"

class HBaseDefs:
    SINGLE_SRC_EXISTING_FRAGMENT_CF = "docs"
    SINGLE_SRC_EXISTING_FRAGMENT_COL = "existing_fragment"
    SINGLE_SRC_LATEST_MERGED_DOC_COL = "latest"
    SINGLE_SRC_UTIME_COL = "utime"
    SINGLE_SRC_SITE_COL = "site"
    SINGLE_SRC_META_COL = "meta"
    SINGLE_SRC_META_MERGED_TIMES = "merged_times_without_output"
    SINGLE_SRC_META_TOTAL_OUTPUT_TIMES = "total_output_times"
    SINGLE_SRC_META_SEEN_LATEST_DL_TIME = "seen_latest_dl_time"
    SINGLE_SRC_META_LATEST_MERGED_TIME = "latest_merged_time"
    SINGLE_SRC_ATTR_TIME_TRACE = "time_trace"
    SINGLE_SRC_ATTR_TOPIC_META = "topic_meta"
    SINGLE_SRC_DANGLING_FRAGMENTS = "dangling"
    SINGLE_SRC_CURRENT_UUID = "current_uuid"
    SINGLE_SRC_CURRENT_OUTPUT_DOC_UNIQUE_ID = "current_output_doc_unique_id"
    SINGLE_SRC_SEEN_TOPO_TOKENS = "seen_topo_tokens"
    SINGLE_SRC_BASE_INFO_URL = "base_info_url"
    SINGLE_SRC_IS_TOPO_TOKEN_BASED = "is_token_based"
    SINGLE_SRC_SEQ = "single_src_seq"

    MULTI_SRC_LATEST_MERGED_COL = "latest"
    MULTI_SRC_DOCS_CF = "docs"
    MULTI_SRC_TRACE = "trace"


FIELDNAME_RECORD_ID = '_record_id'
FIELDNAME_SOURCE = '_src'
FIELDNAME_IN_TIME = '_in_time'
FIELDNAME_UTIME = '_utime'

DOWNLOAD_TIME = 'download_time'

class MetaFields:
    RECORD_ID = FIELDNAME_RECORD_ID
    SRC = FIELDNAME_SOURCE
    IN_TIME = FIELDNAME_IN_TIME
    UTIME = FIELDNAME_UTIME
    SEQ = "_single_src_seq"
    SITE = 'site'
    SITE_RECORD_ID = SITE_RECORD_ID
    TOPO_TOKEN = "_topo_token"
    TOPO_TOKEN_INCOMMING = "_topo_token_incomming"
    TOPO_TOKEN_LOCAL = "_topo_token_local"
    TOPO_TOKEN_RAW = "_topo_token_raw"
    UUID = "_page_uuid"
    DOC_UNIQUE_ID = "_doc_unique_id"
    DOC_MERGED_TIMES = "_doc_merged_times"
    HAS_SCHEMA_ERROR = "_has_schema_error"
    SCHEMA_ERROR_DETAIL = "_schema_error_detail"
    IS_IMPORTED = "_is_imported"

    SINGLE_SRC_MERGER_ALLOW_OUTPUT = {SEQ, RECORD_ID, IN_TIME, UTIME, UUID, SITE_RECORD_ID, SRC, '_download_time'}
    ALLOW_META_FIELDS = {RECORD_ID, IN_TIME, UTIME, SRC}

DATE_STR_FORMAT = '%Y-%m-%d'
DATETIME_STR_FORMAT = DATE_STR_FORMAT + ' %H:%M:%S'

STOP_TOKEN = "_STOP_THREAD_THIS_IS_A_VERY_UNLIKELY_VALUE_"

BATCH_IMPORTED_DATA = "BATCH_IMPORTED_DATA"