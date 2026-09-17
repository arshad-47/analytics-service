-- Enable standard uuid extension if needed (PostgreSQL 13+ has gen_random_uuid() built-in)
-- CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =========================================================================
-- 1. BASE MULTI-TENANCY
-- =========================================================================

CREATE TABLE tenant (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL UNIQUE,
    code        TEXT NOT NULL UNIQUE,
    created_by  TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    meta        JSONB
);

-- =========================================================================
-- 2. METADATA REFERENCES
-- =========================================================================

CREATE TABLE leader_category (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL,
    description TEXT,
    tenant_code TEXT NOT NULL REFERENCES tenant(code) ON DELETE CASCADE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE programs (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    leaders_id  UUID NOT NULL REFERENCES leader_category(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    description TEXT,
    tenant_code TEXT NOT NULL REFERENCES tenant(code) ON DELETE CASCADE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =========================================================================
-- 3. PROMPT MANAGEMENT SYSTEM (Generic & Shared)
-- =========================================================================

CREATE TABLE prompts (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name          TEXT NOT NULL UNIQUE,
    analysis_type TEXT NOT NULL, -- 'pii', 'theme', 'environment', etc.
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE prompt_version (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    prompt_id     UUID NOT NULL REFERENCES prompts(id) ON DELETE CASCADE,
    version       INT NOT NULL,
    system_prompt TEXT NOT NULL,
    user_prompt   TEXT NOT NULL, -- Template containing {{variables}}
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_by    TEXT,
    change_note   TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    
    CONSTRAINT unique_prompt_version UNIQUE (prompt_id, version)
);

-- =========================================================================
-- 4. CENTRAL INGESTION TRACKING
-- =========================================================================

CREATE TABLE submissions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id      TEXT UNIQUE, -- 32-character random alphanumeric string
    submission_id   TEXT NOT NULL,
    tenant_code     TEXT NOT NULL REFERENCES tenant(code) ON DELETE CASCADE,
    submission_type TEXT NOT NULL, -- 'discussion', 'story', etc.
    user_id         TEXT, -- Future login integration
    user_name       TEXT,
    role            TEXT,
    state           TEXT,
    district        TEXT,
    organization    TEXT,
    submission_date TIMESTAMPTZ,
    process_status  JSONB,
    status          TEXT NOT NULL DEFAULT 'pending', -- 'pending', 'processing', 'success', 'failed'
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    program_id      UUID REFERENCES programs(id) ON DELETE SET NULL,
    leader_id       UUID REFERENCES leader_category(id) ON DELETE SET NULL,
    
    CONSTRAINT unique_submission_tenant UNIQUE (submission_id, tenant_code)
);

-- =========================================================================
-- 5. SOURCE PAYLOADS (Raw CSV / Kafka Output)
-- =========================================================================

CREATE TABLE discussion_submissions (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    submission_id                     TEXT NOT NULL,
    tenant_code                       TEXT NOT NULL,
    title                             TEXT,
    discussion_date                   TIMESTAMPTZ,
    challenges                        TEXT[], -- one array element per discrete statement (see operations.py's _normalize_statement_list)
    solutions                         TEXT[], -- same format as challenges
    author                            TEXT,
    language                          TEXT,
    image_urls                        TEXT[] DEFAULT '{}',
    blur_image_urls                   TEXT[] DEFAULT '{}',
    pdf_urls                          TEXT[] DEFAULT '{}',
    masked_pdf_urls                   TEXT[] DEFAULT '{}',
    transcript_link                   TEXT,
    pii_masked                        BOOLEAN NOT NULL DEFAULT FALSE,
    pii_masked_at                     TEXT[] DEFAULT '{}',
    abusive_masked_at                 TEXT[] DEFAULT '{}',
    pri_member_name                   TEXT,
    pri_member_designation            TEXT,
    school_representative_name        TEXT,
    school_representative_designation TEXT,
    created_at                        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                        TIMESTAMPTZ NOT NULL DEFAULT now(),
    meta_data                         JSONB,
    
    FOREIGN KEY (submission_id, tenant_code) 
        REFERENCES submissions(submission_id, tenant_code) ON DELETE CASCADE
);

CREATE TABLE story_submissions (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    submission_id       TEXT NOT NULL,
    tenant_code         TEXT NOT NULL,
    title               TEXT,
    objective           TEXT,
    challenge           TEXT[], -- one array element per discrete statement, same format as discussion_submissions.challenges (see operations.py's _normalize_statement_list)
    action_steps        TEXT[], -- same format as challenge
    impact              TEXT,
    duration            TEXT,
    blurb               TEXT,
    content             TEXT,
    image_urls          TEXT[] DEFAULT '{}',
    blur_image_urls     TEXT[] DEFAULT '{}',
    pdf_urls            TEXT[] DEFAULT '{}',
    masked_pdf_urls     TEXT[] DEFAULT '{}',
    transcript_link     TEXT,
    pii_masked          BOOLEAN NOT NULL DEFAULT FALSE,
    pii_masked_at       TEXT[] DEFAULT '{}',
    abusive_masked_at   TEXT[] DEFAULT '{}',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    meta_data           JSONB,
    
    FOREIGN KEY (submission_id, tenant_code) 
        REFERENCES submissions(submission_id, tenant_code) ON DELETE CASCADE
);

-- =========================================================================
-- 6. EXECUTION & AUDIT LOGGING
-- =========================================================================

CREATE TABLE llm_logs (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    submission_id       TEXT NOT NULL,
    tenant_code         TEXT NOT NULL,
    model_name          TEXT NOT NULL,
    model_version       TEXT,
    analysis_type       TEXT NOT NULL,
    prompt_version_id   UUID NOT NULL REFERENCES prompt_version(id),
    prompt_tokens       INT NOT NULL DEFAULT 0,
    completion_tokens   INT NOT NULL DEFAULT 0,
    total_tokens        INT GENERATED ALWAYS AS (prompt_tokens + completion_tokens) STORED,
    status              TEXT NOT NULL, -- 'success', 'failed', 'timeout', 'retried'
    error_message       TEXT,
    called_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    meta_data           JSONB,
    
    FOREIGN KEY (submission_id, tenant_code) 
        REFERENCES submissions(submission_id, tenant_code) ON DELETE CASCADE
);

-- =========================================================================
-- 7. TAXONOMY (Global)
-- =========================================================================

CREATE TABLE themes (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                    TEXT NOT NULL UNIQUE,
    definitions             TEXT,
    keywords                TEXT,
    examples                TEXT,
    status                  TEXT -- 'Approved', 'Rejected', 'Merged', 'Draft'
    -- total_objective_count   INTEGER DEFAULT 0, --newly added can be removed 
    -- original_statement_text TEXT[] DEFAULT '{}' --newly added can be removed 
);

-- =========================================================================
-- 8. STATEMENT EXTRACTION
-- Individual statements extracted from a submission (challenges, solutions,
-- questions, answers). parent_id links related statements, e.g. a challenge
-- that already exists will be set as the parent of the duplicate.
-- =========================================================================

CREATE TABLE statements (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    submission_id    TEXT NOT NULL,
    tenant_code      TEXT NOT NULL,

    submission_type  TEXT NOT NULL,
    statement_type   TEXT NOT NULL,
    raw_statement     TEXT NOT NULL,         -- original text as submitted, punctuation intact
    cleaned_statement TEXT NOT NULL,         -- punctuation-stripped form used for deduplication

    parent_id        UUID,

    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    FOREIGN KEY (submission_id, tenant_code)
        REFERENCES submissions(submission_id, tenant_code)
        ON DELETE CASCADE,

    FOREIGN KEY (parent_id)
        REFERENCES statements(id)
        ON DELETE SET NULL
);

-- Expression index for fast case-insensitive deduplication (matches LOWER() query on cleaned form).
CREATE INDEX idx_statements_cleaned_lower
    ON statements (LOWER(cleaned_statement));

-- Submission lookup + cascade delete path.
CREATE INDEX IF NOT EXISTS idx_statements_submission_parent ON statements (submission_id, parent_id);
CREATE INDEX IF NOT EXISTS idx_statements_parent ON statements (parent_id) WHERE parent_id IS NOT NULL;

-- =========================================================================
-- Trigger: automatically promote a duplicate child to be the new parent
-- whenever the current root (parent_id IS NULL) statement is deleted.
-- This keeps the hierarchy flat — no orphaned chains.
-- =========================================================================

CREATE OR REPLACE FUNCTION promote_statement_child()
RETURNS TRIGGER AS $$
DECLARE
    new_parent_id UUID;
    new_sub_id TEXT;
    new_tenant TEXT;
BEGIN
    -- Only act if the statement being deleted is a root (parent)
    IF OLD.parent_id IS NULL THEN
        -- Find one child to become the new parent (the oldest duplicate)
        SELECT id, submission_id, tenant_code
        INTO new_parent_id, new_sub_id, new_tenant
        FROM statements
        WHERE parent_id = OLD.id
        ORDER BY created_at ASC
        LIMIT 1;

        IF new_parent_id IS NOT NULL THEN
            -- Make the chosen child a root (new parent)
            UPDATE statements
            SET parent_id = NULL
            WHERE id = new_parent_id;

            -- Repoint all remaining children to the new parent
            UPDATE statements
            SET parent_id = new_parent_id
            WHERE parent_id = OLD.id AND id != new_parent_id;
            
        END IF;
    END IF;

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_promote_statement_child ON statements;
CREATE TRIGGER trg_promote_statement_child
    BEFORE DELETE ON statements
    FOR EACH ROW
    EXECUTE FUNCTION promote_statement_child();

-- =========================================================================
-- 8a. THEMATIC & ENVIRONMENTAL EXTRACTION OUTPUTS
-- =========================================================================

CREATE TABLE analysis_results (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    submission_id           TEXT NOT NULL,
    tenant_code             TEXT NOT NULL,
    statement_id            UUID,

    analysis_type           TEXT NOT NULL,
    -- 'theme', 'environment', 'statement_category'

    analysis_column         TEXT[],

    ml_model_name           TEXT,
    ml_model_version        TEXT,

    model_confidence_score  FLOAT,
    llm_confidence_score    FLOAT,

    threshold               FLOAT,

    model_prediction        TEXT,
    llm_prediction          TEXT,

    theme_id                UUID,

    justification           TEXT,

    multi_theme_mapped      BOOLEAN NOT NULL DEFAULT FALSE,

    category_type           TEXT,
    -- 'Standard', 'Others', 'Unknown/Unclear', 'Flagged'

    meta_data               JSONB,

    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

    FOREIGN KEY (submission_id, tenant_code)
        REFERENCES submissions(submission_id, tenant_code)
        ON DELETE CASCADE
);

-- =========================================================================
-- 9. QUALITATIVE SCORING & SUMMARIES
-- =========================================================================

CREATE TABLE ranking (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    submission_id       TEXT NOT NULL,
    tenant_code         TEXT NOT NULL,
    criteria_data       JSONB NOT NULL, -- Rich criteria-specific scores & justifications
    composite_score     FLOAT NOT NULL,
    tier                TEXT,           -- 'Tier 1', 'Tier 2', etc.
    overall_summary     TEXT,           -- Concise LLM qualitative report summary
    meta_data           JSONB,
    
    FOREIGN KEY (submission_id, tenant_code) 
        REFERENCES submissions(submission_id, tenant_code) ON DELETE CASCADE
);

-- =========================================================================
-- 10. DYNAMIC KPI METRICS (EAV Model)
-- =========================================================================

CREATE TABLE metric_definitions (
    code            TEXT PRIMARY KEY, -- 'men', 'women', 'duration'
    label           TEXT NOT NULL,
    value_type      TEXT NOT NULL DEFAULT 'numeric' CHECK (value_type IN ('numeric', 'text')),
    submission_type TEXT,            -- Scoped payload type ('discussion', 'story')
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE submission_metrics (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    submission_id   TEXT NOT NULL,
    tenant_code     TEXT NOT NULL,
    metric_code     TEXT NOT NULL REFERENCES metric_definitions(code) ON DELETE CASCADE,
    numeric_value   INT,
    text_value      TEXT,
    
    CONSTRAINT one_value_required CHECK (numeric_value IS NOT NULL OR text_value IS NOT NULL),
    CONSTRAINT unique_submission_metric UNIQUE (submission_id, tenant_code, metric_code),
    
    FOREIGN KEY (submission_id, tenant_code) 
        REFERENCES submissions(submission_id, tenant_code) ON DELETE CASCADE
);

-- =========================================================================
-- 11. CSV UPLOAD TRACKING
-- =========================================================================

CREATE TABLE csv_uploads (
    id                     SERIAL PRIMARY KEY,
    report_type            VARCHAR(100) NOT NULL,
    program_name           VARCHAR(255),
    leader_category        VARCHAR(255),
    file_name              VARCHAR(500),
    file_size              BIGINT,
    cloud_storage_path     TEXT NOT NULL,
    meta_data              JSONB DEFAULT '{}'::jsonb,
    status                 VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_csv_uploads UNIQUE (program_name, leader_category, report_type, file_name, file_size)
);

CREATE INDEX idx_csv_uploads_status
    ON csv_uploads (status);

-- Keep updated_at fresh automatically
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_csv_uploads_updated_at ON csv_uploads;
CREATE TRIGGER trg_csv_uploads_updated_at
    BEFORE UPDATE ON csv_uploads
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at();

-- =========================================================================
-- INDEXES FOR HIGH-PERFORMANCE ANALYTICS
-- =========================================================================

-- Multi-tenancy isolation indexes
CREATE INDEX idx_submissions_tenant ON submissions (tenant_code);
CREATE INDEX idx_discussion_tenant ON discussion_submissions (tenant_code);
CREATE INDEX idx_story_tenant ON story_submissions (tenant_code);
CREATE INDEX idx_llm_logs_tenant ON llm_logs (tenant_code);
CREATE INDEX idx_analysis_results_tenant ON analysis_results (tenant_code);
CREATE INDEX idx_ranking_tenant ON ranking (tenant_code);
CREATE INDEX idx_submission_metrics_tenant ON submission_metrics (tenant_code);

-- Composite query mapping indexes (Foreign key performance optimization)
CREATE INDEX idx_discussion_submission_mapping ON discussion_submissions (submission_id, tenant_code);
CREATE INDEX idx_story_submission_mapping ON story_submissions (submission_id, tenant_code);
CREATE INDEX idx_statements_submission_mapping ON statements (submission_id);
CREATE INDEX idx_llm_logs_submission_mapping ON llm_logs (submission_id, tenant_code);
CREATE INDEX idx_analysis_results_submission_mapping ON analysis_results (submission_id, tenant_code);
CREATE INDEX idx_ranking_submission_mapping ON ranking (submission_id, tenant_code);
CREATE INDEX idx_submission_metrics_mapping ON submission_metrics (submission_id, tenant_code);

-- Program & Leader category filtering
CREATE INDEX idx_submissions_program ON submissions (program_id);
CREATE INDEX idx_submissions_leader ON submissions (leader_id);
CREATE INDEX idx_programs_leader ON programs (leaders_id);

-- Theme-specific analytics
CREATE INDEX idx_analysis_results_theme ON analysis_results (theme_id) WHERE theme_id IS NOT NULL;
CREATE INDEX idx_analysis_results_type ON analysis_results (analysis_type);
CREATE INDEX idx_analysis_results_statement ON analysis_results (statement_id) WHERE statement_id IS NOT NULL;

-- Prompt version active check
CREATE INDEX idx_prompt_version_active ON prompt_version (prompt_id) WHERE is_active = TRUE;

-- LLM log analysis performance
CREATE INDEX idx_llm_logs_called ON llm_logs (called_at DESC);
CREATE INDEX idx_llm_logs_analysis_type ON llm_logs (analysis_type);

-- Dynamic metric filtering
CREATE INDEX idx_submission_metrics_code ON submission_metrics (metric_code);

-- -- =========================================================================
-- -- EXPANDED VIEWS (To bypass JOINs in BI tools like Metabase)
-- -- =========================================================================

-- CREATE OR REPLACE VIEW story_submissions_expanded AS
-- SELECT 
--     s.id AS submission_uuid,
--     ss.id AS story_payload_uuid,
--     ss.submission_id,
--     ss.tenant_code,
--     s.session_id,
--     s.submission_type,
--     s.user_id,
--     s.user_name,
--     s.role,
--     s.state,
--     s.district,
--     s.organization,
--     s.submission_date,
--     s.status AS ingestion_status,
--     s.program_id,
--     s.leader_id,
--     ss.title,
--     ss.objective,
--     ss.challenge,
--     ss.action_steps,
--     ss.impact,
--     ss.duration,
--     ss.blurb,
--     ss.content,
--     ss.image_urls,
--     ss.blur_image_urls,
--     ss.pdf_urls,
--     ss.transcript_link,
--     ss.pii_masked,
--     ss.created_at,
--     ss.updated_at
-- FROM story_submissions ss
-- JOIN submissions s 
--   ON ss.submission_id = s.submission_id 
--  AND ss.tenant_code = s.tenant_code;

-- CREATE OR REPLACE VIEW discussion_submissions_expanded AS
-- SELECT 
--     s.id AS submission_uuid,
--     ds.id AS discussion_payload_uuid,
--     ds.submission_id,
--     ds.tenant_code,
--     s.session_id,
--     s.submission_type,
--     s.user_id,
--     s.user_name,
--     s.role,
--     s.state,
--     s.district,
--     s.organization,
--     s.submission_date,
--     s.status AS ingestion_status,
--     s.program_id,
--     s.leader_id,
--     ds.title,
--     ds.challenges,
--     ds.solutions,
--     ds.author,
--     ds.language,
--     ds.image_urls,
--     ds.blur_image_urls,
--     ds.pdf_urls,
--     ds.transcript_link,
--     ds.pii_masked,
--     ds.created_at,
--     ds.updated_at
-- FROM discussion_submissions ds
-- JOIN submissions s 
--   ON ds.submission_id = s.submission_id 
--  AND ds.tenant_code = s.tenant_code;
