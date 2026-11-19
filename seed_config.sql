-- =========================================================================
-- SEED CONFIG: the versioned settings stored in the `config` / `config_version` tables, plus the
-- leader categories and programs those configs belong to. Run by app/database/db.py at every start,
-- after schema.sql and the other seed files (it depends on none of them).
--
-- This file is DATA: the settings of the first program, 1 Million Voices, tenant 'mitra'. `feedback` is a generic
-- report type: another program, tenant, source (a free [a-z0-9_] label), persona or CSV shape is added as more
-- config rows (the 1MV blocks below are the pattern to copy); nothing here is a limit of the platform. Every
-- feedback_upload config names its `adapter`: 'conversation_rows' (one row per message; its CSV headers are the
-- explicit `column_map`, its status words the `completion_map`) or 'wide_form' (one row per respondent).
--
-- Today: the 1M Voice feedback settings of tenant 'mitra', for the four personas (Youth, Parent,
-- General Public, Educator):
--   * feedback_upload      / mitra         adapter conversation_rows: how to read a Mitra raw conversation CSV
--                                          (one per persona)
--   * feedback_upload      / google_forms  adapter wide_form: how to read a Google Forms wide CSV (Youth, Parent,
--                                          General Public)
--   * feedback_extraction  / any           the metadata fields and bands of the LLM extraction and the
--                                          prompt it uses (prompt_name, a row of seed_prompts.sql)
-- Other kinds of config are added to this file in the same pattern (one CONFIG block each).
--
-- Rules every block follows (the file is a seed, never a migration):
--   * It creates the tenant 'mitra' when this database does not have it yet (section 0), then everything that
--     belongs to it. An existing tenant is a NO-OP: it is never touched or renamed.
--   * Nothing is ever overwritten. leader_category / programs / config are inserted ON CONFLICT DO NOTHING
--     under fixed ids; version 1 of a config is inserted ONLY while that config has no version row at
--     all, so a config edited with a new config_version (old one deactivated, kept as history), or one
--     whose version 1 was removed, is never touched and the one-active-version-per-config unique index
--     is never violated. It is idempotent: running it again changes nothing.
--   * The ids are fixed (the personas' ids; the extraction config ids are uuid5 of the config type and
--     the session_type) so every database holds the same rows.
--
-- A database that is already seeded keeps its configs. To change a seeded config there, write a NEW
-- config_version (see docs/feedback-extraction-bands.md, "Changing a band or a field"); to make a fresh
-- database start with the change, edit the JSON of the block below. Only the JSON inside the
-- dollar-quoted literal of a block is config content; each block starts with the marker line
-- `-- CONFIG <config_type> <config_source> <session_type>` (tests/seed_support.py reads the file through
-- these markers, so keep the layout of a block as it is).
-- =========================================================================
-- =========================================================================
-- 0. The tenant (created here when this database does not have it yet; an existing tenant is never touched.
--    ON CONFLICT DO NOTHING without a column also ignores a clash on the unique name, so it can never fail the start)
-- =========================================================================

INSERT INTO tenant (name, code, created_by)
VALUES ('Mitra', 'mitra', 'seed')
ON CONFLICT DO NOTHING;

-- =========================================================================
-- 1. Leader categories and programs (one per persona; the program is named "1M Voice")
-- =========================================================================

-- PERSONA Youth Youth_Voice
INSERT INTO leader_category (id, name, tenant_code)
SELECT '5f3d1f0f-f472-47af-8c55-66e10aa48c43'::uuid, 'Youth', 'mitra'
WHERE EXISTS (SELECT 1 FROM tenant WHERE code = 'mitra')
ON CONFLICT (id) DO NOTHING;

INSERT INTO programs (id, leaders_id, name, tenant_code)
SELECT 'dcd67b14-f0ac-4e71-87fd-fa818c8f2d6a'::uuid, '5f3d1f0f-f472-47af-8c55-66e10aa48c43'::uuid, '1M Voice', 'mitra'
WHERE EXISTS (SELECT 1 FROM tenant WHERE code = 'mitra')
  AND EXISTS (SELECT 1 FROM leader_category WHERE id = '5f3d1f0f-f472-47af-8c55-66e10aa48c43'::uuid)
ON CONFLICT (id) DO NOTHING;

-- PERSONA Parent Parent_Voice
INSERT INTO leader_category (id, name, tenant_code)
SELECT '99e4b68a-ea5e-432d-8282-c4d79d3f62e8'::uuid, 'Parent', 'mitra'
WHERE EXISTS (SELECT 1 FROM tenant WHERE code = 'mitra')
ON CONFLICT (id) DO NOTHING;

INSERT INTO programs (id, leaders_id, name, tenant_code)
SELECT '31dae489-5647-4d0b-91cb-1c5f53dba4ae'::uuid, '99e4b68a-ea5e-432d-8282-c4d79d3f62e8'::uuid, '1M Voice', 'mitra'
WHERE EXISTS (SELECT 1 FROM tenant WHERE code = 'mitra')
  AND EXISTS (SELECT 1 FROM leader_category WHERE id = '99e4b68a-ea5e-432d-8282-c4d79d3f62e8'::uuid)
ON CONFLICT (id) DO NOTHING;

-- PERSONA General Public General_Public_Voice
INSERT INTO leader_category (id, name, tenant_code)
SELECT 'f7cf3db1-55bd-4940-be70-556ca14c49b4'::uuid, 'General Public', 'mitra'
WHERE EXISTS (SELECT 1 FROM tenant WHERE code = 'mitra')
ON CONFLICT (id) DO NOTHING;

INSERT INTO programs (id, leaders_id, name, tenant_code)
SELECT 'bc53156c-bcb9-4332-b817-f6c5ef4a0640'::uuid, 'f7cf3db1-55bd-4940-be70-556ca14c49b4'::uuid, '1M Voice', 'mitra'
WHERE EXISTS (SELECT 1 FROM tenant WHERE code = 'mitra')
  AND EXISTS (SELECT 1 FROM leader_category WHERE id = 'f7cf3db1-55bd-4940-be70-556ca14c49b4'::uuid)
ON CONFLICT (id) DO NOTHING;

-- PERSONA Educator Educator_Voice
INSERT INTO leader_category (id, name, tenant_code)
SELECT '117e0bb9-c0de-414a-a7f0-5ee62e48477e'::uuid, 'Educator', 'mitra'
WHERE EXISTS (SELECT 1 FROM tenant WHERE code = 'mitra')
ON CONFLICT (id) DO NOTHING;

INSERT INTO programs (id, leaders_id, name, tenant_code)
SELECT 'b09ca040-f2cb-4c31-b0a8-07ce0acb69a2'::uuid, '117e0bb9-c0de-414a-a7f0-5ee62e48477e'::uuid, '1M Voice', 'mitra'
WHERE EXISTS (SELECT 1 FROM tenant WHERE code = 'mitra')
  AND EXISTS (SELECT 1 FROM leader_category WHERE id = '117e0bb9-c0de-414a-a7f0-5ee62e48477e'::uuid)
ON CONFLICT (id) DO NOTHING;

-- =========================================================================
-- 2. Upload configs: how a CSV is read (feedback_upload)
-- =========================================================================

-- CONFIG feedback_upload mitra Youth_Voice
INSERT INTO config (id, config_source, name, config_type, program_id, tenant_code, leader_id, session_type, bot_id, bot_name)
SELECT '864bd1a7-3f34-473e-8578-65c801908657'::uuid, 'mitra', 'Youth bot CSV validator', 'feedback_upload', 'dcd67b14-f0ac-4e71-87fd-fa818c8f2d6a'::uuid, 'mitra', NULL, 'Youth_Voice', '90', '1MV Youth Voice'
WHERE EXISTS (SELECT 1 FROM tenant WHERE code = 'mitra')
  AND EXISTS (SELECT 1 FROM programs WHERE id = 'dcd67b14-f0ac-4e71-87fd-fa818c8f2d6a'::uuid)
ON CONFLICT DO NOTHING;

INSERT INTO config_version (config_id, version, is_active, config, created_by, change_note)
SELECT '864bd1a7-3f34-473e-8578-65c801908657'::uuid, 1, TRUE, $cfg$
{
  "session_type": "Youth_Voice",
  "adapter": "conversation_rows",
  "column_map": {
    "session_type": "session_type",
    "session_status": "session_status",
    "session": "session",
    "submission_id": "submission_id",
    "language": "language",
    "sequence": "sequence",
    "message": "message",
    "created_at": "created_at",
    "stage": "stage",
    "sender_id": "sender_id",
    "bot_id": "bot_id",
    "bot_name": "bot_name",
    "translated_message": "translated_message",
    "conversation_row_id": "conversation_row_id",
    "receiver_id": "receiver_id",
    "updated_at": "updated_at"
  },
  "opening_row": {
    "mode": "auto"
  },
  "questionnaire_versions": [
    {
      "key": "V1",
      "effective_from": null
    }
  ],
  "bot_sender_ids": [
    "1"
  ],
  "message_map": {
    "bot": {
      "message": "translated_message",
      "translated_message": "message"
    },
    "respondent": {
      "message": "message",
      "translated_message": "translated_message"
    }
  },
  "sequence_offset": 1,
  "completion_map": {
    "COMPLETED": "COMPLETED",
    "IN_PROGRESS": "IN_PROGRESS"
  },
  "closing_keys": [
    "THANK_YOU",
    "THANKYOU",
    "CLOSING"
  ],
  "datetime": {
    "fallback_timezone": "Asia/Kolkata"
  },
  "questions": [
    {
      "key": "AGE_IDENTIFICATION",
      "text": "Can you please tell me your age?"
    }
  ],
  "max_csv_upload_bytes": 104857600,
  "ingestion_schema": {
    "create": {
      "required": [
        "submissionId",
        "submissionType",
        "sessionId",
        "tenantCode",
        "eventType",
        "eventPublishedAt",
        "tags.programId",
        "tags.leaderCategoryId",
        "data.source",
        "data.sessionType",
        "data.questionnaireVersion",
        "data.language",
        "data.completionStatus",
        "data.submissionDate",
        "data.messages"
      ],
      "optional": [
        "tags.state",
        "tags.district",
        "tags.programName",
        "tags.leaderCategoryName",
        "tags.source",
        "data.botId",
        "data.botName"
      ]
    },
    "update": {
      "required": [
        "submissionId",
        "submissionType",
        "sessionId",
        "tenantCode",
        "eventType",
        "eventPublishedAt"
      ],
      "newValuesNoEmpty": true
    },
    "delete": {
      "required": [
        "submissionId",
        "submissionType",
        "sessionId",
        "tenantCode",
        "eventType",
        "eventPublishedAt"
      ]
    }
  },
  "process_steps": [
    {
      "name": "feedback_metadata_extraction"
    }
  ]
}
$cfg$::jsonb, 'seed', 'Initial format'
WHERE EXISTS (SELECT 1 FROM config WHERE id = '864bd1a7-3f34-473e-8578-65c801908657'::uuid)
  AND NOT EXISTS (SELECT 1 FROM config_version WHERE config_id = '864bd1a7-3f34-473e-8578-65c801908657'::uuid)
ON CONFLICT (config_id, version) DO NOTHING;

-- CONFIG feedback_upload google_forms Youth_Voice
INSERT INTO config (id, config_source, name, config_type, program_id, tenant_code, leader_id, session_type, bot_id, bot_name)
SELECT '957b809a-028a-4139-8455-881d24696eb7'::uuid, 'google_forms', 'Youth form CSV validator', 'feedback_upload', 'dcd67b14-f0ac-4e71-87fd-fa818c8f2d6a'::uuid, 'mitra', NULL, 'Youth_Voice', NULL, NULL
WHERE EXISTS (SELECT 1 FROM tenant WHERE code = 'mitra')
  AND EXISTS (SELECT 1 FROM programs WHERE id = 'dcd67b14-f0ac-4e71-87fd-fa818c8f2d6a'::uuid)
ON CONFLICT DO NOTHING;

INSERT INTO config_version (config_id, version, is_active, config, created_by, change_note)
SELECT '957b809a-028a-4139-8455-881d24696eb7'::uuid, 1, TRUE, $cfg$
{
  "session_type": "Youth_Voice",
  "adapter": "wide_form",
  "timestamp_column": "Timestamp",
  "datetime": {
    "formats": [
      "%m/%d/%Y %H:%M:%S"
    ],
    "fallback_timezone": "Asia/Kolkata"
  },
  "default_completion": "COMPLETED",
  "ignored_columns": [
    "Email Address"
  ],
  "ignored_column_patterns": [
    "^Column \\d+$"
  ],
  "language": {
    "default": "en",
    "script_detect": {
      "kn": "[\\u0C80-\\u0CFF]"
    },
    "detect_kinds": [
      "open"
    ]
  },
  "forms": {
    "youth_launch": {
      "name": "Youth (English Launch and Karnataka V-Launch)",
      "questions": [
        {
          "headers": [
            "How old are you?"
          ],
          "question_key": "AGE_IDENTIFICATION",
          "text": "How old are you?",
          "kind": "metadata"
        },
        {
          "headers": [
            "What is your gender?"
          ],
          "question_key": "GENDER_IDENTIFICATION",
          "text": "What is your gender?",
          "kind": "metadata"
        },
        {
          "headers": [
            "Which state do you stay in?"
          ],
          "question_key": "STATE_IDENTIFICATION",
          "text": "Which state do you stay in?",
          "kind": "metadata"
        },
        {
          "headers": [
            "Which district do you stay in?"
          ],
          "question_key": "DISTRICT_IDENTIFICATION",
          "text": "Which district do you stay in?",
          "kind": "metadata"
        },
        {
          "headers": [
            "Right now, are you studying, working, or doing something else?"
          ],
          "question_key": "CURRENT_STATUS",
          "text": "Right now, are you studying, working, or doing something else?",
          "kind": "metadata"
        },
        {
          "headers": [
            "If you are studying, what class/ course are you studying?"
          ],
          "question_key": "CURRENT_STATUS",
          "text": "If you are studying, what class/ course are you studying?",
          "kind": "metadata"
        },
        {
          "headers": [
            "Did you or are you attending a government, government-aided or private education institution?"
          ],
          "question_key": "SCHOOL_TYPE",
          "text": "Did you or are you attending a government, government-aided or private education institution?",
          "kind": "metadata"
        },
        {
          "headers": [
            "What is your average annual household income?"
          ],
          "question_key": "HOUSEHOLD_INCOME",
          "text": "What is your average annual household income?",
          "kind": "metadata"
        },
        {
          "headers": [
            "Do you have any disabilities?"
          ],
          "question_key": "DISABILITY_STATUS",
          "text": "Do you have any disabilities?",
          "kind": "metadata"
        },
        {
          "headers": [
            "If you said yes to the above question and if you are comfortable sharing, what kind of disability it is?"
          ],
          "question_key": "DISABILITY_STATUS",
          "text": "If you said yes to the above question and if you are comfortable sharing, what kind of disability it is?",
          "kind": "metadata"
        },
        {
          "headers": [
            "When you think about your future or when you grow up, what do you want your life and work to look like?"
          ],
          "question_key": "FUTURE_LIFE_AND_WORK_VISION",
          "text": "When you think about your future or when you grow up, what do you want your life and work to look like?",
          "kind": "open"
        },
        {
          "headers": [
            "Do you feel confident that you can achieve the work and life you want?"
          ],
          "question_key": "CONFIDENCE",
          "text": "Do you feel confident that you can achieve the work and life you want?",
          "kind": "open"
        },
        {
          "headers": [
            "What do you think you have to learn or do to achieve the future you want?"
          ],
          "question_key": "LEARNING_NEEDS_FOR_FUTURE",
          "text": "What do you think you have to learn or do to achieve the future you want?",
          "kind": "open"
        },
        {
          "headers": [
            "For the things you just mentioned, are you getting the chance to learn them right now?",
            "Out of the things you just mentioned, are you getting the chance to learn them right now?"
          ],
          "question_key": "CHANCE_TO_LEARN_NOW",
          "text": "For the things you just mentioned, are you getting the chance to learn them right now?",
          "kind": "open"
        },
        {
          "headers": [
            "Can you give examples of what or who helped you learn and build these skills?"
          ],
          "question_key": "CHANCE_TO_LEARN_NOW",
          "text": "Can you give examples of what or who helped you learn and build these skills?",
          "kind": "open"
        },
        {
          "headers": [
            "What more could your school, college, community, friends, or family do to support you?"
          ],
          "question_key": "SUPPORT_FROM_ENVIRONMENT",
          "text": "What more could your school, college, community, friends, or family do to support you?",
          "kind": "open"
        },
        {
          "headers": [
            "When you have questions about how to prepare for your future, who do you talk to or where do you get this information from?"
          ],
          "question_key": "ADVICE_AND_INFORMATION_SOURCES",
          "text": "When you have questions about how to prepare for your future, who do you talk to or where do you get this information from?",
          "kind": "open"
        },
        {
          "headers": [
            "Is there something you would like to do to help make learning and education better for young people like you in the future?"
          ],
          "question_key": "ADDITIONAL_COMMENTS",
          "text": "Is there something you would like to do to help make learning and education better for young people like you in the future?",
          "kind": "open"
        }
      ],
      "questionnaire_version": "V1"
    }
  },
  "max_csv_upload_bytes": 104857600,
  "ingestion_schema": {
    "create": {
      "required": [
        "submissionId",
        "submissionType",
        "sessionId",
        "tenantCode",
        "eventType",
        "eventPublishedAt",
        "tags.programId",
        "tags.leaderCategoryId",
        "data.source",
        "data.sessionType",
        "data.questionnaireVersion",
        "data.language",
        "data.completionStatus",
        "data.submissionDate",
        "data.messages"
      ],
      "optional": [
        "tags.state",
        "tags.district",
        "tags.programName",
        "tags.leaderCategoryName",
        "tags.source",
        "data.botId",
        "data.botName"
      ]
    },
    "update": {
      "required": [
        "submissionId",
        "submissionType",
        "sessionId",
        "tenantCode",
        "eventType",
        "eventPublishedAt"
      ],
      "newValuesNoEmpty": true
    },
    "delete": {
      "required": [
        "submissionId",
        "submissionType",
        "sessionId",
        "tenantCode",
        "eventType",
        "eventPublishedAt"
      ]
    }
  },
  "process_steps": [
    {
      "name": "feedback_metadata_extraction"
    }
  ]
}
$cfg$::jsonb, 'seed', 'Initial format'
WHERE EXISTS (SELECT 1 FROM config WHERE id = '957b809a-028a-4139-8455-881d24696eb7'::uuid)
  AND NOT EXISTS (SELECT 1 FROM config_version WHERE config_id = '957b809a-028a-4139-8455-881d24696eb7'::uuid)
ON CONFLICT (config_id, version) DO NOTHING;

-- CONFIG feedback_upload mitra Parent_Voice
INSERT INTO config (id, config_source, name, config_type, program_id, tenant_code, leader_id, session_type, bot_id, bot_name)
SELECT 'aa3bf372-219a-405f-b13d-caef9becad68'::uuid, 'mitra', 'Parent bot CSV validator', 'feedback_upload', '31dae489-5647-4d0b-91cb-1c5f53dba4ae'::uuid, 'mitra', NULL, 'Parent_Voice', '88', '1MV Parent Voice'
WHERE EXISTS (SELECT 1 FROM tenant WHERE code = 'mitra')
  AND EXISTS (SELECT 1 FROM programs WHERE id = '31dae489-5647-4d0b-91cb-1c5f53dba4ae'::uuid)
ON CONFLICT DO NOTHING;

INSERT INTO config_version (config_id, version, is_active, config, created_by, change_note)
SELECT 'aa3bf372-219a-405f-b13d-caef9becad68'::uuid, 1, TRUE, $cfg$
{
  "session_type": "Parent_Voice",
  "adapter": "conversation_rows",
  "column_map": {
    "session_type": "session_type",
    "session_status": "session_status",
    "session": "session",
    "submission_id": "submission_id",
    "language": "language",
    "sequence": "sequence",
    "message": "message",
    "created_at": "created_at",
    "stage": "stage",
    "sender_id": "sender_id",
    "bot_id": "bot_id",
    "bot_name": "bot_name",
    "translated_message": "translated_message",
    "conversation_row_id": "conversation_row_id",
    "receiver_id": "receiver_id",
    "updated_at": "updated_at"
  },
  "opening_row": {
    "mode": "auto"
  },
  "questionnaire_versions": [
    {
      "key": "V1",
      "effective_from": null
    }
  ],
  "bot_sender_ids": [
    "1"
  ],
  "message_map": {
    "bot": {
      "message": "translated_message",
      "translated_message": "message"
    },
    "respondent": {
      "message": "message",
      "translated_message": "translated_message"
    }
  },
  "sequence_offset": 1,
  "completion_map": {
    "COMPLETED": "COMPLETED",
    "IN_PROGRESS": "IN_PROGRESS"
  },
  "closing_keys": [
    "THANK_YOU",
    "THANKYOU",
    "CLOSING"
  ],
  "datetime": {
    "fallback_timezone": "Asia/Kolkata"
  },
  "questions": [
    {
      "key": "STATE_IDENTIFICATION",
      "text": "Which state do you stay in?"
    }
  ],
  "max_csv_upload_bytes": 104857600,
  "ingestion_schema": {
    "create": {
      "required": [
        "submissionId",
        "submissionType",
        "sessionId",
        "tenantCode",
        "eventType",
        "eventPublishedAt",
        "tags.programId",
        "tags.leaderCategoryId",
        "data.source",
        "data.sessionType",
        "data.questionnaireVersion",
        "data.language",
        "data.completionStatus",
        "data.submissionDate",
        "data.messages"
      ],
      "optional": [
        "tags.state",
        "tags.district",
        "tags.programName",
        "tags.leaderCategoryName",
        "tags.source",
        "data.botId",
        "data.botName"
      ]
    },
    "update": {
      "required": [
        "submissionId",
        "submissionType",
        "sessionId",
        "tenantCode",
        "eventType",
        "eventPublishedAt"
      ],
      "newValuesNoEmpty": true
    },
    "delete": {
      "required": [
        "submissionId",
        "submissionType",
        "sessionId",
        "tenantCode",
        "eventType",
        "eventPublishedAt"
      ]
    }
  },
  "process_steps": [
    {
      "name": "feedback_metadata_extraction"
    }
  ]
}
$cfg$::jsonb, 'seed', 'Initial format'
WHERE EXISTS (SELECT 1 FROM config WHERE id = 'aa3bf372-219a-405f-b13d-caef9becad68'::uuid)
  AND NOT EXISTS (SELECT 1 FROM config_version WHERE config_id = 'aa3bf372-219a-405f-b13d-caef9becad68'::uuid)
ON CONFLICT (config_id, version) DO NOTHING;

-- CONFIG feedback_upload google_forms Parent_Voice
INSERT INTO config (id, config_source, name, config_type, program_id, tenant_code, leader_id, session_type, bot_id, bot_name)
SELECT '34bd6a47-1f76-4760-bf45-bea23bea72be'::uuid, 'google_forms', 'Parent form CSV validator', 'feedback_upload', '31dae489-5647-4d0b-91cb-1c5f53dba4ae'::uuid, 'mitra', NULL, 'Parent_Voice', NULL, NULL
WHERE EXISTS (SELECT 1 FROM tenant WHERE code = 'mitra')
  AND EXISTS (SELECT 1 FROM programs WHERE id = '31dae489-5647-4d0b-91cb-1c5f53dba4ae'::uuid)
ON CONFLICT DO NOTHING;

INSERT INTO config_version (config_id, version, is_active, config, created_by, change_note)
SELECT '34bd6a47-1f76-4760-bf45-bea23bea72be'::uuid, 1, TRUE, $cfg$
{
  "session_type": "Parent_Voice",
  "adapter": "wide_form",
  "timestamp_column": "Timestamp",
  "datetime": {
    "formats": [
      "%m/%d/%Y %H:%M:%S"
    ],
    "fallback_timezone": "Asia/Kolkata"
  },
  "default_completion": "COMPLETED",
  "ignored_columns": [
    "Email Address"
  ],
  "ignored_column_patterns": [
    "^Column \\d+$"
  ],
  "language": {
    "default": "en",
    "script_detect": {
      "kn": "[\\u0C80-\\u0CFF]"
    },
    "detect_kinds": [
      "open"
    ]
  },
  "forms": {
    "parents_english_launch": {
      "name": "Parents (English Launch)",
      "questions": [
        {
          "headers": [
            "Which State do you stay in?"
          ],
          "question_key": "STATE_IDENTIFICATION",
          "text": "Which State do you stay in?",
          "kind": "metadata"
        },
        {
          "headers": [
            "Please tell us the name of the district where you child attends school"
          ],
          "question_key": "DISTRICT_IDENTIFICATION",
          "text": "Please tell us the name of the district where you child attends school",
          "kind": "metadata"
        },
        {
          "headers": [
            "Are you the child's mother, father or guardian?"
          ],
          "question_key": "RESPONDENT_RELATION",
          "text": "Are you the child's mother, father or guardian?",
          "kind": "metadata"
        },
        {
          "headers": [
            "Does your child study in government school or private school?"
          ],
          "question_key": "SCHOOL_TYPE",
          "text": "Does your child study in government school or private school?",
          "kind": "metadata"
        },
        {
          "headers": [
            "In which class/grade does your child study?"
          ],
          "question_key": "CHILD_CLASS",
          "text": "In which class/grade does your child study?",
          "kind": "metadata"
        },
        {
          "headers": [
            "Did you get an opportunity to go to school? If yes, up to which class did you study?"
          ],
          "question_key": "PARENT_EDUCATION",
          "text": "Did you get an opportunity to go to school? If yes, up to which class did you study?",
          "kind": "metadata"
        },
        {
          "headers": [
            "Over the last 6 months, how many times have you visited your child's school?"
          ],
          "question_key": "SCHOOL_VISIT_FREQUENCY",
          "text": "Over the last 6 months, how many times have you visited your child's school?",
          "kind": "open"
        },
        {
          "headers": [
            "Why do you usually visit your child's school?"
          ],
          "question_key": "SCHOOL_VISIT_REASONS",
          "text": "Why do you usually visit your child's school?",
          "kind": "open"
        },
        {
          "headers": [
            "How satisfied are you with how your child is doing in studies? Please provide reasons why you think so"
          ],
          "question_key": "ACADEMIC_PROGRESS_SATISFACTION",
          "text": "How satisfied are you with how your child is doing in studies? Please provide reasons why you think so",
          "kind": "open"
        },
        {
          "headers": [
            "When your child grows up, what kind of work and life do you wish they will have?"
          ],
          "question_key": "FUTURE_ASPIRATION",
          "text": "When your child grows up, what kind of work and life do you wish they will have?",
          "kind": "open"
        },
        {
          "headers": [
            "What are 2-3 things you would like to see improved in your child's school/college, to better prepare them for their future?"
          ],
          "question_key": "SCHOOL_IMPROVEMENTS",
          "text": "What are 2-3 things you would like to see improved in your child's school/college, to better prepare them for their future?",
          "kind": "open"
        },
        {
          "headers": [
            "What are 2-3 things that will make the school more safe and welcoming for your child?"
          ],
          "question_key": "SAFE_AND_WELCOMING_SCHOOL",
          "text": "What are 2-3 things that will make the school more safe and welcoming for your child?",
          "kind": "open"
        },
        {
          "headers": [
            "What all do you do at home to support your child's learning?"
          ],
          "question_key": "HOME_LEARNING_SUPPORT",
          "text": "What all do you do at home to support your child's learning?",
          "kind": "open"
        },
        {
          "headers": [
            "Many parents find it hard to help their child's education. Is it hard or easy for you? Why or why not?"
          ],
          "question_key": "SUPPORT_EASE",
          "text": "Many parents find it hard to help their child's education. Is it hard or easy for you? Why or why not?",
          "kind": "open"
        },
        {
          "headers": [
            "What is one thing that would help your child most to reach the future they want?"
          ],
          "question_key": "KEY_SUPPORT_FOR_FUTURE",
          "text": "What is one thing that would help your child most to reach the future they want?",
          "kind": "open"
        }
      ],
      "questionnaire_version": "V1"
    }
  },
  "max_csv_upload_bytes": 104857600,
  "ingestion_schema": {
    "create": {
      "required": [
        "submissionId",
        "submissionType",
        "sessionId",
        "tenantCode",
        "eventType",
        "eventPublishedAt",
        "tags.programId",
        "tags.leaderCategoryId",
        "data.source",
        "data.sessionType",
        "data.questionnaireVersion",
        "data.language",
        "data.completionStatus",
        "data.submissionDate",
        "data.messages"
      ],
      "optional": [
        "tags.state",
        "tags.district",
        "tags.programName",
        "tags.leaderCategoryName",
        "tags.source",
        "data.botId",
        "data.botName"
      ]
    },
    "update": {
      "required": [
        "submissionId",
        "submissionType",
        "sessionId",
        "tenantCode",
        "eventType",
        "eventPublishedAt"
      ],
      "newValuesNoEmpty": true
    },
    "delete": {
      "required": [
        "submissionId",
        "submissionType",
        "sessionId",
        "tenantCode",
        "eventType",
        "eventPublishedAt"
      ]
    }
  },
  "process_steps": [
    {
      "name": "feedback_metadata_extraction"
    }
  ]
}
$cfg$::jsonb, 'seed', 'Initial format'
WHERE EXISTS (SELECT 1 FROM config WHERE id = '34bd6a47-1f76-4760-bf45-bea23bea72be'::uuid)
  AND NOT EXISTS (SELECT 1 FROM config_version WHERE config_id = '34bd6a47-1f76-4760-bf45-bea23bea72be'::uuid)
ON CONFLICT (config_id, version) DO NOTHING;

-- CONFIG feedback_upload mitra General_Public_Voice
INSERT INTO config (id, config_source, name, config_type, program_id, tenant_code, leader_id, session_type, bot_id, bot_name)
SELECT 'c19ca45c-7cc5-4fea-a157-c8f508b4c592'::uuid, 'mitra', 'General bot CSV validator', 'feedback_upload', 'bc53156c-bcb9-4332-b817-f6c5ef4a0640'::uuid, 'mitra', NULL, 'General_Public_Voice', '89', '1MV General voice'
WHERE EXISTS (SELECT 1 FROM tenant WHERE code = 'mitra')
  AND EXISTS (SELECT 1 FROM programs WHERE id = 'bc53156c-bcb9-4332-b817-f6c5ef4a0640'::uuid)
ON CONFLICT DO NOTHING;

INSERT INTO config_version (config_id, version, is_active, config, created_by, change_note)
SELECT 'c19ca45c-7cc5-4fea-a157-c8f508b4c592'::uuid, 1, TRUE, $cfg$
{
  "session_type": "General_Public_Voice",
  "adapter": "conversation_rows",
  "column_map": {
    "session_type": "session_type",
    "session_status": "session_status",
    "session": "session",
    "submission_id": "submission_id",
    "language": "language",
    "sequence": "sequence",
    "message": "message",
    "created_at": "created_at",
    "stage": "stage",
    "sender_id": "sender_id",
    "bot_id": "bot_id",
    "bot_name": "bot_name",
    "translated_message": "translated_message",
    "conversation_row_id": "conversation_row_id",
    "receiver_id": "receiver_id",
    "updated_at": "updated_at"
  },
  "opening_row": {
    "mode": "auto"
  },
  "questionnaire_versions": [
    {
      "key": "V1",
      "effective_from": null
    }
  ],
  "bot_sender_ids": [
    "1"
  ],
  "message_map": {
    "bot": {
      "message": "translated_message",
      "translated_message": "message"
    },
    "respondent": {
      "message": "message",
      "translated_message": "translated_message"
    }
  },
  "sequence_offset": 1,
  "completion_map": {
    "COMPLETED": "COMPLETED",
    "IN_PROGRESS": "IN_PROGRESS"
  },
  "closing_keys": [
    "THANK_YOU",
    "THANKYOU",
    "CLOSING"
  ],
  "datetime": {
    "fallback_timezone": "Asia/Kolkata"
  },
  "questions": [
    {
      "key": "AGE_IDENTIFICATION",
      "text": "Can you please tell me your age?"
    }
  ],
  "max_csv_upload_bytes": 104857600,
  "ingestion_schema": {
    "create": {
      "required": [
        "submissionId",
        "submissionType",
        "sessionId",
        "tenantCode",
        "eventType",
        "eventPublishedAt",
        "tags.programId",
        "tags.leaderCategoryId",
        "data.source",
        "data.sessionType",
        "data.questionnaireVersion",
        "data.language",
        "data.completionStatus",
        "data.submissionDate",
        "data.messages"
      ],
      "optional": [
        "tags.state",
        "tags.district",
        "tags.programName",
        "tags.leaderCategoryName",
        "tags.source",
        "data.botId",
        "data.botName"
      ]
    },
    "update": {
      "required": [
        "submissionId",
        "submissionType",
        "sessionId",
        "tenantCode",
        "eventType",
        "eventPublishedAt"
      ],
      "newValuesNoEmpty": true
    },
    "delete": {
      "required": [
        "submissionId",
        "submissionType",
        "sessionId",
        "tenantCode",
        "eventType",
        "eventPublishedAt"
      ]
    }
  },
  "process_steps": [
    {
      "name": "feedback_metadata_extraction"
    }
  ]
}
$cfg$::jsonb, 'seed', 'Initial format'
WHERE EXISTS (SELECT 1 FROM config WHERE id = 'c19ca45c-7cc5-4fea-a157-c8f508b4c592'::uuid)
  AND NOT EXISTS (SELECT 1 FROM config_version WHERE config_id = 'c19ca45c-7cc5-4fea-a157-c8f508b4c592'::uuid)
ON CONFLICT (config_id, version) DO NOTHING;

-- CONFIG feedback_upload google_forms General_Public_Voice
INSERT INTO config (id, config_source, name, config_type, program_id, tenant_code, leader_id, session_type, bot_id, bot_name)
SELECT '6543fdb5-8d6e-4929-ae6e-576d7237f38a'::uuid, 'google_forms', 'General Public form CSV validator', 'feedback_upload', 'bc53156c-bcb9-4332-b817-f6c5ef4a0640'::uuid, 'mitra', NULL, 'General_Public_Voice', NULL, NULL
WHERE EXISTS (SELECT 1 FROM tenant WHERE code = 'mitra')
  AND EXISTS (SELECT 1 FROM programs WHERE id = 'bc53156c-bcb9-4332-b817-f6c5ef4a0640'::uuid)
ON CONFLICT DO NOTHING;

INSERT INTO config_version (config_id, version, is_active, config, created_by, change_note)
SELECT '6543fdb5-8d6e-4929-ae6e-576d7237f38a'::uuid, 1, TRUE, $cfg$
{
  "session_type": "General_Public_Voice",
  "adapter": "wide_form",
  "timestamp_column": "Timestamp",
  "datetime": {
    "formats": [
      "%m/%d/%Y %H:%M:%S"
    ],
    "fallback_timezone": "Asia/Kolkata"
  },
  "default_completion": "COMPLETED",
  "ignored_columns": [
    "Email Address"
  ],
  "ignored_column_patterns": [
    "^Column \\d+$"
  ],
  "language": {
    "default": "en",
    "script_detect": {
      "kn": "[\\u0C80-\\u0CFF]"
    },
    "detect_kinds": [
      "open"
    ]
  },
  "forms": {
    "general_public_english_launch": {
      "name": "General Public (English launch)",
      "questions": [
        {
          "headers": [
            "How old are you?"
          ],
          "question_key": "AGE_IDENTIFICATION",
          "text": "How old are you?",
          "kind": "metadata"
        },
        {
          "headers": [
            "What is your gender?"
          ],
          "question_key": "GENDER_IDENTIFICATION",
          "text": "What is your gender?",
          "kind": "metadata"
        },
        {
          "headers": [
            "Which state do you stay in?"
          ],
          "question_key": "STATE_IDENTIFICATION",
          "text": "Which state do you stay in?",
          "kind": "metadata"
        },
        {
          "headers": [
            "Which district do you stay in?"
          ],
          "question_key": "DISTRICT_IDENTIFICATION",
          "text": "Which district do you stay in?",
          "kind": "metadata"
        },
        {
          "headers": [
            "What do you currently do - your work or occupation?"
          ],
          "question_key": "CURRENT_STATUS",
          "text": "What do you currently do - your work or occupation?",
          "kind": "metadata"
        },
        {
          "headers": [
            "How is life different for young people today compared to when you were younger?"
          ],
          "question_key": "DEFFERENCE",
          "text": "How is life different for young people today compared to when you were younger?",
          "kind": "open"
        },
        {
          "headers": [
            "What do you think young people need to learn or experience to thrive in the future?"
          ],
          "question_key": "LEARNING_NEEDS_FOR_FUTURE",
          "text": "What do you think young people need to learn or experience to thrive in the future?",
          "kind": "open"
        },
        {
          "headers": [
            "Where do you think they can get these skills and experiences?"
          ],
          "question_key": "LEARNING_NEEDS_FOR_FUTURE",
          "text": "Where do you think they can get these skills and experiences?",
          "kind": "open"
        },
        {
          "headers": [
            "What is your one biggest hope for young people around you?"
          ],
          "question_key": "HOPE",
          "text": "What is your one biggest hope for young people around you?",
          "kind": "open"
        },
        {
          "headers": [
            "What is your one biggest worry for young people around you?"
          ],
          "question_key": "WORRY",
          "text": "What is your one biggest worry for young people around you?",
          "kind": "open"
        },
        {
          "headers": [
            "What do you think you can do to help young people achieve the future they want?"
          ],
          "question_key": "ACHIEVE",
          "text": "What do you think you can do to help young people achieve the future they want?",
          "kind": "open"
        }
      ],
      "questionnaire_version": "V1"
    }
  },
  "max_csv_upload_bytes": 104857600,
  "ingestion_schema": {
    "create": {
      "required": [
        "submissionId",
        "submissionType",
        "sessionId",
        "tenantCode",
        "eventType",
        "eventPublishedAt",
        "tags.programId",
        "tags.leaderCategoryId",
        "data.source",
        "data.sessionType",
        "data.questionnaireVersion",
        "data.language",
        "data.completionStatus",
        "data.submissionDate",
        "data.messages"
      ],
      "optional": [
        "tags.state",
        "tags.district",
        "tags.programName",
        "tags.leaderCategoryName",
        "tags.source",
        "data.botId",
        "data.botName"
      ]
    },
    "update": {
      "required": [
        "submissionId",
        "submissionType",
        "sessionId",
        "tenantCode",
        "eventType",
        "eventPublishedAt"
      ],
      "newValuesNoEmpty": true
    },
    "delete": {
      "required": [
        "submissionId",
        "submissionType",
        "sessionId",
        "tenantCode",
        "eventType",
        "eventPublishedAt"
      ]
    }
  },
  "process_steps": [
    {
      "name": "feedback_metadata_extraction"
    }
  ]
}
$cfg$::jsonb, 'seed', 'Initial format'
WHERE EXISTS (SELECT 1 FROM config WHERE id = '6543fdb5-8d6e-4929-ae6e-576d7237f38a'::uuid)
  AND NOT EXISTS (SELECT 1 FROM config_version WHERE config_id = '6543fdb5-8d6e-4929-ae6e-576d7237f38a'::uuid)
ON CONFLICT (config_id, version) DO NOTHING;

-- CONFIG feedback_upload mitra Educator_Voice
INSERT INTO config (id, config_source, name, config_type, program_id, tenant_code, leader_id, session_type, bot_id, bot_name)
SELECT '4bc09524-71e3-4f0e-95b9-9d936e6e8697'::uuid, 'mitra', 'Educator bot CSV validator', 'feedback_upload', 'b09ca040-f2cb-4c31-b0a8-07ce0acb69a2'::uuid, 'mitra', NULL, 'Educator_Voice', '110', '1MV Educator Voice'
WHERE EXISTS (SELECT 1 FROM tenant WHERE code = 'mitra')
  AND EXISTS (SELECT 1 FROM programs WHERE id = 'b09ca040-f2cb-4c31-b0a8-07ce0acb69a2'::uuid)
ON CONFLICT DO NOTHING;

INSERT INTO config_version (config_id, version, is_active, config, created_by, change_note)
SELECT '4bc09524-71e3-4f0e-95b9-9d936e6e8697'::uuid, 1, TRUE, $cfg$
{
  "session_type": "Educator_Voice",
  "adapter": "conversation_rows",
  "column_map": {
    "session_type": "session_type",
    "session_status": "session_status",
    "session": "session",
    "submission_id": "submission_id",
    "language": "language",
    "sequence": "sequence",
    "message": "message",
    "created_at": "created_at",
    "stage": "stage",
    "sender_id": "sender_id",
    "bot_id": "bot_id",
    "bot_name": "bot_name",
    "translated_message": "translated_message",
    "conversation_row_id": "conversation_row_id",
    "receiver_id": "receiver_id",
    "updated_at": "updated_at"
  },
  "opening_row": {
    "mode": "auto"
  },
  "questionnaire_versions": [
    {
      "key": "V1",
      "effective_from": null
    }
  ],
  "bot_sender_ids": [
    "1"
  ],
  "message_map": {
    "bot": {
      "message": "translated_message",
      "translated_message": "message"
    },
    "respondent": {
      "message": "message",
      "translated_message": "translated_message"
    }
  },
  "sequence_offset": 1,
  "completion_map": {
    "COMPLETED": "COMPLETED",
    "IN_PROGRESS": "IN_PROGRESS"
  },
  "closing_keys": [
    "THANK_YOU",
    "THANKYOU",
    "CLOSING"
  ],
  "datetime": {
    "fallback_timezone": "Asia/Kolkata"
  },
  "questions": [
    {
      "key": "STATE_IDENTIFICATION",
      "text": "Which state do you stay in?"
    }
  ],
  "max_csv_upload_bytes": 104857600,
  "ingestion_schema": {
    "create": {
      "required": [
        "submissionId",
        "submissionType",
        "sessionId",
        "tenantCode",
        "eventType",
        "eventPublishedAt",
        "tags.programId",
        "tags.leaderCategoryId",
        "data.source",
        "data.sessionType",
        "data.questionnaireVersion",
        "data.language",
        "data.completionStatus",
        "data.submissionDate",
        "data.messages"
      ],
      "optional": [
        "tags.state",
        "tags.district",
        "tags.programName",
        "tags.leaderCategoryName",
        "tags.source",
        "data.botId",
        "data.botName"
      ]
    },
    "update": {
      "required": [
        "submissionId",
        "submissionType",
        "sessionId",
        "tenantCode",
        "eventType",
        "eventPublishedAt"
      ],
      "newValuesNoEmpty": true
    },
    "delete": {
      "required": [
        "submissionId",
        "submissionType",
        "sessionId",
        "tenantCode",
        "eventType",
        "eventPublishedAt"
      ]
    }
  },
  "process_steps": [
    {
      "name": "feedback_metadata_extraction"
    }
  ]
}
$cfg$::jsonb, 'seed', 'Initial format'
WHERE EXISTS (SELECT 1 FROM config WHERE id = '4bc09524-71e3-4f0e-95b9-9d936e6e8697'::uuid)
  AND NOT EXISTS (SELECT 1 FROM config_version WHERE config_id = '4bc09524-71e3-4f0e-95b9-9d936e6e8697'::uuid)
ON CONFLICT (config_id, version) DO NOTHING;

-- =========================================================================
-- 3. Extraction configs: the metadata fields and bands of the LLM extraction (feedback_extraction)
-- =========================================================================

-- CONFIG feedback_extraction any Youth_Voice
INSERT INTO config (id, config_source, name, config_type, program_id, tenant_code, leader_id, session_type, bot_id, bot_name)
SELECT '09e1cd04-4605-54e5-a535-8e21218bb05c'::uuid, 'any', 'Youth metadata extraction', 'feedback_extraction', 'dcd67b14-f0ac-4e71-87fd-fa818c8f2d6a'::uuid, 'mitra', NULL, 'Youth_Voice', NULL, NULL
WHERE EXISTS (SELECT 1 FROM tenant WHERE code = 'mitra')
  AND EXISTS (SELECT 1 FROM programs WHERE id = 'dcd67b14-f0ac-4e71-87fd-fa818c8f2d6a'::uuid)
ON CONFLICT DO NOTHING;

INSERT INTO config_version (config_id, version, is_active, config, created_by, change_note)
SELECT '09e1cd04-4605-54e5-a535-8e21218bb05c'::uuid, 1, TRUE, $cfg$
{
  "schema_version": 1,
  "persona": "Youth",
  "session_type": "Youth_Voice",
  "source_note": "Fields are the DOTS metadata columns of the persona. The category lists are the data team's finalised lists (1MV dashboard requirements sheet) in the engine's normalised spellings; Not answered and N/A are written by the engine and are not listed. Fields with `versions` exist in those questionnaire versions only (N/A otherwise); a field with `applies_when` is N/A unless the earlier field holds one of the listed values; `multi` fields may hold several categories. Sensitive fields are filled only when the respondent states them explicitly.",
  "prompt_name": "Feedback Metadata Extraction - Youth",
  "fields": [
    {
      "name": "gender",
      "kind": "category",
      "description": "The gender of the respondent, as answered to the bot's gender question.",
      "categories": [
        "Male",
        "Female",
        "Others",
        "Prefer not to say",
        "Invalid response",
        "Unsure / don't know"
      ],
      "guidance": {
        "Male": "boy, man, male",
        "Female": "girl, woman, female",
        "Others": "a valid gender that is not male or female, for example non-binary, transgender, 'something else'",
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Invalid response": "a given answer that is gibberish or has nothing to do with the question",
        "Unsure / don't know": "the respondent says they are unsure of their gender identity or do not know which option to choose"
      }
    },
    {
      "name": "age",
      "kind": "integer",
      "description": "The age of the respondent in years, as a number (the Youth bot is for 12 to 25, but older respondents answer it). Use the number the respondent states (decimals such as 15.5 are valid; 'around 16' is 16); do not compute an age from a class, a birth year or a date of birth. A stated range that lies inside one band is a number inside it; a range that spans two or more bands is Unclear.",
      "categories": [
        "Prefer not to say",
        "Unclear",
        "Invalid response"
      ],
      "guidance": {
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Unclear": "a range that spans two or more bands, or a vague answer such as 'thirty something'",
        "Invalid response": "a given answer that is gibberish or has nothing to do with the question"
      },
      "min": 0,
      "max": 120,
      "bands": [
        {
          "label": "< 12",
          "min": null,
          "max": 12
        },
        {
          "label": "≥ 12 and < 16",
          "min": 12,
          "max": 16
        },
        {
          "label": "≥ 16 and < 22",
          "min": 16,
          "max": 22
        },
        {
          "label": "≥ 22 and < 26",
          "min": 22,
          "max": 26
        },
        {
          "label": "≥ 26 and < 30",
          "min": 26,
          "max": 30
        },
        {
          "label": "≥ 30 and < 35",
          "min": 30,
          "max": 35
        },
        {
          "label": "≥ 35 and < 40",
          "min": 35,
          "max": 40
        },
        {
          "label": "≥ 40 and < 45",
          "min": 40,
          "max": 45
        },
        {
          "label": "≥ 45 and < 50",
          "min": 45,
          "max": 50
        },
        {
          "label": "≥ 50 and < 55",
          "min": 50,
          "max": 55
        },
        {
          "label": "≥ 55 and < 60",
          "min": 55,
          "max": 60
        },
        {
          "label": "≥ 60",
          "min": 60,
          "max": null
        }
      ]
    },
    {
      "name": "state",
      "kind": "category",
      "description": "The Indian state or union territory the respondent lives in, as answered to the bot's state question. Map spelling variants, abbreviations and older names to the official name. 'Delhi/NCR' or 'NCR' is decided with the place the respondent names: Noida, Greater Noida and Ghaziabad give Uttar Pradesh, Gurugram (Gurgaon) and Faridabad give Haryana, a Delhi place (or no place at all) gives Delhi. When the respondent names no state (or only a direction, a region or a city) but names a district or a city that belongs to exactly one state, answer with that state and say in the justification that it was derived; a name used in several states, with no state given, is Unmapped. A direction ('North India') or a place that is not one state or union territory is Unmapped. A place outside India makes the state 'Outside India' (and the district too).",
      "categories": [
        "Andhra Pradesh",
        "Arunachal Pradesh",
        "Assam",
        "Bihar",
        "Chhattisgarh",
        "Goa",
        "Gujarat",
        "Haryana",
        "Himachal Pradesh",
        "Jharkhand",
        "Karnataka",
        "Kerala",
        "Madhya Pradesh",
        "Maharashtra",
        "Manipur",
        "Meghalaya",
        "Mizoram",
        "Nagaland",
        "Odisha",
        "Punjab",
        "Rajasthan",
        "Sikkim",
        "Tamil Nadu",
        "Telangana",
        "Tripura",
        "Uttar Pradesh",
        "Uttarakhand",
        "West Bengal",
        "Andaman and Nicobar Islands",
        "Chandigarh",
        "Dadra and Nagar Haveli and Daman and Diu",
        "Delhi",
        "Jammu and Kashmir",
        "Ladakh",
        "Lakshadweep",
        "Puducherry",
        "Prefer not to say",
        "Unmapped",
        "Invalid response",
        "Outside India"
      ],
      "guidance": {
        "Delhi": "Delhi, New Delhi, NCT of Delhi; 'Delhi/NCR' or 'NCR' when the district is a Delhi district or none is given",
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Unmapped": "a place that does not identify one state / district: a direction, a region, a city, or a district name used in several states when no state is known",
        "Invalid response": "names that sound like gibberish, or an answer that has nothing to do with the question",
        "Outside India": "a place that is not in India"
      },
      "aliases": {
        "Delhi": [
          "New Delhi",
          "NCT",
          "NCT of Delhi",
          "National Capital Territory of Delhi",
          "Delhi NCT",
          "Delhi NCR",
          "Delhi/NCR",
          "NCR",
          "Dilli"
        ],
        "Odisha": [
          "Orissa"
        ],
        "Puducherry": [
          "Pondicherry",
          "Pondy"
        ],
        "Uttarakhand": [
          "Uttaranchal"
        ],
        "Telangana": [
          "Telengana"
        ],
        "Tamil Nadu": [
          "Tamilnadu",
          "TN"
        ],
        "Uttar Pradesh": [
          "UP",
          "U.P."
        ],
        "Madhya Pradesh": [
          "MP",
          "M.P."
        ],
        "West Bengal": [
          "WB",
          "Bengal"
        ],
        "Jammu and Kashmir": [
          "J&K",
          "J and K",
          "Jammu & Kashmir",
          "Kashmir"
        ],
        "Andaman and Nicobar Islands": [
          "Andaman",
          "Nicobar",
          "A&N Islands",
          "Andaman & Nicobar Islands"
        ],
        "Dadra and Nagar Haveli and Daman and Diu": [
          "Dadra and Nagar Haveli",
          "Daman and Diu",
          "Daman",
          "Diu",
          "Silvassa",
          "Dadra & Nagar Haveli",
          "Daman & Diu"
        ]
      }
    },
    {
      "name": "district",
      "kind": "free_text",
      "description": "The district the respondent lives in (free text). Return the commonly used official spelling of the district name (for example 'Bengaluru Urban', 'Mysuru'). If the bot asked the respondent to choose between districts, use the one they chose. Do not guess a district the respondent did not name. A city that is not itself a district name is answered with the district it belongs to when you are sure (Noida is in Gautam Buddha Nagar), otherwise Unmapped. A place outside India is 'Outside India', also when the state is. When there is no district to return, answer with one of the special answers.",
      "categories": [
        "Prefer not to say",
        "Unmapped",
        "Invalid response",
        "Outside India"
      ],
      "guidance": {
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Unmapped": "a place that does not identify one state / district: a direction, a region, a city, or a district name used in several states when no state is known",
        "Invalid response": "names that sound like gibberish, or an answer that has nothing to do with the question",
        "Outside India": "a place that is not in India"
      }
    },
    {
      "name": "current_activity_status",
      "kind": "category",
      "description": "What the respondent is doing right now (studying, working, self-employed, looking for work, something else), as answered to the bot's current-activity question. A respondent in two statuses (a student who works part time) gets both.",
      "categories": [
        "Studying",
        "Working",
        "Self-employed",
        "Looking for work/unemployed",
        "Not currently studying",
        "Others",
        "Prefer not to say",
        "Invalid response"
      ],
      "guidance": {
        "Studying": "in school, college or any course, or saying they go to a named school or college",
        "Working": "employed by someone: public or private sector work, a job",
        "Self-employed": "runs their own business or works for themselves (runs a cafe, a shop)",
        "Looking for work/unemployed": "has no work and is looking for work",
        "Not currently studying": "has dropped out, discontinued education, or is not enrolled in an educational institution",
        "Others": "a valid status that is none of these, for example preparing for an exam, or caring for family and not looking for a job",
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Invalid response": "a given answer that is gibberish or has nothing to do with the question"
      },
      "multi": true
    },
    {
      "name": "education_level",
      "kind": "category",
      "description": "The level of education the respondent is in, asked only of those who study. If the respondent says they dropped out, use the last COMPLETED level (someone who dropped out of a BA completed grade 12: Grade 11 to 12). A level below grade 6 is Others.",
      "categories": [
        "Grade 6 to 10",
        "Grade 11 to 12",
        "Skill programs",
        "Pursuing undergraduate and postgraduate",
        "Others",
        "Prefer not to say",
        "Invalid response"
      ],
      "guidance": {
        "Grade 6 to 10": "school classes 6 to 10",
        "Grade 11 to 12": "school classes 11 and 12",
        "Skill programs": "ITI, polytechnic, vocational programmes",
        "Pursuing undergraduate and postgraduate": "a bachelor's or master's course",
        "Others": "a valid course that is not listed, such as a doctoral programme or a certificate course; also a level below grade 6",
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Invalid response": "a given answer that is gibberish or has nothing to do with the question"
      },
      "applies_when": {
        "field": "current_activity_status",
        "in": [
          "Studying"
        ]
      }
    },
    {
      "name": "course_details",
      "kind": "category",
      "description": "The stream or programme the respondent studies, matching education_level: a stream for Grade 11 to 12 (Science or Commerce; any other stream such as Arts or Humanities is Others), a trade for Skill programs, a degree for Pursuing undergraduate and postgraduate. Use only the values that belong to the respondent's education level; a valid course that is not listed is Others.",
      "categories": [
        "Science",
        "Commerce",
        "Electrical or electronics",
        "Mechanical or manufacturing",
        "Automobile or transport",
        "Civil or construction",
        "Computer or IT",
        "Health",
        "Beauty",
        "Hospitality",
        "Arts (BA or MA)",
        "Science (B.Sc or M.Sc)",
        "Commerce (B.Com or M.Com)",
        "Technology (B.Tech or M.Tech)",
        "Business administration (BBA or MBA)",
        "Computer applications (BCA or MCA)",
        "Medicine (MBBS or MD)",
        "Law (LLB or LLM)",
        "Others",
        "Prefer not to say",
        "Invalid response"
      ],
      "guidance": {
        "Science": "the science stream of grades 11 and 12",
        "Commerce": "the commerce stream of grades 11 and 12",
        "Health": "health-related skill programmes (nursing assistant, lab technician, ...)",
        "Arts (BA or MA)": "BA, MA or another arts / humanities degree",
        "Technology (B.Tech or M.Tech)": "B.Tech, M.Tech, BE, ME",
        "Medicine (MBBS or MD)": "MBBS, MD and similar medical degrees",
        "Others": "a valid stream, trade or degree that is not listed (for example Arts or Humanities in grades 11-12, nursing, pharmacy, BDS, B.Ed)",
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Invalid response": "a given answer that is gibberish or has nothing to do with the question"
      },
      "applies_when": {
        "field": "education_level",
        "in": [
          "Grade 11 to 12",
          "Skill programs",
          "Pursuing undergraduate and postgraduate"
        ]
      }
    },
    {
      "name": "institution_type",
      "kind": "category",
      "description": "The type of school or institution the respondent studies in or studied in, as answered to the bot's institution question. An international school or a board, home schooling and open schooling are Others. If the answer is about a college (the question is about schools), it is Invalid response.",
      "categories": [
        "Government",
        "Private",
        "Government-aided",
        "Others",
        "Not sure",
        "Prefer not to say",
        "Invalid response"
      ],
      "guidance": {
        "Government": "government, govt or public (state-run school or institution)",
        "Private": "private school or institution",
        "Others": "any institution beyond the listed ones: home schooling, international, open schooling",
        "Not sure": "the respondent says they are not sure of the institution type or do not know",
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Invalid response": "a given answer that is gibberish or has nothing to do with the question, or an answer about a college (the question is about schools)",
        "Government-aided": "an aided school or institution (privately run, funded by the government)"
      }
    },
    {
      "name": "monthly_household_income",
      "kind": "category",
      "description": "The household income per MONTH, as a band. Asked only in the newer questionnaire. When the respondent speaks of a year (annual, yearly, per year, a lakh a year), divide the amount by 12 first, then pick the band; when it is per month, use it as is.",
      "categories": [
        "Zero income / no income",
        "< ₹25,000",
        "≥ ₹25,000 and < ₹66,000",
        "≥ ₹66,000 and < ₹83,000",
        "≥ ₹83,000",
        "Don't know",
        "Prefer not to say",
        "Invalid response",
        "Unclear"
      ],
      "guidance": {
        "Zero income / no income": "the household has no income",
        "Don't know": "the respondent says they are unsure or do not know the household income",
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Invalid response": "a given answer that is gibberish or has nothing to do with the question",
        "Unclear": "a range that spans two or more bands, or an amount whose period (month or year) cannot be told",
        "< ₹25,000": "a monthly amount above zero and below 25,000",
        "≥ ₹25,000 and < ₹66,000": "a monthly amount from 25,000 up to but not including 66,000; 'around 25,000' belongs here",
        "≥ ₹66,000 and < ₹83,000": "a monthly amount from 66,000 up to but not including 83,000",
        "≥ ₹83,000": "a monthly amount of 83,000 or more"
      },
      "sensitive": true,
      "versions": [
        "V2"
      ]
    },
    {
      "name": "annual_household_income",
      "kind": "category",
      "description": "The household income per YEAR, as a band. Asked only in the older questionnaire. When the respondent speaks of a month, multiply the amount by 12 first, then pick the band; when it is per year, use it as is.",
      "categories": [
        "Zero income / no income",
        "< ₹3 lakh",
        "≥ ₹3 lakh and < ₹8 lakh",
        "≥ ₹8 lakh and < ₹10 lakh",
        "≥ ₹10 lakh",
        "Don't know",
        "Prefer not to say",
        "Invalid response",
        "Unclear"
      ],
      "guidance": {
        "Zero income / no income": "the household has no income",
        "Don't know": "the respondent says they are unsure or do not know the household income",
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Invalid response": "a given answer that is gibberish or has nothing to do with the question",
        "Unclear": "a range that spans two or more bands, or an amount whose period (month or year) cannot be told",
        "< ₹3 lakh": "a yearly amount above zero and below 3 lakh",
        "≥ ₹3 lakh and < ₹8 lakh": "a yearly amount from 3 lakh up to but not including 8 lakh; 'around 4 lakh' belongs here",
        "≥ ₹8 lakh and < ₹10 lakh": "a yearly amount from 8 lakh up to but not including 10 lakh",
        "≥ ₹10 lakh": "a yearly amount of 10 lakh or more"
      },
      "sensitive": true,
      "versions": [
        "V1"
      ]
    },
    {
      "name": "disability",
      "kind": "category",
      "description": "Whether the respondent has a disability, as answered to the bot's disability question (or stated explicitly anywhere else in the conversation). If the respondent names a disability instead of answering yes or no, it is Yes.",
      "categories": [
        "Yes",
        "No",
        "Prefer not to say",
        "Invalid response"
      ],
      "guidance": {
        "Yes": "the respondent says that they have a disability (or, for a parent, that the child has one), including when they state it while answering another question",
        "No": "the respondent says there is none",
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Invalid response": "a given answer that is gibberish or has nothing to do with the question"
      },
      "sensitive": true
    },
    {
      "name": "disability_type",
      "kind": "category",
      "description": "The kind of disability the respondent has, only when disability is Yes. When there are several kinds, list each one.",
      "categories": [
        "Physical disability",
        "Intellectual disability",
        "Others",
        "Prefer not to say",
        "Invalid response"
      ],
      "guidance": {
        "Physical disability": "any locomotor, visual or hearing disability, and speech impairments",
        "Intellectual disability": "any cognitive impairment, specific learning disability, autism spectrum disorder or psychological disability",
        "Others": "a valid disability that fits neither",
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Invalid response": "a given answer that is gibberish or has nothing to do with the question"
      },
      "sensitive": true,
      "multi": true,
      "applies_when": {
        "field": "disability",
        "in": [
          "Yes"
        ]
      }
    }
  ],
  "pii": {
    "masking_prompt_name": "Feedback PII and Abusive-Language Detection",
    "types": [
      {
        "name": "name",
        "description": "a personal name of the respondent or of anyone they mention (family, teacher, friend)"
      },
      {
        "name": "phone",
        "description": "a phone number"
      },
      {
        "name": "email",
        "description": "an email address or a social media handle"
      },
      {
        "name": "address",
        "description": "a home address, street, house number, or a village/locality small enough to identify a person"
      },
      {
        "name": "school_name",
        "description": "the name of a specific school, college, institution or workplace"
      },
      {
        "name": "id_number",
        "description": "an identity or document number (Aadhaar, PAN, roll number, admission number)"
      },
      {
        "name": "other",
        "description": "any other detail that could identify a person"
      }
    ],
    "min_digit_run": 8,
    "min_digit_run_short_groups": 10,
    "url_tlds": [
      "com",
      "in",
      "org",
      "net",
      "co",
      "io",
      "me",
      "edu",
      "gov",
      "app",
      "link",
      "ly",
      "tv",
      "info"
    ],
    "extraction_text": "original"
  },
  "limits": {
    "max_respondent_message_chars": 20000,
    "max_bot_message_chars": 4000,
    "max_conversation_chars": 200000,
    "max_value_chars": 120,
    "max_raw_chars": 500
  },
  "placeholders": [
    "n/a",
    "na",
    "n.a",
    "nil",
    "null",
    "none",
    "nan",
    "undefined",
    "unknown",
    "not known",
    "not provided",
    "not specified",
    "not applicable",
    "not available",
    "not mentioned",
    "not given",
    "not answered",
    "no answer",
    "no district",
    "nothing",
    "not sure",
    "dont know",
    "don't know",
    "do not know",
    "skip",
    "skipped",
    "नहीं पता",
    "पता नहीं",
    "लागू नहीं",
    "idk",
    "i dont know",
    "i don't know",
    "i do not know",
    "no idea"
  ],
  "llm": {
    "max_tokens": 10000
  },
  "copy_to_submission": {
    "state": "state",
    "district": "district"
  }
}
$cfg$::jsonb, 'seed', 'Initial extraction config: the DOTS metadata columns of the persona with the data team''s finalised category lists and the persona''s own prompt'
WHERE EXISTS (SELECT 1 FROM config WHERE id = '09e1cd04-4605-54e5-a535-8e21218bb05c'::uuid)
  AND NOT EXISTS (SELECT 1 FROM config_version WHERE config_id = '09e1cd04-4605-54e5-a535-8e21218bb05c'::uuid)
ON CONFLICT (config_id, version) DO NOTHING;

-- CONFIG feedback_extraction any Parent_Voice
INSERT INTO config (id, config_source, name, config_type, program_id, tenant_code, leader_id, session_type, bot_id, bot_name)
SELECT '973cd290-fbe9-5e02-9eba-861804e5f91d'::uuid, 'any', 'Parent metadata extraction', 'feedback_extraction', '31dae489-5647-4d0b-91cb-1c5f53dba4ae'::uuid, 'mitra', NULL, 'Parent_Voice', NULL, NULL
WHERE EXISTS (SELECT 1 FROM tenant WHERE code = 'mitra')
  AND EXISTS (SELECT 1 FROM programs WHERE id = '31dae489-5647-4d0b-91cb-1c5f53dba4ae'::uuid)
ON CONFLICT DO NOTHING;

INSERT INTO config_version (config_id, version, is_active, config, created_by, change_note)
SELECT '973cd290-fbe9-5e02-9eba-861804e5f91d'::uuid, 1, TRUE, $cfg$
{
  "schema_version": 1,
  "persona": "Parent",
  "session_type": "Parent_Voice",
  "source_note": "Fields are the DOTS metadata columns of the persona. The category lists are the data team's finalised lists (1MV dashboard requirements sheet) in the engine's normalised spellings; Not answered and N/A are written by the engine and are not listed. Fields with `versions` exist in those questionnaire versions only (N/A otherwise); a field with `applies_when` is N/A unless the earlier field holds one of the listed values; `multi` fields may hold several categories. Sensitive fields are filled only when the respondent states them explicitly.",
  "prompt_name": "Feedback Metadata Extraction - Parent",
  "fields": [
    {
      "name": "state",
      "kind": "category",
      "description": "The Indian state or union territory the respondent's child (or, where the bot asked the parent's own state, the respondent) lives in, as answered to the bot's state question. Map spelling variants, abbreviations and older names to the official name. 'Delhi/NCR' or 'NCR' is decided with the place the respondent names: Noida, Greater Noida and Ghaziabad give Uttar Pradesh, Gurugram (Gurgaon) and Faridabad give Haryana, a Delhi place (or no place at all) gives Delhi. When the respondent names no state (or only a direction, a region or a city) but names a district or a city that belongs to exactly one state, answer with that state and say in the justification that it was derived; a name used in several states, with no state given, is Unmapped. A direction ('North India') or a place that is not one state or union territory is Unmapped. A place outside India makes the state 'Outside India' (and the district too).",
      "categories": [
        "Andhra Pradesh",
        "Arunachal Pradesh",
        "Assam",
        "Bihar",
        "Chhattisgarh",
        "Goa",
        "Gujarat",
        "Haryana",
        "Himachal Pradesh",
        "Jharkhand",
        "Karnataka",
        "Kerala",
        "Madhya Pradesh",
        "Maharashtra",
        "Manipur",
        "Meghalaya",
        "Mizoram",
        "Nagaland",
        "Odisha",
        "Punjab",
        "Rajasthan",
        "Sikkim",
        "Tamil Nadu",
        "Telangana",
        "Tripura",
        "Uttar Pradesh",
        "Uttarakhand",
        "West Bengal",
        "Andaman and Nicobar Islands",
        "Chandigarh",
        "Dadra and Nagar Haveli and Daman and Diu",
        "Delhi",
        "Jammu and Kashmir",
        "Ladakh",
        "Lakshadweep",
        "Puducherry",
        "Prefer not to say",
        "Unmapped",
        "Invalid response",
        "Outside India"
      ],
      "guidance": {
        "Delhi": "Delhi, New Delhi, NCT of Delhi; 'Delhi/NCR' or 'NCR' when the district is a Delhi district or none is given",
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Unmapped": "a place that does not identify one state / district: a direction, a region, a city, or a district name used in several states when no state is known",
        "Invalid response": "names that sound like gibberish, or an answer that has nothing to do with the question",
        "Outside India": "a place that is not in India"
      },
      "aliases": {
        "Delhi": [
          "New Delhi",
          "NCT",
          "NCT of Delhi",
          "National Capital Territory of Delhi",
          "Delhi NCT",
          "Delhi NCR",
          "Delhi/NCR",
          "NCR",
          "Dilli"
        ],
        "Odisha": [
          "Orissa"
        ],
        "Puducherry": [
          "Pondicherry",
          "Pondy"
        ],
        "Uttarakhand": [
          "Uttaranchal"
        ],
        "Telangana": [
          "Telengana"
        ],
        "Tamil Nadu": [
          "Tamilnadu",
          "TN"
        ],
        "Uttar Pradesh": [
          "UP",
          "U.P."
        ],
        "Madhya Pradesh": [
          "MP",
          "M.P."
        ],
        "West Bengal": [
          "WB",
          "Bengal"
        ],
        "Jammu and Kashmir": [
          "J&K",
          "J and K",
          "Jammu & Kashmir",
          "Kashmir"
        ],
        "Andaman and Nicobar Islands": [
          "Andaman",
          "Nicobar",
          "A&N Islands",
          "Andaman & Nicobar Islands"
        ],
        "Dadra and Nagar Haveli and Daman and Diu": [
          "Dadra and Nagar Haveli",
          "Daman and Diu",
          "Daman",
          "Diu",
          "Silvassa",
          "Dadra & Nagar Haveli",
          "Daman & Diu"
        ]
      }
    },
    {
      "name": "district",
      "kind": "free_text",
      "description": "The district the respondent's child attends school in (or, where the bot asked the parent's own district, the district the respondent lives in) (free text). Return the commonly used official spelling of the district name (for example 'Bengaluru Urban', 'Mysuru'). If the bot asked the respondent to choose between districts, use the one they chose. Do not guess a district the respondent did not name. A city that is not itself a district name is answered with the district it belongs to when you are sure (Noida is in Gautam Buddha Nagar), otherwise Unmapped. A place outside India is 'Outside India', also when the state is. When there is no district to return, answer with one of the special answers.",
      "categories": [
        "Prefer not to say",
        "Unmapped",
        "Invalid response",
        "Outside India"
      ],
      "guidance": {
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Unmapped": "a place that does not identify one state / district: a direction, a region, a city, or a district name used in several states when no state is known",
        "Invalid response": "names that sound like gibberish, or an answer that has nothing to do with the question",
        "Outside India": "a place that is not in India"
      }
    },
    {
      "name": "child_gender",
      "kind": "category",
      "description": "The gender of the respondent's CHILD, not of the parent (boy maps to Male, girl to Female), as answered to the bot's question about the child. A parent of several children gets every gender that applies, once each.",
      "categories": [
        "Male",
        "Female",
        "Others",
        "Prefer not to say",
        "Invalid response",
        "Unsure / don't know"
      ],
      "guidance": {
        "Male": "boy, man, male",
        "Female": "girl, woman, female",
        "Others": "a valid gender that is not male or female, for example non-binary, transgender, 'something else'",
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Invalid response": "a given answer that is gibberish or has nothing to do with the question",
        "Unsure / don't know": "the respondent says they are unsure of their gender identity or do not know which option to choose"
      },
      "multi": true,
      "versions": [
        "V2"
      ]
    },
    {
      "name": "disability",
      "kind": "category",
      "description": "Whether the respondent's CHILD has a disability, as answered to the bot's disability question (or stated explicitly anywhere else in the conversation). If the respondent names a disability instead of answering yes or no, it is Yes.",
      "categories": [
        "Yes",
        "No",
        "Prefer not to say",
        "Invalid response"
      ],
      "guidance": {
        "Yes": "the respondent says that they have a disability (or, for a parent, that the child has one), including when they state it while answering another question",
        "No": "the respondent says there is none",
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Invalid response": "a given answer that is gibberish or has nothing to do with the question"
      },
      "sensitive": true,
      "versions": [
        "V2"
      ]
    },
    {
      "name": "disability_type",
      "kind": "category",
      "description": "The kind of disability the respondent's CHILD has, only when disability is Yes. When there are several kinds, list each one.",
      "categories": [
        "Physical disability",
        "Intellectual disability",
        "Others",
        "Prefer not to say",
        "Invalid response"
      ],
      "guidance": {
        "Physical disability": "any locomotor, visual or hearing disability, and speech impairments",
        "Intellectual disability": "any cognitive impairment, specific learning disability, autism spectrum disorder or psychological disability",
        "Others": "a valid disability that fits neither",
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Invalid response": "a given answer that is gibberish or has nothing to do with the question"
      },
      "sensitive": true,
      "multi": true,
      "applies_when": {
        "field": "disability",
        "in": [
          "Yes"
        ]
      }
    },
    {
      "name": "respondent_relationship",
      "kind": "category",
      "description": "How the respondent is related to the child, as answered to the bot's relationship question.",
      "categories": [
        "Mother",
        "Father",
        "Guardian",
        "Other relative",
        "Unmapped",
        "Prefer not to say",
        "Invalid response"
      ],
      "guidance": {
        "Mother": "the child's mother",
        "Father": "the child's father",
        "Guardian": "the respondent explicitly says they are the child's guardian",
        "Other relative": "any other family relationship, such as an aunt, uncle or grandparent",
        "Unmapped": "the respondent says 'parent' or 'parents' without saying which one",
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Invalid response": "a given answer that is gibberish or has nothing to do with the question"
      }
    },
    {
      "name": "school_type",
      "kind": "category",
      "description": "The type of school the respondent's child studies in, as answered to the bot's school question. An international school or a board, home schooling and open schooling are Others. If the answer is about a college (the question is about schools), it is Invalid response. An aided school is Others.",
      "categories": [
        "Government",
        "Private",
        "Others",
        "Not sure",
        "Prefer not to say",
        "Invalid response"
      ],
      "guidance": {
        "Government": "government, govt or public (state-run school or institution)",
        "Private": "private school or institution",
        "Others": "any institution beyond the listed ones: home schooling, international, open schooling; also a government-aided school",
        "Not sure": "the respondent says they are not sure of the institution type or do not know",
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Invalid response": "a given answer that is gibberish or has nothing to do with the question, or an answer about a college (the question is about schools)"
      }
    },
    {
      "name": "child_grade",
      "kind": "category",
      "description": "The class the respondent's CHILD studies in, as a band. Only the older questionnaire asks it. A class given as a word ('fifth', '5th') is the number. A parent of several children gets every band that applies, once each. A child who dropped out gets the last COMPLETED level; a child who is not in school and has no further detail is Out of school.",
      "categories": [
        "Pre-primary",
        "Primary (classes 1-5)",
        "Middle (classes 6-8)",
        "High school (classes 9-12)",
        "Undergraduate/postgraduate",
        "Skill programs",
        "Others",
        "Prefer not to say",
        "Invalid response",
        "Unsure / don't know",
        "Out of school / not currently enrolled"
      ],
      "guidance": {
        "Pre-primary": "nursery, LKG, UKG, kindergarten and other pre-primary education",
        "Primary (classes 1-5)": "classes 1 to 5",
        "Middle (classes 6-8)": "classes 6 to 8",
        "High school (classes 9-12)": "classes 9 to 12",
        "Undergraduate/postgraduate": "bachelor's and master's degrees",
        "Skill programs": "ITI, polytechnic, vocational programmes",
        "Others": "a valid education category that is none of these, such as home schooling, alternative education, doctoral programmes",
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Invalid response": "a given answer that is gibberish or has nothing to do with the question",
        "Unsure / don't know": "the respondent says they are unsure of the grade or do not know",
        "Out of school / not currently enrolled": "the respondent says the child dropped out or does not go to school, with no further detail"
      },
      "multi": true,
      "versions": [
        "V1"
      ]
    },
    {
      "name": "parent_education",
      "kind": "category",
      "description": "Whether the RESPONDENT (the parent or guardian, not the child) went to school or college, as answered to the bot's question about their own education.",
      "categories": [
        "Yes",
        "No",
        "Prefer not to say",
        "Invalid response"
      ],
      "guidance": {
        "Yes": "the respondent went to school or college",
        "No": "the respondent says they never went to school",
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Invalid response": "a given answer that is gibberish or has nothing to do with the question"
      }
    },
    {
      "name": "parent_education_level",
      "kind": "category",
      "description": "How far the RESPONDENT (the parent or guardian, not the child) studied, as a band: the highest level COMPLETED (a parent who dropped out of a BA completed class 12: High school). When parent_education is No, this is 'Did not go to school'.",
      "categories": [
        "Pre-primary",
        "Primary (classes 1-5)",
        "Middle (classes 6-8)",
        "High school (classes 9-12)",
        "Undergraduate/postgraduate",
        "Skill programs",
        "Others",
        "Prefer not to say",
        "Invalid response",
        "Unsure / don't know",
        "Did not go to school"
      ],
      "guidance": {
        "Pre-primary": "nursery, LKG, UKG, kindergarten and other pre-primary education",
        "Primary (classes 1-5)": "classes 1 to 5",
        "Middle (classes 6-8)": "classes 6 to 8",
        "High school (classes 9-12)": "classes 9 to 12",
        "Undergraduate/postgraduate": "bachelor's and master's degrees",
        "Skill programs": "ITI, polytechnic, vocational programmes",
        "Others": "a valid education category that is none of these, such as home schooling, alternative education, doctoral programmes",
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Invalid response": "a given answer that is gibberish or has nothing to do with the question",
        "Unsure / don't know": "the respondent says they are unsure of the grade or do not know",
        "Did not go to school": "never went to school"
      }
    }
  ],
  "pii": {
    "masking_prompt_name": "Feedback PII and Abusive-Language Detection",
    "types": [
      {
        "name": "name",
        "description": "a personal name of the respondent or of anyone they mention (family, teacher, friend)"
      },
      {
        "name": "phone",
        "description": "a phone number"
      },
      {
        "name": "email",
        "description": "an email address or a social media handle"
      },
      {
        "name": "address",
        "description": "a home address, street, house number, or a village/locality small enough to identify a person"
      },
      {
        "name": "school_name",
        "description": "the name of a specific school, college, institution or workplace"
      },
      {
        "name": "id_number",
        "description": "an identity or document number (Aadhaar, PAN, roll number, admission number)"
      },
      {
        "name": "other",
        "description": "any other detail that could identify a person"
      }
    ],
    "min_digit_run": 8,
    "min_digit_run_short_groups": 10,
    "url_tlds": [
      "com",
      "in",
      "org",
      "net",
      "co",
      "io",
      "me",
      "edu",
      "gov",
      "app",
      "link",
      "ly",
      "tv",
      "info"
    ],
    "extraction_text": "original"
  },
  "limits": {
    "max_respondent_message_chars": 20000,
    "max_bot_message_chars": 4000,
    "max_conversation_chars": 200000,
    "max_value_chars": 120,
    "max_raw_chars": 500
  },
  "placeholders": [
    "n/a",
    "na",
    "n.a",
    "nil",
    "null",
    "none",
    "nan",
    "undefined",
    "unknown",
    "not known",
    "not provided",
    "not specified",
    "not applicable",
    "not available",
    "not mentioned",
    "not given",
    "not answered",
    "no answer",
    "no district",
    "nothing",
    "not sure",
    "dont know",
    "don't know",
    "do not know",
    "skip",
    "skipped",
    "नहीं पता",
    "पता नहीं",
    "लागू नहीं",
    "idk",
    "i dont know",
    "i don't know",
    "i do not know",
    "no idea"
  ],
  "llm": {
    "max_tokens": 10000
  },
  "copy_to_submission": {
    "state": "state",
    "district": "district"
  }
}
$cfg$::jsonb, 'seed', 'Initial extraction config: the DOTS metadata columns of the persona with the data team''s finalised category lists and the persona''s own prompt'
WHERE EXISTS (SELECT 1 FROM config WHERE id = '973cd290-fbe9-5e02-9eba-861804e5f91d'::uuid)
  AND NOT EXISTS (SELECT 1 FROM config_version WHERE config_id = '973cd290-fbe9-5e02-9eba-861804e5f91d'::uuid)
ON CONFLICT (config_id, version) DO NOTHING;

-- CONFIG feedback_extraction any General_Public_Voice
INSERT INTO config (id, config_source, name, config_type, program_id, tenant_code, leader_id, session_type, bot_id, bot_name)
SELECT '880879eb-a540-53ea-bb7e-074743fa7d87'::uuid, 'any', 'General Public metadata extraction', 'feedback_extraction', 'bc53156c-bcb9-4332-b817-f6c5ef4a0640'::uuid, 'mitra', NULL, 'General_Public_Voice', NULL, NULL
WHERE EXISTS (SELECT 1 FROM tenant WHERE code = 'mitra')
  AND EXISTS (SELECT 1 FROM programs WHERE id = 'bc53156c-bcb9-4332-b817-f6c5ef4a0640'::uuid)
ON CONFLICT DO NOTHING;

INSERT INTO config_version (config_id, version, is_active, config, created_by, change_note)
SELECT '880879eb-a540-53ea-bb7e-074743fa7d87'::uuid, 1, TRUE, $cfg$
{
  "schema_version": 1,
  "persona": "General Public",
  "session_type": "General_Public_Voice",
  "source_note": "Fields are the DOTS metadata columns of the persona. The category lists are the data team's finalised lists (1MV dashboard requirements sheet) in the engine's normalised spellings; Not answered and N/A are written by the engine and are not listed. Fields with `versions` exist in those questionnaire versions only (N/A otherwise); a field with `applies_when` is N/A unless the earlier field holds one of the listed values; `multi` fields may hold several categories. Sensitive fields are filled only when the respondent states them explicitly.",
  "prompt_name": "Feedback Metadata Extraction - General Public",
  "fields": [
    {
      "name": "gender",
      "kind": "category",
      "description": "The gender of the respondent, as answered to the bot's gender question.",
      "categories": [
        "Male",
        "Female",
        "Others",
        "Prefer not to say",
        "Invalid response",
        "Unsure / don't know"
      ],
      "guidance": {
        "Male": "boy, man, male",
        "Female": "girl, woman, female",
        "Others": "a valid gender that is not male or female, for example non-binary, transgender, 'something else'",
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Invalid response": "a given answer that is gibberish or has nothing to do with the question",
        "Unsure / don't know": "the respondent says they are unsure of their gender identity or do not know which option to choose"
      }
    },
    {
      "name": "age",
      "kind": "integer",
      "description": "The age of the respondent in years, as a number. Use the number the respondent states (decimals such as 15.5 are valid; 'around 16' is 16); do not compute an age from a class, a birth year or a date of birth. A stated range that lies inside one band is a number inside it; a range that spans two or more bands is Unclear.",
      "categories": [
        "Prefer not to say",
        "Unclear",
        "Invalid response"
      ],
      "guidance": {
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Unclear": "a range that spans two or more bands, or a vague answer such as 'thirty something'",
        "Invalid response": "a given answer that is gibberish or has nothing to do with the question"
      },
      "min": 0,
      "max": 120,
      "bands": [
        {
          "label": "< 12",
          "min": null,
          "max": 12
        },
        {
          "label": "≥ 12 and < 26",
          "min": 12,
          "max": 26
        },
        {
          "label": "≥ 26 and < 36",
          "min": 26,
          "max": 36
        },
        {
          "label": "≥ 36 and < 46",
          "min": 36,
          "max": 46
        },
        {
          "label": "≥ 46 and < 60",
          "min": 46,
          "max": 60
        },
        {
          "label": "≥ 60",
          "min": 60,
          "max": null
        }
      ]
    },
    {
      "name": "state",
      "kind": "category",
      "description": "The Indian state or union territory the respondent lives in, as answered to the bot's state question. Map spelling variants, abbreviations and older names to the official name. 'Delhi/NCR' or 'NCR' is decided with the place the respondent names: Noida, Greater Noida and Ghaziabad give Uttar Pradesh, Gurugram (Gurgaon) and Faridabad give Haryana, a Delhi place (or no place at all) gives Delhi. When the respondent names no state (or only a direction, a region or a city) but names a district or a city that belongs to exactly one state, answer with that state and say in the justification that it was derived; a name used in several states, with no state given, is Unmapped. A direction ('North India') or a place that is not one state or union territory is Unmapped. A place outside India makes the state 'Outside India' (and the district too).",
      "categories": [
        "Andhra Pradesh",
        "Arunachal Pradesh",
        "Assam",
        "Bihar",
        "Chhattisgarh",
        "Goa",
        "Gujarat",
        "Haryana",
        "Himachal Pradesh",
        "Jharkhand",
        "Karnataka",
        "Kerala",
        "Madhya Pradesh",
        "Maharashtra",
        "Manipur",
        "Meghalaya",
        "Mizoram",
        "Nagaland",
        "Odisha",
        "Punjab",
        "Rajasthan",
        "Sikkim",
        "Tamil Nadu",
        "Telangana",
        "Tripura",
        "Uttar Pradesh",
        "Uttarakhand",
        "West Bengal",
        "Andaman and Nicobar Islands",
        "Chandigarh",
        "Dadra and Nagar Haveli and Daman and Diu",
        "Delhi",
        "Jammu and Kashmir",
        "Ladakh",
        "Lakshadweep",
        "Puducherry",
        "Prefer not to say",
        "Unmapped",
        "Invalid response",
        "Outside India"
      ],
      "guidance": {
        "Delhi": "Delhi, New Delhi, NCT of Delhi; 'Delhi/NCR' or 'NCR' when the district is a Delhi district or none is given",
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Unmapped": "a place that does not identify one state / district: a direction, a region, a city, or a district name used in several states when no state is known",
        "Invalid response": "names that sound like gibberish, or an answer that has nothing to do with the question",
        "Outside India": "a place that is not in India"
      },
      "aliases": {
        "Delhi": [
          "New Delhi",
          "NCT",
          "NCT of Delhi",
          "National Capital Territory of Delhi",
          "Delhi NCT",
          "Delhi NCR",
          "Delhi/NCR",
          "NCR",
          "Dilli"
        ],
        "Odisha": [
          "Orissa"
        ],
        "Puducherry": [
          "Pondicherry",
          "Pondy"
        ],
        "Uttarakhand": [
          "Uttaranchal"
        ],
        "Telangana": [
          "Telengana"
        ],
        "Tamil Nadu": [
          "Tamilnadu",
          "TN"
        ],
        "Uttar Pradesh": [
          "UP",
          "U.P."
        ],
        "Madhya Pradesh": [
          "MP",
          "M.P."
        ],
        "West Bengal": [
          "WB",
          "Bengal"
        ],
        "Jammu and Kashmir": [
          "J&K",
          "J and K",
          "Jammu & Kashmir",
          "Kashmir"
        ],
        "Andaman and Nicobar Islands": [
          "Andaman",
          "Nicobar",
          "A&N Islands",
          "Andaman & Nicobar Islands"
        ],
        "Dadra and Nagar Haveli and Daman and Diu": [
          "Dadra and Nagar Haveli",
          "Daman and Diu",
          "Daman",
          "Diu",
          "Silvassa",
          "Dadra & Nagar Haveli",
          "Daman & Diu"
        ]
      }
    },
    {
      "name": "district",
      "kind": "free_text",
      "description": "The district the respondent lives in (free text). Return the commonly used official spelling of the district name (for example 'Bengaluru Urban', 'Mysuru'). If the bot asked the respondent to choose between districts, use the one they chose. Do not guess a district the respondent did not name. A city that is not itself a district name is answered with the district it belongs to when you are sure (Noida is in Gautam Buddha Nagar), otherwise Unmapped. A place outside India is 'Outside India', also when the state is. When there is no district to return, answer with one of the special answers.",
      "categories": [
        "Prefer not to say",
        "Unmapped",
        "Invalid response",
        "Outside India"
      ],
      "guidance": {
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Unmapped": "a place that does not identify one state / district: a direction, a region, a city, or a district name used in several states when no state is known",
        "Invalid response": "names that sound like gibberish, or an answer that has nothing to do with the question",
        "Outside India": "a place that is not in India"
      }
    },
    {
      "name": "occupation",
      "kind": "category",
      "description": "The sector of the respondent's work, as answered to the bot's occupation question. An answer that is a job title (for example teacher, nurse, farmer) maps to the sector of that job. When the work could belong to two sectors, the sector of the work wins over the employer.",
      "categories": [
        "Education",
        "Health",
        "Government and public services",
        "IT and services",
        "Manufacturing",
        "Agriculture",
        "Retail and trade",
        "NGO or social sector",
        "Others",
        "Not currently working",
        "Homemaker",
        "Student",
        "Prefer not to say",
        "Unclear",
        "Invalid response"
      ],
      "guidance": {
        "Education": "teacher, tutor, anganwadi teacher, lecturer, school or college staff",
        "Health": "doctor, nurse, dentist, healthcare assistant, ASHA worker, pharmacist, hospital staff",
        "Government and public services": "government employee, civil servant, IAS, IPS, government officer, police, traffic police, army, navy, air force",
        "IT and services": "developer, designer, IT support, software, BPO",
        "Manufacturing": "factory worker, assembly line worker, textile or garment factory worker, automobile worker",
        "Agriculture": "farmer, anything to do with farming, dairy, fishing",
        "Retail and trade": "shopkeeper, retail salesperson, shop assistant, store manager, vendor, business owner",
        "NGO or social sector": "NGO employee, social worker, community mobiliser, field coordinator, programme manager at an NGO, CSR professional",
        "Others": "a valid occupation that fits none of the sectors, for example actor, musician, content creator, driver, electrician, tailor, construction or daily-wage worker, domestic worker, retired",
        "Not currently working": "between jobs, looking for work, unemployed",
        "Homemaker": "managing the household or caregiving as the main activity",
        "Student": "in school, studying, in college",
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Unclear": "some information but not enough to tell the occupation or sector ('I work somewhere', 'I work in a private company')",
        "Invalid response": "gibberish, nonsensical or irrelevant"
      }
    }
  ],
  "pii": {
    "masking_prompt_name": "Feedback PII and Abusive-Language Detection",
    "types": [
      {
        "name": "name",
        "description": "a personal name of the respondent or of anyone they mention (family, teacher, friend)"
      },
      {
        "name": "phone",
        "description": "a phone number"
      },
      {
        "name": "email",
        "description": "an email address or a social media handle"
      },
      {
        "name": "address",
        "description": "a home address, street, house number, or a village/locality small enough to identify a person"
      },
      {
        "name": "school_name",
        "description": "the name of a specific school, college, institution or workplace"
      },
      {
        "name": "id_number",
        "description": "an identity or document number (Aadhaar, PAN, roll number, admission number)"
      },
      {
        "name": "other",
        "description": "any other detail that could identify a person"
      }
    ],
    "min_digit_run": 8,
    "min_digit_run_short_groups": 10,
    "url_tlds": [
      "com",
      "in",
      "org",
      "net",
      "co",
      "io",
      "me",
      "edu",
      "gov",
      "app",
      "link",
      "ly",
      "tv",
      "info"
    ],
    "extraction_text": "original"
  },
  "limits": {
    "max_respondent_message_chars": 20000,
    "max_bot_message_chars": 4000,
    "max_conversation_chars": 200000,
    "max_value_chars": 120,
    "max_raw_chars": 500
  },
  "placeholders": [
    "n/a",
    "na",
    "n.a",
    "nil",
    "null",
    "none",
    "nan",
    "undefined",
    "unknown",
    "not known",
    "not provided",
    "not specified",
    "not applicable",
    "not available",
    "not mentioned",
    "not given",
    "not answered",
    "no answer",
    "no district",
    "nothing",
    "not sure",
    "dont know",
    "don't know",
    "do not know",
    "skip",
    "skipped",
    "नहीं पता",
    "पता नहीं",
    "लागू नहीं",
    "idk",
    "i dont know",
    "i don't know",
    "i do not know",
    "no idea"
  ],
  "llm": {
    "max_tokens": 10000
  },
  "copy_to_submission": {
    "state": "state",
    "district": "district"
  }
}
$cfg$::jsonb, 'seed', 'Initial extraction config: the DOTS metadata columns of the persona with the data team''s finalised category lists and the persona''s own prompt'
WHERE EXISTS (SELECT 1 FROM config WHERE id = '880879eb-a540-53ea-bb7e-074743fa7d87'::uuid)
  AND NOT EXISTS (SELECT 1 FROM config_version WHERE config_id = '880879eb-a540-53ea-bb7e-074743fa7d87'::uuid)
ON CONFLICT (config_id, version) DO NOTHING;

-- CONFIG feedback_extraction any Educator_Voice
INSERT INTO config (id, config_source, name, config_type, program_id, tenant_code, leader_id, session_type, bot_id, bot_name)
SELECT '1e8f69f1-2626-58dc-a0e8-786edcfc58fe'::uuid, 'any', 'Educator metadata extraction', 'feedback_extraction', 'b09ca040-f2cb-4c31-b0a8-07ce0acb69a2'::uuid, 'mitra', NULL, 'Educator_Voice', NULL, NULL
WHERE EXISTS (SELECT 1 FROM tenant WHERE code = 'mitra')
  AND EXISTS (SELECT 1 FROM programs WHERE id = 'b09ca040-f2cb-4c31-b0a8-07ce0acb69a2'::uuid)
ON CONFLICT DO NOTHING;

INSERT INTO config_version (config_id, version, is_active, config, created_by, change_note)
SELECT '1e8f69f1-2626-58dc-a0e8-786edcfc58fe'::uuid, 1, TRUE, $cfg$
{
  "schema_version": 1,
  "persona": "Educator",
  "session_type": "Educator_Voice",
  "source_note": "Fields are the DOTS metadata columns of the persona. The category lists are the data team's finalised lists (1MV dashboard requirements sheet) in the engine's normalised spellings; Not answered and N/A are written by the engine and are not listed. Fields with `versions` exist in those questionnaire versions only (N/A otherwise); a field with `applies_when` is N/A unless the earlier field holds one of the listed values; `multi` fields may hold several categories. Sensitive fields are filled only when the respondent states them explicitly.",
  "prompt_name": "Feedback Metadata Extraction - Educator",
  "fields": [
    {
      "name": "state",
      "kind": "category",
      "description": "The Indian state or union territory the respondent lives in, as answered to the bot's state question. Map spelling variants, abbreviations and older names to the official name. 'Delhi/NCR' or 'NCR' is decided with the place the respondent names: Noida, Greater Noida and Ghaziabad give Uttar Pradesh, Gurugram (Gurgaon) and Faridabad give Haryana, a Delhi place (or no place at all) gives Delhi. When the respondent names no state (or only a direction, a region or a city) but names a district or a city that belongs to exactly one state, answer with that state and say in the justification that it was derived; a name used in several states, with no state given, is Unmapped. A direction ('North India') or a place that is not one state or union territory is Unmapped. A place outside India makes the state 'Outside India' (and the district too).",
      "categories": [
        "Andhra Pradesh",
        "Arunachal Pradesh",
        "Assam",
        "Bihar",
        "Chhattisgarh",
        "Goa",
        "Gujarat",
        "Haryana",
        "Himachal Pradesh",
        "Jharkhand",
        "Karnataka",
        "Kerala",
        "Madhya Pradesh",
        "Maharashtra",
        "Manipur",
        "Meghalaya",
        "Mizoram",
        "Nagaland",
        "Odisha",
        "Punjab",
        "Rajasthan",
        "Sikkim",
        "Tamil Nadu",
        "Telangana",
        "Tripura",
        "Uttar Pradesh",
        "Uttarakhand",
        "West Bengal",
        "Andaman and Nicobar Islands",
        "Chandigarh",
        "Dadra and Nagar Haveli and Daman and Diu",
        "Delhi",
        "Jammu and Kashmir",
        "Ladakh",
        "Lakshadweep",
        "Puducherry",
        "Prefer not to say",
        "Unmapped",
        "Invalid response",
        "Outside India"
      ],
      "guidance": {
        "Delhi": "Delhi, New Delhi, NCT of Delhi; 'Delhi/NCR' or 'NCR' when the district is a Delhi district or none is given",
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Unmapped": "a place that does not identify one state / district: a direction, a region, a city, or a district name used in several states when no state is known",
        "Invalid response": "names that sound like gibberish, or an answer that has nothing to do with the question",
        "Outside India": "a place that is not in India"
      },
      "aliases": {
        "Delhi": [
          "New Delhi",
          "NCT",
          "NCT of Delhi",
          "National Capital Territory of Delhi",
          "Delhi NCT",
          "Delhi NCR",
          "Delhi/NCR",
          "NCR",
          "Dilli"
        ],
        "Odisha": [
          "Orissa"
        ],
        "Puducherry": [
          "Pondicherry",
          "Pondy"
        ],
        "Uttarakhand": [
          "Uttaranchal"
        ],
        "Telangana": [
          "Telengana"
        ],
        "Tamil Nadu": [
          "Tamilnadu",
          "TN"
        ],
        "Uttar Pradesh": [
          "UP",
          "U.P."
        ],
        "Madhya Pradesh": [
          "MP",
          "M.P."
        ],
        "West Bengal": [
          "WB",
          "Bengal"
        ],
        "Jammu and Kashmir": [
          "J&K",
          "J and K",
          "Jammu & Kashmir",
          "Kashmir"
        ],
        "Andaman and Nicobar Islands": [
          "Andaman",
          "Nicobar",
          "A&N Islands",
          "Andaman & Nicobar Islands"
        ],
        "Dadra and Nagar Haveli and Daman and Diu": [
          "Dadra and Nagar Haveli",
          "Daman and Diu",
          "Daman",
          "Diu",
          "Silvassa",
          "Dadra & Nagar Haveli",
          "Daman & Diu"
        ]
      }
    },
    {
      "name": "district",
      "kind": "free_text",
      "description": "The district the respondent teaches in (free text). Return the commonly used official spelling of the district name (for example 'Bengaluru Urban', 'Mysuru'). If the bot asked the respondent to choose between districts, use the one they chose. Do not guess a district the respondent did not name. A city that is not itself a district name is answered with the district it belongs to when you are sure (Noida is in Gautam Buddha Nagar), otherwise Unmapped. A place outside India is 'Outside India', also when the state is. When there is no district to return, answer with one of the special answers.",
      "categories": [
        "Prefer not to say",
        "Unmapped",
        "Invalid response",
        "Outside India"
      ],
      "guidance": {
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Unmapped": "a place that does not identify one state / district: a direction, a region, a city, or a district name used in several states when no state is known",
        "Invalid response": "names that sound like gibberish, or an answer that has nothing to do with the question",
        "Outside India": "a place that is not in India"
      }
    },
    {
      "name": "gender",
      "kind": "category",
      "description": "The gender of the respondent, as answered to the bot's gender question.",
      "categories": [
        "Male",
        "Female",
        "Others",
        "Prefer not to say",
        "Invalid response",
        "Unsure / don't know"
      ],
      "guidance": {
        "Male": "boy, man, male",
        "Female": "girl, woman, female",
        "Others": "a valid gender that is not male or female, for example non-binary, transgender, 'something else'",
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Invalid response": "a given answer that is gibberish or has nothing to do with the question",
        "Unsure / don't know": "the respondent says they are unsure of their gender identity or do not know which option to choose"
      }
    },
    {
      "name": "institution_type",
      "kind": "category",
      "description": "The type of institution the respondent teaches at, as answered to the bot's institution question. An international school or a board, home schooling and open schooling are Others. If the answer is about a college (the question is about schools), it is Invalid response. An aided school is Others.",
      "categories": [
        "Government",
        "Private",
        "Others",
        "Not sure",
        "Prefer not to say",
        "Invalid response"
      ],
      "guidance": {
        "Government": "government, govt or public (state-run school or institution)",
        "Private": "private school or institution",
        "Others": "any institution beyond the listed ones: home schooling, international, open schooling; also a government-aided school",
        "Not sure": "the respondent says they are not sure of the institution type or do not know",
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Invalid response": "a given answer that is gibberish or has nothing to do with the question, or an answer about a college (the question is about schools)"
      }
    },
    {
      "name": "teaching_level",
      "kind": "category",
      "description": "The level the respondent teaches, as a band. The bot asks 'primary, middle, high school or college': 'primary' is Primary, 'middle' is Middle, 'high school' is High school; 'college' alone is Undergraduate, and Postgraduate only when the respondent names a master's level. A respondent who teaches several levels gets every band that applies, once each.",
      "categories": [
        "Pre-primary",
        "Primary (classes 1-5)",
        "Middle (classes 6-8)",
        "High school (classes 9-12)",
        "Undergraduate",
        "Postgraduate",
        "Others",
        "Prefer not to say",
        "Invalid response"
      ],
      "guidance": {
        "Pre-primary": "nursery, LKG, UKG, kindergarten and other pre-primary education",
        "Primary (classes 1-5)": "classes 1 to 5",
        "Middle (classes 6-8)": "classes 6 to 8",
        "High school (classes 9-12)": "classes 9 to 12",
        "Undergraduate": "teaches bachelor's degree students (for example BCom, BCA, BE)",
        "Postgraduate": "teaches master's degree students (for example MBA, MCom)",
        "Others": "a valid level that is none of these: vocational or skill training (ITI, polytechnic), tuitions, home schooling, alternative education, doctoral programmes",
        "Prefer not to say": "the respondent explicitly says they do not want to share it (a skipped question or no answer is null, not this)",
        "Invalid response": "a given answer that is gibberish or has nothing to do with the question"
      },
      "multi": true
    }
  ],
  "pii": {
    "masking_prompt_name": "Feedback PII and Abusive-Language Detection",
    "types": [
      {
        "name": "name",
        "description": "a personal name of the respondent or of anyone they mention (family, teacher, friend)"
      },
      {
        "name": "phone",
        "description": "a phone number"
      },
      {
        "name": "email",
        "description": "an email address or a social media handle"
      },
      {
        "name": "address",
        "description": "a home address, street, house number, or a village/locality small enough to identify a person"
      },
      {
        "name": "school_name",
        "description": "the name of a specific school, college, institution or workplace"
      },
      {
        "name": "id_number",
        "description": "an identity or document number (Aadhaar, PAN, roll number, admission number)"
      },
      {
        "name": "other",
        "description": "any other detail that could identify a person"
      }
    ],
    "min_digit_run": 8,
    "min_digit_run_short_groups": 10,
    "url_tlds": [
      "com",
      "in",
      "org",
      "net",
      "co",
      "io",
      "me",
      "edu",
      "gov",
      "app",
      "link",
      "ly",
      "tv",
      "info"
    ],
    "extraction_text": "original"
  },
  "limits": {
    "max_respondent_message_chars": 20000,
    "max_bot_message_chars": 4000,
    "max_conversation_chars": 200000,
    "max_value_chars": 120,
    "max_raw_chars": 500
  },
  "placeholders": [
    "n/a",
    "na",
    "n.a",
    "nil",
    "null",
    "none",
    "nan",
    "undefined",
    "unknown",
    "not known",
    "not provided",
    "not specified",
    "not applicable",
    "not available",
    "not mentioned",
    "not given",
    "not answered",
    "no answer",
    "no district",
    "nothing",
    "not sure",
    "dont know",
    "don't know",
    "do not know",
    "skip",
    "skipped",
    "नहीं पता",
    "पता नहीं",
    "लागू नहीं",
    "idk",
    "i dont know",
    "i don't know",
    "i do not know",
    "no idea"
  ],
  "llm": {
    "max_tokens": 10000
  },
  "copy_to_submission": {
    "state": "state",
    "district": "district"
  }
}
$cfg$::jsonb, 'seed', 'Initial extraction config: the DOTS metadata columns of the persona with the data team''s finalised category lists and the persona''s own prompt'
WHERE EXISTS (SELECT 1 FROM config WHERE id = '1e8f69f1-2626-58dc-a0e8-786edcfc58fe'::uuid)
  AND NOT EXISTS (SELECT 1 FROM config_version WHERE config_id = '1e8f69f1-2626-58dc-a0e8-786edcfc58fe'::uuid)
ON CONFLICT (config_id, version) DO NOTHING;
