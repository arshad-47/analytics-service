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
--   * The whole seed is a NO-OP when tenant 'mitra' does not exist (the tenant is an environment row;
--     this file never creates it).
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
  "source_note": "Fields are the DOTS metadata columns of the persona. Category sets come from the Katha stories (still Draft); fields marked provisional wait for product confirmation, sensitive fields are filled only when the respondent states them explicitly. Districts are free text without a reference list.",
  "prompt_name": "Feedback Metadata Extraction - Youth",
  "fields": [
    {
      "name": "age",
      "kind": "integer",
      "description": "The age in whole years of the respondent (the Youth bot is for 12 to 25, but older respondents answer it), as a number. Use the number the respondent states; do not compute an age from a class, a birth year or a date of birth, and do not round a stated age. An age given as a range, or a number that cannot be an age, is not a value (put the words in raw).",
      "min": 5,
      "max": 100,
      "provisional": true
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
        "Invalid"
      ],
      "guidance": {
        "Male": "boy, man, male",
        "Female": "girl, woman, female",
        "Others": "a valid gender that is not male or female, for example non-binary, transgender, 'something else'",
        "Prefer not to say": "the respondent declines or skips the question ('skip', 'do not want to say', 'not sure')",
        "Invalid": "a given answer that is gibberish or has nothing to do with the question"
      },
      "provisional": true
    },
    {
      "name": "state",
      "kind": "category",
      "description": "The Indian state or union territory the respondent lives in, as answered to the bot's state question. Map spelling variants and abbreviations to the official name; never infer a state from a district, a language or a city. A place that is not an Indian state or union territory does not fit any allowed value.",
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
        "Delhi (NCT)",
        "Jammu and Kashmir",
        "Ladakh",
        "Lakshadweep",
        "Puducherry"
      ],
      "guidance": {
        "Andaman and Nicobar Islands": "also Andaman, Nicobar, A&N Islands",
        "Dadra and Nagar Haveli and Daman and Diu": "also Dadra and Nagar Haveli, Daman, Diu, Silvassa",
        "Delhi (NCT)": "also Delhi, New Delhi, NCT of Delhi",
        "Odisha": "also Orissa",
        "Puducherry": "also Pondicherry",
        "Uttarakhand": "also Uttaranchal",
        "Telangana": "also Telengana",
        "Jammu and Kashmir": "also J&K, Kashmir"
      }
    },
    {
      "name": "district",
      "kind": "free_text",
      "description": "The district the respondent lives in (free text). Return the commonly used official spelling of the district name (for example 'Bengaluru Urban', 'Mysuru'). If the bot asked the respondent to choose between districts, use the one they chose. Do not guess a district the respondent did not name."
    },
    {
      "name": "current_activity_status",
      "kind": "category",
      "description": "What the respondent is doing right now (studying, working, self-employed, looking for work, something else), as answered to the bot's current-activity question.",
      "categories": [
        "Studying",
        "Working",
        "Self-employed",
        "Looking for work/Unemployed",
        "Other",
        "Invalid"
      ],
      "guidance": {
        "Studying": "in school, college or any course; also preparing for an exam as a student",
        "Working": "has a job or works for someone, including a family business or farm",
        "Self-employed": "runs their own business or work",
        "Looking for work/Unemployed": "not working and looking for work, or unemployed",
        "Other": "anything else the respondent names (for example caring for family, 'doing something else' with no detail)",
        "Invalid": "a given answer that is gibberish or has nothing to do with the question"
      },
      "provisional": true
    },
    {
      "name": "education_level",
      "kind": "free_text",
      "description": "The class the respondent studies in now, as asked after 'studying' (free text: for a school class just the number, for example '9'; for college the year or level in a few words). Empty when the respondent does not study or never named a class. No bands are defined yet (H7).",
      "provisional": true
    },
    {
      "name": "course_details",
      "kind": "free_text",
      "description": "The course or stream the respondent studies (free text, in a few words, for example the stream of a school student or the degree and subject of a college student). Empty when not asked or not named. No bands are defined yet (H7).",
      "provisional": true
    },
    {
      "name": "institution_type",
      "kind": "category",
      "description": "The type of educational institution the respondent studies in or studied in (government, government-aided or private), as answered to the bot's institution question. Answered by every respondent in the current bot, including those who work (about their past school).",
      "categories": [
        "Government",
        "Government-aided",
        "Private",
        "Others",
        "Invalid"
      ],
      "guidance": {
        "Government": "government or state-run school, college or institution",
        "Government-aided": "aided school or institution (privately run, funded by the government)",
        "Private": "private school, college or institution",
        "Others": "any other kind of institution the respondent names (for example madrasa, open school, home schooling)",
        "Invalid": "a given answer that is gibberish or has nothing to do with the question"
      },
      "provisional": true
    },
    {
      "name": "monthly_household_income",
      "kind": "category",
      "description": "The household income per MONTH, as a band. The bot's question was per YEAR in the older questionnaire and per MONTH in the newer: when the question or the respondent speaks of a year (annual, yearly, per year, a lakh a year), divide the amount by 12 first, then pick the band; when it is per month, use it as is. A range that spans two bands, or an amount whose period is unclear, fits no band (value null, put the words in raw).",
      "categories": [
        "Less than ₹25,000",
        "₹25,000 - ₹66,000",
        "₹67,000 - ₹83,000",
        "Above ₹83,000",
        "Don't know"
      ],
      "guidance": {
        "Less than ₹25,000": "a monthly amount below 25,000 (including 0)",
        "₹25,000 - ₹66,000": "a monthly amount from 25,000 up to but not including 67,000 (an amount between 66,000 and 67,000 belongs here)",
        "₹67,000 - ₹83,000": "a monthly amount from 67,000 up to and including 83,000",
        "Above ₹83,000": "a monthly amount above 83,000",
        "Don't know": "the respondent says they do not know the household income"
      },
      "provisional": true,
      "sensitive": true
    },
    {
      "name": "disability",
      "kind": "category",
      "description": "Whether the respondent has a disability, as answered to the bot's disability question (or stated explicitly anywhere else in the conversation).",
      "categories": [
        "Yes",
        "No",
        "Prefer not to say",
        "Invalid"
      ],
      "guidance": {
        "Yes": "the respondent says that they have a disability (or, for a parent, that the child has one), including when they state it while answering another question",
        "No": "the respondent says there is none",
        "Prefer not to say": "the respondent declines to answer ('skip', 'do not want to say')",
        "Invalid": "a given answer that is gibberish or has nothing to do with the question"
      },
      "provisional": true,
      "sensitive": true
    },
    {
      "name": "disability_type",
      "kind": "free_text",
      "description": "The kind of disability the respondent has (free text, for example 'visual impairment', 'hearing impairment', 'locomotor disability'), only when disability is Yes and the respondent names the kind. No list of types is defined yet.",
      "provisional": true,
      "sensitive": true
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
    ]
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
    "लागू नहीं"
  ],
  "llm": {}
}
$cfg$::jsonb, 'seed', 'Initial extraction config: the DOTS metadata columns of the persona with bands from the Katha stories (fields marked provisional await product) and the persona''s own prompt'
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
  "source_note": "Fields are the DOTS metadata columns of the persona. Category sets come from the Katha stories (still Draft); fields marked provisional wait for product confirmation, sensitive fields are filled only when the respondent states them explicitly. Districts are free text without a reference list.",
  "prompt_name": "Feedback Metadata Extraction - Parent",
  "fields": [
    {
      "name": "state",
      "kind": "category",
      "description": "The Indian state or union territory the respondent's child (or, where the bot asked the parent's own state, the respondent) lives in, as answered to the bot's state question. Map spelling variants and abbreviations to the official name; never infer a state from a district, a language or a city. A place that is not an Indian state or union territory does not fit any allowed value.",
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
        "Delhi (NCT)",
        "Jammu and Kashmir",
        "Ladakh",
        "Lakshadweep",
        "Puducherry"
      ],
      "guidance": {
        "Andaman and Nicobar Islands": "also Andaman, Nicobar, A&N Islands",
        "Dadra and Nagar Haveli and Daman and Diu": "also Dadra and Nagar Haveli, Daman, Diu, Silvassa",
        "Delhi (NCT)": "also Delhi, New Delhi, NCT of Delhi",
        "Odisha": "also Orissa",
        "Puducherry": "also Pondicherry",
        "Uttarakhand": "also Uttaranchal",
        "Telangana": "also Telengana",
        "Jammu and Kashmir": "also J&K, Kashmir"
      }
    },
    {
      "name": "district",
      "kind": "free_text",
      "description": "The district the respondent's child attends school in (or, where the bot asked the parent's own district, the district the respondent lives in) (free text). Return the commonly used official spelling of the district name (for example 'Bengaluru Urban', 'Mysuru'). If the bot asked the respondent to choose between districts, use the one they chose. Do not guess a district the respondent did not name."
    },
    {
      "name": "child_gender",
      "kind": "category",
      "description": "The gender of the respondent's CHILD, not of the parent (boy maps to Male, girl to Female), as answered to the bot's question about the child.",
      "categories": [
        "Male",
        "Female",
        "Others",
        "Prefer not to say",
        "Invalid"
      ],
      "guidance": {
        "Male": "boy, man, male",
        "Female": "girl, woman, female",
        "Others": "a valid gender that is not male or female, for example non-binary, transgender, 'something else'",
        "Prefer not to say": "the respondent declines or skips the question ('skip', 'do not want to say', 'not sure')",
        "Invalid": "a given answer that is gibberish or has nothing to do with the question"
      },
      "provisional": true
    },
    {
      "name": "disability",
      "kind": "category",
      "description": "Whether the respondent's CHILD has a disability, as answered to the bot's disability question (or stated explicitly anywhere else in the conversation).",
      "categories": [
        "Yes",
        "No",
        "Prefer not to say",
        "Invalid"
      ],
      "guidance": {
        "Yes": "the respondent says that they have a disability (or, for a parent, that the child has one), including when they state it while answering another question",
        "No": "the respondent says there is none",
        "Prefer not to say": "the respondent declines to answer ('skip', 'do not want to say')",
        "Invalid": "a given answer that is gibberish or has nothing to do with the question"
      },
      "provisional": true,
      "sensitive": true
    },
    {
      "name": "disability_type",
      "kind": "free_text",
      "description": "The kind of disability the respondent's CHILD has (free text, for example 'visual impairment', 'hearing impairment', 'locomotor disability'), only when disability is Yes and the respondent names the kind. No list of types is defined yet.",
      "provisional": true,
      "sensitive": true
    },
    {
      "name": "respondent_relationship",
      "kind": "category",
      "description": "How the respondent is related to the child, as answered to the bot's relationship question.",
      "categories": [
        "Mother",
        "Father",
        "Guardian"
      ],
      "guidance": {
        "Mother": "the child's mother",
        "Father": "the child's father",
        "Guardian": "any other caregiver (grandparent, aunt, uncle, older sibling, legal guardian)"
      },
      "provisional": true
    },
    {
      "name": "school_type",
      "kind": "category",
      "description": "The type of school the respondent's child studies in (government or private), as answered to the bot's school question.",
      "categories": [
        "Government",
        "Private",
        "Others",
        "Invalid"
      ],
      "guidance": {
        "Government": "government or state-run school, college or institution",
        "Private": "private school, college or institution",
        "Others": "any other kind of institution the respondent names (for example madrasa, open school, home schooling)",
        "Invalid": "a given answer that is gibberish or has nothing to do with the question"
      },
      "provisional": true
    },
    {
      "name": "child_grade",
      "kind": "category",
      "description": "The class the respondent's CHILD studies in, as a band. Only the older questionnaire asks it: empty when never asked. A class given as a word ('fifth', '5th') is the number. Several children in different bands, or a class that fits no band (for example nursery), fit no band (value null, put the words in raw).",
      "categories": [
        "Primary (Classes 1-5)",
        "Middle (Classes 6-8)",
        "Secondary (Classes 9-10)",
        "Higher Secondary (Classes 11-12)",
        "College/University",
        "Other"
      ],
      "guidance": {
        "Primary (Classes 1-5)": "classes 1 to 5",
        "Middle (Classes 6-8)": "classes 6 to 8",
        "Secondary (Classes 9-10)": "classes 9 and 10",
        "Higher Secondary (Classes 11-12)": "classes 11 and 12",
        "College/University": "any degree or college course",
        "Other": "out of school, not studying, home schooling"
      },
      "provisional": true
    },
    {
      "name": "parent_education",
      "kind": "category",
      "description": "Whether the RESPONDENT (the parent or guardian, not the child) went to school or college, as answered to the bot's question about their own education.",
      "categories": [
        "Yes",
        "No"
      ],
      "guidance": {
        "Yes": "the respondent went to school or college",
        "No": "the respondent never went to school"
      },
      "provisional": true
    },
    {
      "name": "parent_education_level",
      "kind": "category",
      "description": "How far the RESPONDENT (the parent or guardian, not the child) studied, as a band: the highest level reached. When parent_education is No, this is 'Did Not Go to School'. A parent who says yes without a level has no level (value null, put the words in raw).",
      "categories": [
        "Did Not Go to School",
        "Primary",
        "Middle",
        "Secondary",
        "Higher Secondary",
        "Undergraduate",
        "Postgraduate",
        "Others",
        "Invalid"
      ],
      "guidance": {
        "Did Not Go to School": "never went to school",
        "Primary": "studied up to class 5",
        "Middle": "studied up to class 8",
        "Secondary": "studied up to class 10",
        "Higher Secondary": "studied up to class 12",
        "Undergraduate": "any bachelor's degree (for example BA, BCom, BSc, BCA, BE)",
        "Postgraduate": "any master's degree (for example MA, MSc, MBA, MCA)",
        "Others": "PhD, diploma, vocational training, skill certification",
        "Invalid": "a given answer that is gibberish or has nothing to do with the question"
      },
      "provisional": true
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
    ]
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
    "लागू नहीं"
  ],
  "llm": {}
}
$cfg$::jsonb, 'seed', 'Initial extraction config: the DOTS metadata columns of the persona with bands from the Katha stories (fields marked provisional await product) and the persona''s own prompt'
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
  "source_note": "Fields are the DOTS metadata columns of the persona. Category sets come from the Katha stories (still Draft); fields marked provisional wait for product confirmation, sensitive fields are filled only when the respondent states them explicitly. Districts are free text without a reference list.",
  "prompt_name": "Feedback Metadata Extraction - General Public",
  "fields": [
    {
      "name": "age",
      "kind": "integer",
      "description": "The age in whole years of the respondent, as a number. Use the number the respondent states; do not compute an age from a class, a birth year or a date of birth, and do not round a stated age. An age given as a range, or a number that cannot be an age, is not a value (put the words in raw).",
      "min": 5,
      "max": 100,
      "provisional": true
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
        "Invalid"
      ],
      "guidance": {
        "Male": "boy, man, male",
        "Female": "girl, woman, female",
        "Others": "a valid gender that is not male or female, for example non-binary, transgender, 'something else'",
        "Prefer not to say": "the respondent declines or skips the question ('skip', 'do not want to say', 'not sure')",
        "Invalid": "a given answer that is gibberish or has nothing to do with the question"
      },
      "provisional": true
    },
    {
      "name": "state",
      "kind": "category",
      "description": "The Indian state or union territory the respondent lives in, as answered to the bot's state question. Map spelling variants and abbreviations to the official name; never infer a state from a district, a language or a city. A place that is not an Indian state or union territory does not fit any allowed value.",
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
        "Delhi (NCT)",
        "Jammu and Kashmir",
        "Ladakh",
        "Lakshadweep",
        "Puducherry"
      ],
      "guidance": {
        "Andaman and Nicobar Islands": "also Andaman, Nicobar, A&N Islands",
        "Dadra and Nagar Haveli and Daman and Diu": "also Dadra and Nagar Haveli, Daman, Diu, Silvassa",
        "Delhi (NCT)": "also Delhi, New Delhi, NCT of Delhi",
        "Odisha": "also Orissa",
        "Puducherry": "also Pondicherry",
        "Uttarakhand": "also Uttaranchal",
        "Telangana": "also Telengana",
        "Jammu and Kashmir": "also J&K, Kashmir"
      }
    },
    {
      "name": "district",
      "kind": "free_text",
      "description": "The district the respondent lives in (free text). Return the commonly used official spelling of the district name (for example 'Bengaluru Urban', 'Mysuru'). If the bot asked the respondent to choose between districts, use the one they chose. Do not guess a district the respondent did not name."
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
        "Invalid"
      ],
      "guidance": {
        "Education": "teacher, lecturer, tutor, school or college staff",
        "Health": "doctor, nurse, ASHA or anganwadi worker, pharmacist, hospital staff",
        "Government and public services": "administration, police, defence, railways, utilities, other government office jobs (a teacher or nurse goes to Education or Health, the sector wins)",
        "IT and services": "software, IT, BPO, banking, consulting, other office services",
        "Manufacturing": "factory, mill, construction or workshop work",
        "Agriculture": "farmer, farm worker, dairy, fishing",
        "Retail and trade": "shop, trader, vendor, delivery, sales",
        "NGO or social sector": "NGO, social worker, community organisation",
        "Others": "any other occupation or status (homemaker, student, retired, unemployed, driver, artist, daily wage)",
        "Invalid": "a given answer that is gibberish or has nothing to do with the question"
      },
      "provisional": true
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
    ]
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
    "लागू नहीं"
  ],
  "llm": {}
}
$cfg$::jsonb, 'seed', 'Initial extraction config: the DOTS metadata columns of the persona with bands from the Katha stories (fields marked provisional await product) and the persona''s own prompt'
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
  "source_note": "Fields are the DOTS metadata columns of the persona. Category sets come from the Katha stories (still Draft); fields marked provisional wait for product confirmation, sensitive fields are filled only when the respondent states them explicitly. Districts are free text without a reference list.",
  "prompt_name": "Feedback Metadata Extraction - Educator",
  "fields": [
    {
      "name": "state",
      "kind": "category",
      "description": "The Indian state or union territory the respondent lives in, as answered to the bot's state question. Map spelling variants and abbreviations to the official name; never infer a state from a district, a language or a city. A place that is not an Indian state or union territory does not fit any allowed value.",
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
        "Delhi (NCT)",
        "Jammu and Kashmir",
        "Ladakh",
        "Lakshadweep",
        "Puducherry"
      ],
      "guidance": {
        "Andaman and Nicobar Islands": "also Andaman, Nicobar, A&N Islands",
        "Dadra and Nagar Haveli and Daman and Diu": "also Dadra and Nagar Haveli, Daman, Diu, Silvassa",
        "Delhi (NCT)": "also Delhi, New Delhi, NCT of Delhi",
        "Odisha": "also Orissa",
        "Puducherry": "also Pondicherry",
        "Uttarakhand": "also Uttaranchal",
        "Telangana": "also Telengana",
        "Jammu and Kashmir": "also J&K, Kashmir"
      }
    },
    {
      "name": "district",
      "kind": "free_text",
      "description": "The district the respondent teaches in (free text). Return the commonly used official spelling of the district name (for example 'Bengaluru Urban', 'Mysuru'). If the bot asked the respondent to choose between districts, use the one they chose. Do not guess a district the respondent did not name."
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
        "Invalid"
      ],
      "guidance": {
        "Male": "boy, man, male",
        "Female": "girl, woman, female",
        "Others": "a valid gender that is not male or female, for example non-binary, transgender, 'something else'",
        "Prefer not to say": "the respondent declines or skips the question ('skip', 'do not want to say', 'not sure')",
        "Invalid": "a given answer that is gibberish or has nothing to do with the question"
      },
      "provisional": true
    },
    {
      "name": "institution_type",
      "kind": "category",
      "description": "The type of institution the respondent teaches at (government, private or other), as answered to the bot's institution question.",
      "categories": [
        "Government",
        "Private",
        "Others",
        "Invalid"
      ],
      "guidance": {
        "Government": "government or state-run school, college or institution",
        "Private": "private school, college or institution",
        "Others": "any other kind of institution the respondent names (for example madrasa, open school, home schooling)",
        "Invalid": "a given answer that is gibberish or has nothing to do with the question"
      },
      "provisional": true
    },
    {
      "name": "teaching_level",
      "kind": "category",
      "description": "The level the respondent teaches, as a band. The bot asks 'primary, middle, high school or college': 'primary' is Primary and 'middle' is Middle; 'high school' is Secondary or Higher Secondary only when the respondent names classes (9-10 or 11-12), and 'college' is Undergraduate or Postgraduate only when the respondent names the degree level. Otherwise, and when the respondent teaches several levels, the answer fits no band (value null, put the words in raw).",
      "categories": [
        "Primary",
        "Middle",
        "Secondary",
        "Higher Secondary",
        "Undergraduate",
        "Postgraduate",
        "Others",
        "Invalid"
      ],
      "guidance": {
        "Primary": "classes 1 to 5",
        "Middle": "classes 6 to 8",
        "Secondary": "classes 9 and 10",
        "Higher Secondary": "classes 11 and 12",
        "Undergraduate": "teaches bachelor's degree students",
        "Postgraduate": "teaches master's degree students",
        "Others": "any other level or setting (for example vocational training, skill certification, PhD)",
        "Invalid": "a given answer that is gibberish or has nothing to do with the question"
      },
      "provisional": true
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
    ]
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
    "लागू नहीं"
  ],
  "llm": {}
}
$cfg$::jsonb, 'seed', 'Initial extraction config: the DOTS metadata columns of the persona with bands from the Katha stories (fields marked provisional await product) and the persona''s own prompt'
WHERE EXISTS (SELECT 1 FROM config WHERE id = '1e8f69f1-2626-58dc-a0e8-786edcfc58fe'::uuid)
  AND NOT EXISTS (SELECT 1 FROM config_version WHERE config_id = '1e8f69f1-2626-58dc-a0e8-786edcfc58fe'::uuid)
ON CONFLICT (config_id, version) DO NOTHING;
