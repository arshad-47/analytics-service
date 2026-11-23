SELECT
    cs.company_bot_id AS bot_id,
    cb.name AS bot_name,
    cs.session_type,
    cs.session_status AS session_status,
    cs.session,
    cs.id AS submission_id,
    cs.language,
    ROW_NUMBER() OVER (
        PARTITION BY cc.session
        ORDER BY cc.created_at, cc.id
    ) + 1 AS sequence,
    cc.id AS conversation_row_id,
    cc.message,
    cc.translated_message,
    cc.created_at,
    cc.updated_at,
    cc.stage,
    cc.receiver_id,
    cc.sender_id,
    cc.audio_file,
    cc.file_url,
    cc.other_params
FROM chatbot_companychat cc
JOIN chatbot_chatsession cs ON cc.session = cs.session
JOIN chatbot_companybot cb ON cb.id = cs.company_bot_id
WHERE cs.session_type = ANY(%(session_types)s::text[])
  AND (cardinality(%(submission_ids)s::bigint[]) = 0 OR cs.id = ANY(%(submission_ids)s::bigint[]))

UNION ALL

SELECT
    cs.company_bot_id,
    cb.name,
    cs.session_type,
    cs.session_status,
    cs.session,
    cs.id,
    cs.language,
    1 AS sequence,
    NULL::bigint,
    intro.msg_en,
    intro.msg_trans,
    NULL::timestamptz,
    NULL::timestamptz,
    NULL::text,
    NULL::bigint,
    NULL::bigint,
    NULL::text,
    NULL::text,
    NULL::jsonb
FROM chatbot_chatsession cs
JOIN chatbot_companybot cb ON cb.id = cs.company_bot_id
JOIN LATERAL (
    SELECT MIN(created_at) AS first_at
    FROM chatbot_companychat
    WHERE session = cs.session
) s ON s.first_at IS NOT NULL
JOIN LATERAL (
    SELECT
        (SELECT COALESCE(NULLIF(h.alt_introductory_message, ''), h.introductory_message)
           FROM chatbot_historicalbotvernacular h
          WHERE h.company_bot_id = cs.company_bot_id AND h.language = 'en'
            AND h.history_type <> '-' AND h.history_date <= s.first_at
          ORDER BY h.history_date DESC, h.history_id DESC LIMIT 1) AS msg_en,
        (SELECT COALESCE(NULLIF(h.alt_introductory_message, ''), h.introductory_message)
           FROM chatbot_historicalbotvernacular h
          WHERE h.company_bot_id = cs.company_bot_id AND h.language = cs.language
            AND h.history_type <> '-' AND h.history_date <= s.first_at
          ORDER BY h.history_date DESC, h.history_id DESC LIMIT 1) AS msg_trans
) intro ON (intro.msg_en IS NOT NULL OR intro.msg_trans IS NOT NULL)
WHERE cs.session_type = ANY(%(session_types)s::text[])
  AND (cardinality(%(submission_ids)s::bigint[]) = 0 OR cs.id = ANY(%(submission_ids)s::bigint[]))
ORDER BY bot_id, session, sequence;
