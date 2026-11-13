-- ONE unified extract for every submission in chatbot_story (stories + discussions, all states).
-- Classification (story vs discussion) happens in Python from session_type / flow.
-- Window is on chatbot_story.created_at: (window_start, window_end].
SELECT
    s.id                         AS story_id,
    s.title,
    s.content,
    s.blurb,
    s.objective,
    s.action_steps,
    s.impact,
    s.state                      AS story_state,
    s.district                   AS story_district,
    s.location                   AS story_location,
    s.session                    AS session_id,
    s.created_at                 AS story_created_at,
    s.other_params::jsonb        AS other_params,
    cc.slug                      AS tenant_slug,
    sess.id                      AS chat_session_id,       -- = event.submissionId
    sess.created_at              AS session_created_at,
    sess.session_type,
    to_json(pr.statements)::text AS project_challenges,
    pdf.pdf_url,
    to_json(img.image_urls)::text AS image_urls
FROM chatbot_story s
LEFT JOIN chatbot_profile cp ON s.author_id = cp.id
LEFT JOIN chatbot_company cc ON cp.company_id = cc.id
-- Earliest chat-session row per session; rows with a discussion session_type win,
-- which reproduces the two existing report conventions (discussion vs story).
LEFT JOIN LATERAL (
    SELECT id, created_at, session_type
    FROM chatbot_chatsession
    WHERE session = s.session
    ORDER BY COALESCE(session_type = ANY(%(discussion_types)s::text[]), FALSE) DESC,
             created_at ASC, id ASC
    LIMIT 1
) sess ON TRUE
LEFT JOIN LATERAL (
    SELECT array_agg(actual_problem_statement ORDER BY actual_problem_statement) AS statements
    FROM shikshalokam.project
    WHERE story_id = s.id
      AND NULLIF(btrim(actual_problem_statement), '') IS NOT NULL
) pr ON TRUE
LEFT JOIN LATERAL (
    SELECT 'https://mohini-static.shikshalokam.org/' || file AS pdf_url
    FROM chatbot_storymedia
    WHERE story_id = s.id 
      AND media_type = 'application/pdf'
      AND file IS NOT NULL 
      AND btrim(file) <> ''
    ORDER BY id DESC
    LIMIT 1
) pdf ON TRUE
LEFT JOIN LATERAL (
    SELECT
        COALESCE(
            array_agg(
                COALESCE(NULLIF(btrim(sm.file_url), ''), 'https://mohini-static.shikshalokam.org/' || sm.file)
                ORDER BY sm.id ASC
            ) FILTER (
                WHERE (sm.media_type LIKE 'image/%' OR (sm.media_type IS NULL AND sm.file ~* '\.(jpe?g|png|webp|gif|bmp)$'))
                  AND (
                      (sm.file_url IS NOT NULL AND btrim(sm.file_url) <> '')
                      OR (sm.file IS NOT NULL AND btrim(sm.file) <> '')
                  )
            ),
            ARRAY[]::text[]
        ) AS image_urls
    FROM chatbot_storymedia sm
    WHERE sm.story_id = s.id
) img ON TRUE
WHERE (cardinality(%(tenants)s::text[]) = 0 OR cc.slug = ANY(%(tenants)s::text[]))
  AND (cardinality(%(submission_ids)s::text[]) = 0 OR sess.id::text = ANY(%(submission_ids)s::text[]))
  AND (cardinality(%(session_types)s::text[]) = 0 OR sess.session_type = ANY(%(session_types)s::text[]))
  AND (%(window_start)s::text IS NULL OR s.created_at > %(window_start)s::timestamptz)
  AND (%(window_end)s::text IS NULL OR s.created_at <= %(window_end)s::timestamptz)
  AND (cp.source IS NULL OR cp.source <> ALL(%(excluded_sources)s::text[]))
ORDER BY s.created_at ASC, s.id ASC