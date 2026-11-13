SELECT DISTINCT
    sess.id::text   AS submission_id
FROM chatbot_story s
LEFT JOIN chatbot_profile cp ON s.author_id = cp.id
LEFT JOIN chatbot_company cc ON cp.company_id = cc.id
-- Pick the exact single canonical chat session row for this story, exactly matching submission.sql
JOIN LATERAL (
    SELECT id, session_type
    FROM chatbot_chatsession
    WHERE session = s.session
    ORDER BY COALESCE(session_type = ANY(%(discussion_types)s::text[]), FALSE) DESC,
             created_at ASC, id ASC
    LIMIT 1
) sess ON TRUE
WHERE (cardinality(%(tenants)s::text[]) = 0 OR cc.slug = ANY(%(tenants)s::text[]))
  AND (cardinality(%(session_types)s::text[]) = 0 OR sess.session_type = ANY(%(session_types)s::text[]))
  AND (cp.source IS NULL OR cp.source <> ALL(%(excluded_sources)s::text[]));
