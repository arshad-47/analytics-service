SELECT DISTINCT
    cs.id::text AS submission_id
FROM chatbot_chatsession cs
LEFT JOIN chatbot_profile cp ON cs.profile_id = cp.id
WHERE cs.session_type = ANY(%(session_types)s::text[])
  AND (cp.source IS NULL OR cp.source <> ALL(%(excluded_sources)s::text[]));

