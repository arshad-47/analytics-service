SELECT DISTINCT
    cs.id::text AS submission_id
FROM chatbot_chatsession cs
WHERE cs.session_type = ANY(%(session_types)s::text[]);
