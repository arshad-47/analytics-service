"""
Pure (no database) tests for the second CodeRabbit round on PR 36, config / version side. Synthetic data only.
"""
from datetime import timedelta

from app.services.feedback_version_detect import SessionQuestions, detect

from test_feedback_version_detect import T0, V1_QS, V2_QS, _relabelled_by, make


def test_a_first_old_only_session_at_the_best_boundary_does_not_hide_the_new_question_sessions_of_its_instant():
    """100 baseline sessions, then ONE old-only and 20 new-question sessions that all start at the same instant."""
    old = make(100, V1_QS)
    tie = old[-1].start + timedelta(minutes=10)
    sessions = old + [SessionQuestions("a-old-first", tie, "V1", tuple(V1_QS))]
    sessions += [SessionQuestions(f"n{i}", tie, "V1", tuple(V2_QS)) for i in range(20)]
    result = detect(sessions, ["V1"])
    assert result["proposal"] is not None, result["reason"]
    proposal = result["proposal"]
    assert proposal["effective_from"] == tie.isoformat()
    assert proposal["sessions_before"] == 100 and proposal["sessions_from_start"] == 21
    assert _relabelled_by(proposal, sessions) == proposal["sessions_from_start"]
    # the instant cannot separate the one old-only session from the 20: the mismatch is reported, not hidden
    assert proposal["old_question_sessions_from_start"] == 1 and proposal["new_question_sessions_before"] == 0


def test_a_whole_instant_without_new_question_evidence_is_still_skipped():
    old = make(100, V1_QS)
    tie = old[-1].start + timedelta(minutes=10)
    sessions = old + [SessionQuestions(f"o{i}", tie, "V1", tuple(V1_QS)) for i in range(3)]
    sessions += make(60, V2_QS, start_index=105)
    proposal = detect(sessions, ["V1"])["proposal"]
    assert proposal["sessions_before"] == 103 and proposal["effective_from"] == sessions[103].start.isoformat()
    assert _relabelled_by(proposal, sessions) == proposal["sessions_from_start"] == 60
