"""
Tests of app/services/feedback_version_detect.py (pure, no database): proposing a new questionnaire version and
the instant it started from the main questions the stored sessions were asked. Synthetic questions only.
"""
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher

import pytest

from app.services.feedback_version_detect import WORDING_SIMILARITY, SessionQuestions, detect, next_key, normalise

T0 = datetime(2026, 10, 1, 6, 0, tzinfo=timezone.utc)

V1_QS = ["What is your age?", "Which state do you live in?", "What do you like about school?", "Any wish for the future?"]
V2_QS = ["What is your age?", "How do you feel about school these days?", "Which job do you want?", "Any wish for the future?",
         "Which state do you live in? (asked last now)"]


def make(n, qs, start_index=0, stamped="V1", step=timedelta(minutes=10)):
    return [SessionQuestions(f"s{start_index + i}", T0 + step * (start_index + i), stamped, tuple(qs)) for i in range(n)]


def test_normalise_ignores_case_spacing_and_punctuation_but_not_wording():
    assert normalise("  What is  your AGE?? ") == normalise("what is your age")
    assert normalise("income per year") != normalise("income per month")
    assert normalise("???") == ""


@pytest.mark.parametrize("keys,expected", [(["V1"], "V2"), (["V1", "V2"], "V3"), (["V9"], "V10"), (["base"], "V2"), (["a", "b"], "V3")])
def test_next_key(keys, expected):
    assert next_key(keys) == expected


def test_a_clean_change_is_proposed_with_the_first_session_of_the_new_questions():
    sessions = make(120, V1_QS) + make(80, V2_QS, start_index=120)
    result = detect(sessions, ["V1"])
    proposal = result["proposal"]
    assert proposal["key"] == "V2"
    assert proposal["effective_from"] == sessions[120].start.isoformat()
    assert proposal["sessions_before"] == 120 and proposal["sessions_from_start"] == 80
    assert proposal["new_question_sessions_before"] == 0 and proposal["old_question_sessions_from_start"] == 0
    assert proposal["purity"] == 1.0 and proposal["share_after"] == 1.0
    new = {q["text"] for q in result["new_questions"]}
    assert "How do you feel about school these days?" in new and "Which job do you want?" in new
    assert "What is your age?" not in new
    assert "What do you like about school?" in result["dropped_questions"]


def test_no_change_proposes_nothing():
    result = detect(make(200, V1_QS), ["V1"])
    assert result["proposal"] is None and result["new_questions"] == []
    assert "no new question found" in result["reason"]


def test_a_recorded_version_is_not_proposed_again():
    sessions = make(120, V1_QS) + make(80, V2_QS, start_index=120, stamped="V2")
    result = detect(sessions, ["V1", "V2"])
    assert result["proposal"] is None
    assert result["versions"]["V1"]["sessions"] == 120 and result["versions"]["V2"]["sessions"] == 80
    assert result["versions"]["V2"]["reference_questions"] == len(set(map(normalise, V2_QS)))


def test_the_next_change_after_a_recorded_one_is_v3():
    v3 = ["What is your age?", "Tell us about your village.", "Which job do you want?"]
    sessions = make(120, V1_QS) + make(150, V2_QS, 120, "V2") + make(60, v3, 270, "V2")
    result = detect(sessions, ["V1", "V2"])
    assert result["proposal"]["key"] == "V3"
    assert result["proposal"]["effective_from"] == sessions[270].start.isoformat()


def test_a_stray_early_session_does_not_move_the_start():
    sessions = make(120, V1_QS) + make(80, V2_QS, start_index=120)
    sessions[10] = SessionQuestions("stray", sessions[10].start, "V1", tuple(V2_QS))   # a tester saw the new text early
    result = detect(sessions, ["V1"])
    assert result["proposal"]["effective_from"] == sessions[120].start.isoformat()
    assert result["proposal"]["new_question_sessions_before"] == 1


def test_short_sessions_after_the_change_do_not_hide_it():
    after = make(80, V2_QS, start_index=120)
    after = [SessionQuestions(s.session_id, s.start, s.stamped, s.texts[:1]) if i % 5 == 0 else s for i, s in enumerate(after)]
    result = detect(make(120, V1_QS) + after, ["V1"])
    assert result["proposal"]["effective_from"] == after[1].start.isoformat() or \
        result["proposal"]["effective_from"] == after[0].start.isoformat()
    assert result["proposal"]["old_question_sessions_from_start"] > 0


def test_versions_running_side_by_side_are_reported_not_proposed():
    mixed = make(150, V1_QS)
    for i in range(150, 350):
        mixed += make(1, V1_QS if i % 2 == 0 else V2_QS, start_index=i)
    result = detect(mixed, ["V1"])
    assert result["proposal"] is None
    assert "side by side" in result["reason"] and result["details"]["share_after"] < 1
    assert result["new_questions_total"] > 0


def test_a_rare_conditional_question_is_not_a_new_version():
    sessions = make(300, V1_QS)
    for i in range(0, 300, 25):                 # 12 sessions: fewer than min_sessions
        sessions[i] = SessionQuestions(sessions[i].session_id, sessions[i].start, "V1", tuple(V1_QS) + ("Follow-up for some?",))
    assert detect(sessions, ["V1"])["proposal"] is None


def test_a_scattered_conditional_question_is_not_a_new_version():
    sessions = make(400, V1_QS)
    for i in range(0, 400, 10):                 # 40 sessions but only 10% of the sessions since it first appeared
        sessions[i] = SessionQuestions(sessions[i].session_id, sessions[i].start, "V1", tuple(V1_QS) + ("Branch question?",))
    assert detect(sessions, ["V1"])["proposal"] is None


def test_cosmetic_formatting_is_not_a_change_but_a_reworded_question_is():
    plain = make(120, V1_QS)
    cosmetic = make(80, [q.upper() + "  " for q in V1_QS], start_index=120)
    assert detect(plain + cosmetic, ["V1"])["proposal"] is None
    one_word = make(80, [V1_QS[0], "Which state do you live in these days?"] + V1_QS[2:], start_index=120)
    proposal = detect(plain + one_word, ["V1"])["proposal"]
    assert proposal is not None and proposal["effective_from"] == one_word[0].start.isoformat()


def test_sessions_stamped_with_no_recorded_version_cannot_be_compared():
    sessions = [SessionQuestions(s.session_id, s.start, "X", s.texts) for s in make(100, V1_QS)]
    result = detect(sessions, ["V1"])
    assert result["proposal"] is None and "very first session" in result["reason"]


def test_a_small_deployment_still_works_with_lower_thresholds():
    sessions = make(6, V1_QS) + make(5, V2_QS, 6)
    assert detect(sessions, ["V1"])["proposal"] is None                          # below min_sessions=20
    assert detect(sessions, ["V1"], min_sessions=3, baseline_sessions=6)["proposal"]["effective_from"] == sessions[6].start.isoformat()


def test_input_order_does_not_matter_and_empty_input_is_fine():
    sessions = make(120, V1_QS) + make(80, V2_QS, start_index=120)
    assert detect(list(reversed(sessions)), ["V1"]) == detect(sessions, ["V1"])
    assert detect([], ["V1"])["proposal"] is None


@pytest.mark.parametrize("kwargs", [{"min_sessions": 0}, {"baseline_sessions": 0}, {"min_coverage": 0}, {"min_coverage": 1.5}])
def test_bad_thresholds_are_refused(kwargs):
    with pytest.raises(ValueError):
        detect(make(5, V1_QS), ["V1"], **kwargs)


def test_no_version_key_is_refused():
    with pytest.raises(ValueError):
        detect(make(5, V1_QS), [])


def _with_bots(sessions, bot_for):
    return [SessionQuestions(x.session_id, x.start, x.stamped, x.texts, bot_for(i)) for i, x in enumerate(sessions)]


def test_a_bot_id_that_changes_with_the_questions_corroborates_the_proposal():
    sessions = _with_bots(make(120, V1_QS) + make(80, V2_QS, 120), lambda i: "88" if i < 120 else "101")
    result = detect(sessions, ["V1"])
    proposal = result["proposal"]
    assert proposal["bot_ids_before"] == ["88"] and proposal["bot_ids_from_start"] == ["101"]
    assert proposal["bot_id_changed_at_start"] is True
    assert result["versions"]["V1"]["bot_ids"] == {"88": 120, "101": 80}      # nothing is stamped V2 yet


def test_the_same_bot_edited_in_place_is_still_detected_and_says_so():
    sessions = _with_bots(make(120, V1_QS) + make(80, V2_QS, 120), lambda i: "88")
    proposal = detect(sessions, ["V1"])["proposal"]
    assert proposal is not None and proposal["bot_id_changed_at_start"] is False
    assert proposal["bot_ids_before"] == proposal["bot_ids_from_start"] == ["88"]


def test_sessions_without_a_bot_id_are_fine_and_never_decide_anything():
    proposal = detect(make(120, V1_QS) + make(80, V2_QS, 120), ["V1"])["proposal"]
    assert proposal["bot_ids_before"] == [] and proposal["bot_id_changed_at_start"] is False
    # a changed bot id with unchanged questions proposes nothing
    only_bot = _with_bots(make(200, V1_QS), lambda i: "88" if i < 100 else "101")
    assert detect(only_bot, ["V1"])["proposal"] is None


CONFIDENCE_A = "Thank you for sharing! Would you say you are feeling very confident, somewhat confident, or unsure about your future?"
CONFIDENCE_B = "Thank you for sharing! Would you say you're feeling very confident, somewhat confident, or unsure about your future?"


def test_a_wording_variant_of_one_known_question_is_listed_but_not_proposed():
    base = V2_QS + [CONFIDENCE_A]
    late = [q if q != CONFIDENCE_A else CONFIDENCE_B for q in base]
    sessions = make(150, base, stamped="V2") + make(40, late, 150, "V2")
    result = detect(sessions, ["V1", "V2"])
    assert result["proposal"] is None
    assert "wording variants" in result["reason"]
    variant = result["wording_variants"][0]
    assert variant["similar_to"] == CONFIDENCE_A and variant["nearest_known_similarity"] >= 0.9
    assert result["new_questions_total"] == 1


def test_include_wording_proposes_it_anyway():
    base = V2_QS + [CONFIDENCE_A]
    late = [q if q != CONFIDENCE_A else CONFIDENCE_B for q in base]
    sessions = make(150, base, stamped="V2") + make(40, late, 150, "V2")
    proposal = detect(sessions, ["V1", "V2"], include_wording=True)["proposal"]
    assert proposal is not None and proposal["effective_from"] == sessions[150].start.isoformat()


def test_a_real_new_question_next_to_a_wording_variant_is_still_proposed():
    base = V2_QS + [CONFIDENCE_A]
    late = [q if q != CONFIDENCE_A else CONFIDENCE_B for q in base] + ["Which hobby do you enjoy the most these days?"]
    sessions = make(150, base, stamped="V2") + make(40, late, 150, "V2")
    result = detect(sessions, ["V1", "V2"])
    assert result["proposal"] is not None
    assert any("similar_to" in q for q in result["new_questions"]) and any("similar_to" not in q for q in result["new_questions"])


# C11: the proposed instant must relabel exactly the sessions the details report --------------------------------

def _relabelled_by(proposal, sessions):
    """What `set` would do: stamp_version labels every session whose start is at or after effective_from as new."""
    effective = datetime.fromisoformat(proposal["effective_from"])
    return sum(1 for s in sessions if s.start >= effective)


def test_tied_start_instants_never_relabel_more_sessions_than_reported():
    old = make(100, V1_QS)
    tie = old[-1].start + timedelta(minutes=10)
    # the last old session and the first new one start at the very same instant
    sessions = old + [SessionQuestions("a-tie-old", tie, "V1", tuple(V1_QS))]
    sessions += [SessionQuestions(f"n{i}", tie if i == 0 else tie + timedelta(minutes=i), "V1", tuple(V2_QS))
                 for i in range(100)]
    proposal = detect(sessions, ["V1"])["proposal"]
    assert proposal is not None
    assert _relabelled_by(proposal, sessions) == proposal["sessions_from_start"]
    assert proposal["sessions_before"] + proposal["sessions_from_start"] == len(sessions)
    # the two sessions that share the instant cannot be told apart by time: the mismatch is shown, never hidden
    assert proposal["old_question_sessions_from_start"] + proposal["new_question_sessions_before"] == 1


def test_a_tied_block_is_never_split_by_the_proposed_instant():
    old = make(100, V1_QS)
    tie = old[-1].start + timedelta(minutes=10)
    sessions = old + [SessionQuestions(f"t{i}", tie, "V1", tuple(V2_QS)) for i in range(200)]  # 200 at one instant
    proposal = detect(sessions, ["V1"])["proposal"]
    assert proposal["effective_from"] == tie.isoformat()
    assert _relabelled_by(proposal, sessions) == proposal["sessions_from_start"] == 200
    assert proposal["sessions_before"] == 100


def test_all_sessions_at_one_instant_cannot_be_separated():
    t = T0
    sessions = [SessionQuestions(f"a{i}", t, "V1", tuple(V1_QS)) for i in range(100)]
    sessions += [SessionQuestions(f"b{i}", t, "V1", tuple(V2_QS)) for i in range(100)]
    result = detect(sessions, ["V1"])
    assert result["proposal"] is None
    assert "no instant" in result["reason"] or "very first session" in result["reason"]


# Thresholds: each one is pinned at its boundary, from both sides ----------------------------------------------------

def _with_early_strays(n_early, n_after=40):
    """100 old sessions of which `n_early` already carry the new question set, then `n_after` new-set sessions."""
    old = make(100, V1_QS)
    for k in range(n_early):
        i = 10 + 7 * k
        old[i] = SessionQuestions(old[i].session_id, old[i].start, "V1", tuple(V2_QS))
    return old + make(n_after, V2_QS, start_index=100)


def test_purity_exactly_at_the_minimum_is_proposed():
    # 4 of the 40 sessions with the new questions come before the cut: purity = 36 / 40 = MIN_PURITY exactly
    sessions = _with_early_strays(4, n_after=36)
    result = detect(sessions, ["V1"], min_coverage=0.1, baseline_sessions=10)
    assert result["proposal"] is not None, result["reason"]
    assert result["proposal"]["purity"] == 0.9 and result["proposal"]["new_question_sessions_before"] == 4


def test_purity_below_the_minimum_proposes_nothing_and_says_the_sets_run_side_by_side():
    # 6 of 46 before the cut: purity 40 / 46 = 0.87 < MIN_PURITY
    result = detect(_with_early_strays(6, n_after=40), ["V1"], min_coverage=0.1, baseline_sessions=10)
    assert result["proposal"] is None
    assert "side by side" in result["reason"] and result["details"]["purity"] < 0.9


def _similar(known, new):
    return make(40, [known], 0) + make(40, [new], 40)


def test_a_new_text_at_exactly_the_similarity_threshold_is_a_wording_variant_and_is_not_proposed():
    known, variant = "abcdefghij", "abcdefghiz"          # 9 of 10 characters match: ratio 2 * 9 / 20 = 0.9
    assert SequenceMatcher(None, known, variant).ratio() == WORDING_SIMILARITY
    result = detect(_similar(known, variant), ["V1"], baseline_sessions=40)
    assert result["proposal"] is None and "wording variants" in result["reason"]
    assert [q["similar_to"] for q in result["wording_variants"]] == [known]


def test_a_new_text_just_below_the_similarity_threshold_is_a_real_change():
    known, other = "abcdefghijklmnopqrs", "abcdefgXijklmnopqXs"      # 17 of 19 match: 34 / 38 = 0.895
    assert SequenceMatcher(None, known, other).ratio() < WORDING_SIMILARITY
    result = detect(_similar(known, other), ["V1"], baseline_sessions=40)
    assert result["wording_variants"] == [] and result["proposal"] is not None
    assert result["proposal"]["key"] == "V2"


def _sparse_text_sessions(spread, first_at, count, total=200):
    """`total` old sessions; one extra question appears in `count` of them, every `spread` sessions from `first_at`."""
    sessions = make(total, V1_QS)
    for k in range(count):
        i = first_at + spread * k
        sessions[i] = SessionQuestions(sessions[i].session_id, sessions[i].start, "V1", tuple(V1_QS) + ("Is this a sparse question?",))
    return sessions


def test_a_text_seen_often_enough_but_in_too_small_a_share_of_the_sessions_is_not_new():
    # 20 sightings (min_sessions) over the 100 sessions since it first appeared: coverage 0.2 < min_coverage 0.3
    result = detect(_sparse_text_sessions(spread=5, first_at=100, count=20), ["V1"])
    assert result["new_questions"] == [] and result["new_questions_total"] == 0
    assert result["proposal"] is None and "no new question found" in result["reason"]


def test_a_text_at_exactly_the_minimum_coverage_counts_as_new():
    # 30 sightings, the first at index 100: coverage 30 / 100 = 0.3 = min_coverage exactly
    result = detect(_sparse_text_sessions(spread=3, first_at=100, count=30), ["V1"])
    assert result["new_questions_total"] == 1 and result["new_questions"][0]["coverage"] == 0.3


def test_a_text_seen_in_fewer_than_min_sessions_is_not_new_whatever_its_coverage():
    sessions = make(60, V1_QS) + make(19, V1_QS + ["A rare question about trees?"], start_index=60)
    result = detect(sessions, ["V1"])
    assert result["new_questions_total"] == 0
