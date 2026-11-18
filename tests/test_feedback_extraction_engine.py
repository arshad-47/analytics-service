"""
Pure tests (no database, no network, fake LLM) of the per-persona extraction ENGINE: the config's `prompt_name`, the
'integer' field kind (min / max / numeric strings / out of range), the `sensitive` rule line, the parser rejecting a
bad config, prompt resolution by name (against a fake connection), the prompt_version actually used reaching the write,
and the CLI's upgrade-config wiring and summaries. The configs here are built in the tests themselves: they do not
depend on the seeded field lists. SYNTHETIC data only.
"""
import asyncio
import copy
import importlib.util
import inspect
import json
import traceback
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

import app.database.feedback_extraction_operations as ops
from app.services import feedback_extraction as fx

ROOT = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("run_feedback_extraction_cli_engine", ROOT / "tools" / "run_feedback_extraction.py")
cli = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cli)

SENSITIVE_LINE = "(sensitive: fill only when the respondent states it explicitly; never infer)"
MARKER = "zzq-engine-marker"


def _raw_config(**top):
    raw = {
        "schema_version": 1, "persona": "Youth", "session_type": "Youth_Voice", "prompt_name": "Test Prompt",
        "fields": [
            {"name": "age", "kind": "integer", "description": "The age in years.", "min": 5, "max": 100},
            {"name": "gender", "kind": "category", "description": "Gender.", "categories": ["Male", "Female"]},
            {"name": "district", "kind": "free_text", "description": "The district."},
            {"name": "disability", "kind": "category", "description": "Disability.", "categories": ["Yes", "No"],
             "sensitive": True, "provisional": True},
        ],
    }
    raw.update(top)
    return raw


def _config(**top):
    return fx.parse_extraction_config(_raw_config(**top))


def _answer(**values):
    """A model answer for _raw_config; keyword args name the `value` of a field (default null)."""
    fields = {n: {"value": values.get(n), "raw": None} for n in ("age", "gender", "district", "disability")}
    return json.dumps({"fields": fields, "pii": {"detected": False, "types": []}})


def _field(result, name):
    return {f.field_name: f for f in result.fields}[name]


# ------------------------------------------------------------------------------------------------ config parser

def test_prompt_name_is_required_and_kept_exactly():
    assert _config().prompt_name == "Test Prompt"
    assert _config(prompt_name="Youth Metadata Extraction").prompt_name == "Youth Metadata Extraction"
    raw = _raw_config()
    del raw["prompt_name"]
    with pytest.raises(fx.ExtractionConfigError, match="needs prompt_name.*no default prompt"):
        fx.parse_extraction_config(raw)


@pytest.mark.parametrize("bad", ["", "   ", " Youth", "Youth ", None, 5, True, ["x"], {"a": 1}])
def test_prompt_name_when_present_must_be_a_clean_non_empty_string(bad):
    with pytest.raises(fx.ExtractionConfigError, match="prompt_name"):
        _config(prompt_name=bad)


def test_integer_field_is_parsed_with_its_bounds_and_flags():
    config = _config()
    age, gender, district, disability = config.fields
    assert (age.kind, age.min_value, age.max_value, age.sensitive, age.categories) == ("integer", 5, 100, False, ())
    assert (gender.min_value, gender.max_value, gender.sensitive) == (None, None, False)
    assert disability.sensitive is True and disability.provisional is True and district.sensitive is False


@pytest.mark.parametrize("patch, message", [
    ({"min": None}, "integer min and max"),                      # min left out below
    ({"max": "100"}, "integer min and max"),
    ({"min": 5.0}, "integer min and max"),
    ({"min": True}, "integer min and max"),                      # a boolean is not an int bound
    ({"max": False}, "integer min and max"),
    ({"min": 101}, "min must not be greater than max"),
    ({"max": 10 ** 16}, "within"),
    ({"categories": ["a"]}, "categories only together with bands"),
    ({"guidance": {"a": "b"}}, "has no guidance"),
    ({"sensitive": "yes"}, "sensitive must be a boolean"),
    ({"sensitive": 1}, "sensitive must be a boolean"),
    ({"provisional": "yes"}, "provisional must be a boolean"),
    ({"surprise": 1}, "unknown key"),
])
def test_parser_rejects_a_bad_integer_field(patch, message):
    raw = _raw_config()
    field = raw["fields"][0]
    for key, value in patch.items():
        if value is None:
            del field[key]
        else:
            field[key] = value
    with pytest.raises(fx.ExtractionConfigError, match=message):
        fx.parse_extraction_config(raw)


def test_min_equal_to_max_and_negative_bounds_are_allowed():
    raw = _raw_config()
    raw["fields"][0].update({"min": -3, "max": -3})
    assert fx.parse_extraction_config(raw).fields[0].min_value == -3


@pytest.mark.parametrize("index, extra", [(1, {"min": 1}), (1, {"max": 2}), (2, {"min": 1}), (2, {"max": 2})])
def test_min_and_max_belong_to_integer_fields_only(index, extra):
    raw = _raw_config()
    raw["fields"][index].update(extra)
    with pytest.raises(fx.ExtractionConfigError, match="no min or max"):
        fx.parse_extraction_config(raw)


def test_sensitive_is_accepted_on_every_kind_and_unknown_kind_is_still_rejected():
    raw = _raw_config()
    for item in raw["fields"]:
        item["sensitive"] = True
    assert all(f.sensitive for f in fx.parse_extraction_config(raw).fields)
    raw["fields"][0]["kind"] = "decimal"
    with pytest.raises(fx.ExtractionConfigError, match="kind must be one of"):
        fx.parse_extraction_config(raw)


# ------------------------------------------------------------------------------------------------ rendering

def test_integer_and_sensitive_fields_are_rendered_for_the_model():
    config = _config()
    prompt = fx.render_prompt("{{fields}}", "{{field_names}}", config, fx.build_conversation([]))
    assert "1. age (a whole number from 5 to 100; return it as a JSON number, null when not stated): The age in years." in prompt
    assert prompt.count(SENSITIVE_LINE) == 1                       # only the sensitive field carries it
    lines = prompt.splitlines()
    index = next(i for i, line in enumerate(lines) if line.startswith("4. disability"))
    assert lines[index + 2].strip() == SENSITIVE_LINE and "Allowed values: Yes; No" in lines[index + 1]


def test_sensitive_rule_is_rendered_for_an_integer_and_a_free_text_field_too():
    raw = _raw_config()
    raw["fields"][0]["sensitive"] = True
    raw["fields"][2]["sensitive"] = True
    config = fx.parse_extraction_config(raw)
    text = fx.render_prompt("{{fields}}", "", config, fx.build_conversation([]))
    assert text.count(SENSITIVE_LINE) == 3


def test_sensitive_does_not_change_validation():
    plain = _config()
    raw = _raw_config()
    raw["fields"][3]["sensitive"] = False
    relaxed = fx.parse_extraction_config(raw)
    answer = _answer(disability="Maybe", age="17")

    def validated(result):      # value and status; raw_value of a sensitive field is deliberately not stored
        return [(f.field_name, f.value, f.status, f.confidence) for f in result.fields]

    assert validated(fx.parse_model_response(answer, plain)) == validated(fx.parse_model_response(answer, relaxed))
    assert _field(fx.parse_model_response(answer, plain), "disability").status == "unmapped"


# ------------------------------------------------------------------------------------------------ integer answers

@pytest.mark.parametrize("value, expected", [
    (17, "17"), ("17", "17"), (" 17 ", "17"), ("017", "17"), (17.0, "17"), (5, "5"), (100, "100"), ("100", "100"),
])
def test_integer_inside_the_bounds_is_extracted_as_the_canonical_decimal_string(value, expected):
    field = _field(fx.parse_model_response(_answer(age=value), _config()), "age")
    assert (field.value, field.status) == (expected, "extracted")


@pytest.mark.parametrize("value", [4, 101, "4", "101", -1, "-1", 0, 10 ** 30, "9" * 30, 1e30])
def test_integer_outside_the_bounds_is_unmapped_with_the_raw_kept(value):
    field = _field(fx.parse_model_response(_answer(age=value), _config()), "age")
    assert field.value == "Invalid response" and field.status == "unmapped" and field.raw_value is not None and field.raw_value != ""


@pytest.mark.parametrize("value", [17.5, "17.5", "17.0", "1e1", "seventeen", "17 years", "0x11", "१७", "+17", "--17", "1,7",
                                   True, False, float("1e400")])
def test_a_non_integer_is_unmapped_never_coerced(value):
    raw_json = _answer(age=0).replace('"value": 0', f'"value": {json.dumps(value) if value != float("1e400") else "1e400"}', 1)
    field = _field(fx.parse_model_response(raw_json, _config()), "age")
    assert field.value == "Invalid response" and field.status == "unmapped"
    assert field.raw_value is not None


def test_integer_null_blank_and_placeholders_are_not_provided():
    for value in (None, "", "   ", "N/A", "null", "not provided", "-"):
        field = _field(fx.parse_model_response(_answer(age=value), _config()), "age")
        assert (field.value, field.raw_value, field.status) == ("Not answered", None, "not_provided"), value


def test_integer_null_with_the_respondents_words_in_raw_is_unmapped_and_keeps_them():
    answer = json.dumps({"fields": {"age": {"value": None, "raw": "about sixteen or so"}, "gender": {"value": None, "raw": None},
                                    "district": {"value": None, "raw": None}, "disability": {"value": None, "raw": None}},
                         "pii": {"detected": False, "types": []}})
    field = _field(fx.parse_model_response(answer, _config()), "age")
    assert (field.value, field.raw_value, field.status) == ("Invalid response", "about sixteen or so", "unmapped")


def test_integer_raw_is_the_models_own_words_when_given_else_its_rendering():
    answer = json.dumps({"fields": {"age": {"value": 17, "raw": "i am 17"}, "gender": {"value": None, "raw": None},
                                    "district": {"value": None, "raw": None}, "disability": {"value": None, "raw": None}},
                         "pii": {"detected": False, "types": []}})
    assert _field(fx.parse_model_response(answer, _config()), "age").raw_value == "i am 17"
    bad = _field(fx.parse_model_response(_answer(age=17.5), _config()), "age")
    assert bad.raw_value == "17.5"


@pytest.mark.parametrize("value", [[17], {"n": 17}])
def test_an_integer_value_that_is_a_list_or_object_is_an_output_error_without_model_text(value):
    raw_json = _answer(age=0).replace('"value": 0', f'"value": {json.dumps(value)}', 1)
    with pytest.raises(fx.ExtractionOutputError) as info:
        fx.parse_model_response(raw_json, _config())
    assert "17" not in str(info.value)


def test_a_category_and_a_free_text_field_still_reject_a_json_number():
    for name in ("gender", "district"):
        answer = json.loads(_answer())
        answer["fields"][name]["value"] = 5
        with pytest.raises(fx.ExtractionOutputError, match="must be a string or null"):
            fx.parse_model_response(json.dumps(answer), _config())


def test_unknown_keys_and_missing_fields_are_still_rejected_with_integer_fields():
    answer = json.loads(_answer(age=17))
    answer["fields"]["age"]["extra"] = 1
    with pytest.raises(fx.ExtractionOutputError, match="unexpected key"):
        fx.parse_model_response(json.dumps(answer), _config())
    answer = json.loads(_answer(age=17))
    del answer["fields"]["age"]
    with pytest.raises(fx.ExtractionOutputError, match="exactly the configured fields"):
        fx.parse_model_response(json.dumps(answer), _config())
    with pytest.raises(fx.ExtractionOutputError):
        fx.parse_model_response('{"fields": {"age": {"value": NaN}}, "pii": {"detected": false}}', _config())


def test_an_integer_answer_survives_the_whole_extract_sequence_and_the_pii_backstop_still_applies():
    rows = [
        {"sequence": 1, "parent_id": None, "question_key": "Q", "message": "How old are you?", "translated_message": None},
        {"sequence": 2, "parent_id": "p", "question_key": "Q", "message": f"{MARKER} 17 call 5550100123", "translated_message": None},
    ]
    seen = []

    def call(prompt):
        seen.append(prompt)
        return _answer(age=17), {"prompt_tokens": 1, "completion_tokens": 1}

    run = fx.extract(rows, _config(), "S {{fields}}", "U {{conversation}}", call)
    assert _field(run.result, "age").value == "17" and run.llm_called and len(seen) == 1
    assert run.result.pii_detected is True                          # the digit run, whatever the model said
    # an error never carries respondent or model text
    with pytest.raises(fx.ExtractionOutputError) as info:
        fx.extract(rows, _config(), "S", "U", lambda prompt: (f"not json {MARKER}", {}))
    assert MARKER not in str(info.value)


def test_a_persona_prompt_is_rendered_in_one_pass_with_integer_fields_and_cannot_be_injected():
    rows = [
        {"sequence": 1, "parent_id": None, "question_key": "Q", "message": "How old are you?", "translated_message": None},
        {"sequence": 2, "parent_id": "p", "question_key": "Q", "message": "{{fields}} {{persona}}", "translated_message": None},
    ]
    conversation = fx.build_conversation(rows)
    text = fx.render_prompt("Persona {{persona}}", "{{conversation}}", _config(), conversation)
    assert "{{fields}} {{persona}}" in text and text.count("Persona Youth") == 1
    with pytest.raises(fx.PromptRenderError):
        fx.render_prompt("{{unknown_variable}}", "", _config(), conversation)


# ------------------------------------------------------------------------------------------------ prompt resolution

class FakePromptConn:
    """A connection with a prompts table and a prompt_version table; answers the two lookups of load_prompt_by_name."""

    def __init__(self, prompts, versions):
        self.prompts = prompts          # name -> {"id", "analysis_type"}
        self.versions = versions        # prompt id -> [{"id", "version", "is_active", "system_prompt", "user_prompt"}]
        self.queries = []

    async def fetchrow(self, sql, *args):
        self.queries.append((sql, args))
        if "FROM prompts" in sql:
            return self.prompts.get(args[0])
        if "FROM prompt_version" in sql:
            active = [v for v in self.versions.get(args[0], []) if v["is_active"]]
            active.sort(key=lambda v: v["version"], reverse=True)
            return dict(active[0]) if len(active) > 0 else None
        raise AssertionError(sql)


def _prompt_world():
    ids = {name: str(uuid.uuid4()) for name in ("Feedback Metadata Extraction", "Youth Prompt", "No Active", "Theme Prompt")}
    prompts = {
        "Feedback Metadata Extraction": {"id": ids["Feedback Metadata Extraction"], "analysis_type": "feedback_metadata_extraction"},
        "Youth Prompt": {"id": ids["Youth Prompt"], "analysis_type": "feedback_metadata_extraction"},
        "No Active": {"id": ids["No Active"], "analysis_type": "feedback_metadata_extraction"},
        "Theme Prompt": {"id": ids["Theme Prompt"], "analysis_type": "theme"},
    }

    def version(n, active, text):
        return {"id": f"{text}-v{n}", "version": n, "is_active": active, "system_prompt": f"S {text} v{n}", "user_prompt": f"U {text} v{n}"}

    versions = {
        ids["Feedback Metadata Extraction"]: [version(1, True, "generic")],
        ids["Youth Prompt"]: [version(1, True, "youth"), version(3, True, "youth"), version(4, False, "youth"), version(2, True, "youth")],
        ids["No Active"]: [version(1, False, "none")],
        ids["Theme Prompt"]: [version(1, True, "theme")],
    }
    return FakePromptConn(prompts, versions)


def test_a_named_prompt_resolves_to_its_highest_active_version():
    row = asyncio.run(ops.load_prompt_for_config(_prompt_world(), _config(prompt_name="Youth Prompt")))
    assert row["id"] == "youth-v3" and row["version"] == 3 and row["name"] == "Youth Prompt"
    assert row["system_prompt"] == "S youth v3" and row["user_prompt"] == "U youth v3"


def test_a_config_object_without_a_prompt_name_is_refused_not_answered_with_a_default_prompt():
    bare = fx.ExtractionConfig("Youth", "Youth_Voice", _config().fields, fx.PII_TYPES, {}, None)   # only a hand-built one
    conn = _prompt_world()
    with pytest.raises(ops.ExtractionSetupError, match="prompt_name is required"):
        asyncio.run(ops.load_prompt_for_config(conn, bare))
    assert conn.queries == []


def test_a_name_that_does_not_exist_fails_loudly_without_falling_back_to_the_generic_prompt():
    conn = _prompt_world()
    with pytest.raises(RuntimeError, match="No prompt named 'Missing Prompt'"):
        asyncio.run(ops.load_prompt_for_config(conn, _config(prompt_name="Missing Prompt")))
    assert all("Feedback Metadata Extraction" not in str(args) for _, args in conn.queries)   # never looked up the generic one


def test_a_name_without_an_active_version_fails_loudly():
    with pytest.raises(RuntimeError, match="'No Active' has no active prompt version"):
        asyncio.run(ops.load_prompt_for_config(_prompt_world(), _config(prompt_name="No Active")))


def test_a_name_of_a_prompt_with_another_analysis_type_is_refused():
    with pytest.raises(RuntimeError, match="analysis_type 'theme'"):
        asyncio.run(ops.load_prompt_for_config(_prompt_world(), _config(prompt_name="Theme Prompt")))


def test_a_missing_prompt_is_an_extraction_setup_error_which_is_a_runtime_error():
    conn = _prompt_world()
    with pytest.raises(ops.ExtractionSetupError, match="No prompt named 'Test Prompt'") as caught:
        asyncio.run(ops.load_prompt_for_config(conn, _config()))
    assert isinstance(caught.value, RuntimeError)


@pytest.mark.parametrize("name", ["", None, 0, ["Youth Prompt"], b"Youth Prompt"])
def test_a_prompt_name_that_is_not_a_non_empty_string_is_refused_before_any_query(name):
    # emptiness is tested explicitly: none of these values (all of them false-ish or not text) reaches the database
    conn = _prompt_world()
    with pytest.raises(ValueError, match="non-empty string"):
        asyncio.run(ops.load_prompt_by_name(conn, name))
    assert conn.queries == []


def test_a_whitespace_name_is_looked_up_as_it_is_and_a_prompt_version_numbered_zero_is_still_a_version():
    conn = _prompt_world()
    with pytest.raises(ops.ExtractionSetupError, match="No prompt named '  '"):
        asyncio.run(ops.load_prompt_by_name(conn, "  "))                      # no stripping, no default: not found
    zero = FakePromptConn(
        {"Zero": {"id": "p0", "analysis_type": "feedback_metadata_extraction"}},
        {"p0": [{"id": "z-v0", "version": 0, "is_active": True, "system_prompt": "S", "user_prompt": "U"}]})
    row = asyncio.run(ops.load_prompt_by_name(zero, "Zero"))
    assert row["id"] == "z-v0" and row["version"] == 0                          # 0 is a value, not "missing"


# ------------------------------------------------------------------------------------------------ the used prompt is what is stored

class FakeEnginePool:
    def __init__(self, conn):
        self.conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self.conn


def test_extract_submission_sends_the_named_prompt_and_stores_the_version_it_used(monkeypatch):
    conn = _prompt_world()
    config = _config(prompt_name="Youth Prompt")
    rows = [
        {"sequence": 1, "parent_id": None, "question_key": "Q", "message": "How old are you?", "translated_message": None},
        {"sequence": 2, "parent_id": "p", "question_key": "Q", "message": "17", "translated_message": None},
    ]
    config_version_id = str(uuid.uuid4())

    async def fake_inputs(c, submission_id, tenant_code):
        return {"session_type": "Youth_Voice", "questionnaire_version": "V2", "identity": ("Youth_Voice", "mitra", None),
                "rows": rows, "fingerprint": ops.rows_fingerprint(
            [dict(r) for r in rows]), "loaded_at": None, "config_row": {"id": config_version_id, "config": config}, "closing_keys": []}

    saved = {}

    async def fake_save(c, **kwargs):
        saved.update(kwargs)

    monkeypatch.setattr(ops, "load_submission_inputs", fake_inputs)
    monkeypatch.setattr(ops, "save_extraction", fake_save)
    prompts_sent = []

    def llm(prompt, model=None, max_tokens=None, timeout=None):
        prompts_sent.append(prompt)
        return _answer(age=17), {"prompt_tokens": 3, "completion_tokens": 2}

    outcome = asyncio.run(ops.extract_submission(FakeEnginePool(conn), "s1", "mitra", llm_function=llm))
    assert prompts_sent == ["S youth v3\n\nU youth v3"]
    assert saved["prompt_version_id"] == "youth-v3" and saved["config_version_id"] == config_version_id
    assert saved["llm_log"]["prompt_version_id"] == "youth-v3"
    assert outcome.prompt_version_id == "youth-v3" and outcome.prompt_name == "Youth Prompt"
    assert outcome.field_kinds["age"] == "integer"

    # a config that names a prompt that is not there: no LLM call at all, loud error
    monkeypatch.setattr(ops, "load_submission_inputs", fake_inputs)
    config_missing = _config(prompt_name="Missing Prompt")

    async def fake_inputs_missing(c, submission_id, tenant_code):
        data = await fake_inputs(c, submission_id, tenant_code)
        data["config_row"] = {"id": config_version_id, "config": config_missing}
        return data

    monkeypatch.setattr(ops, "load_submission_inputs", fake_inputs_missing)
    prompts_sent.clear()
    with pytest.raises(RuntimeError, match="No prompt named 'Missing Prompt'"):
        asyncio.run(ops.extract_submission(FakeEnginePool(conn), "s1", "mitra", llm_function=llm))
    assert prompts_sent == []


# ------------------------------------------------------------------------------------------------ CLI

def test_parser_knows_run_and_show_config_and_no_longer_upgrade_config():
    assert cli.build_parser().parse_args(["show-config"]).command == "show-config"
    assert cli.build_parser().parse_args(["show-config", "--session-type", "Youth_Voice"]).session_type == "Youth_Voice"
    assert cli.build_parser().parse_args(["run", "--session-type", "Youth_Voice", "--tenant-code", "t1"]).command == "run"
    # the seed is seed_config.sql and never overwrites: an existing database gets new content through a new
    # config_version (docs/feedback-extraction-bands.md), so the old subcommand is gone
    for argv in (["upgrade-config"], ["upgrade-config", "--dry-run"]):
        with pytest.raises(SystemExit) as info:
            cli.build_parser().parse_args(argv)
        assert info.value.code == 2


def test_summarise_config_lists_prompt_name_and_field_flags_without_bands():
    raw = _raw_config(prompt_name="Youth Prompt")
    summary = cli.summarise_config(raw)
    assert summary["prompt_name"] == "Youth Prompt"
    assert summary["fields"] == [
        {"name": "age", "kind": "integer", "sensitive": False, "provisional": False, "multi": False, "versions": None, "applies_when": None},
        {"name": "gender", "kind": "category", "sensitive": False, "provisional": False, "multi": False, "versions": None, "applies_when": None},
        {"name": "district", "kind": "free_text", "sensitive": False, "provisional": False, "multi": False, "versions": None, "applies_when": None},
        {"name": "disability", "kind": "category", "sensitive": True, "provisional": True, "multi": False, "versions": None, "applies_when": None},
    ]
    assert "categories" not in json.dumps(summary)
    raw_without = _raw_config()
    del raw_without["prompt_name"]
    assert cli.summarise_config(raw_without)["prompt_name"] == "(none)"
    assert cli.summarise_config("not a dict") == {"prompt_name": "(none)", "fields": []}
    assert cli.summarise_config({"fields": [1, {"name": "x"}]})["fields"] == [
        {"name": "x", "kind": None, "sensitive": False, "provisional": False, "multi": False, "versions": None, "applies_when": None}]


def _outcome(config, answer):
    run = fx.extract(
        [{"sequence": 1, "parent_id": None, "question_key": "Q", "message": "q", "translated_message": None},
         {"sequence": 2, "parent_id": "p", "question_key": "Q", "message": "a", "translated_message": None}],
        config, "S", "U", lambda prompt: (answer, {"prompt_tokens": 1, "completion_tokens": 1}))
    return SimpleNamespace(run=run, field_kinds={f.name: f.kind for f in config.fields})


def test_the_run_summary_prints_counts_for_free_text_and_min_max_for_integers_never_values():
    config = _config()
    agg = cli._new_aggregates("Youth_Voice", True)
    values = {}
    for age, district in ((17, "Zzdistrict-A"), ("23", "Zzdistrict-B"), (17, "Zzdistrict-A"), (150, "Zzdistrict-C")):
        outcome = _outcome(config, _answer(age=age, district=district, gender="Female"))
        cli._add_outcome(agg, outcome, 15, values)
    done = cli._finish(agg, 15, values)
    text = json.dumps(done)
    assert done["fields"]["age"]["status"] == {"extracted": 3, "unmapped": 1}
    assert (done["fields"]["age"]["distinct_values"], done["fields"]["age"]["min_value"], done["fields"]["age"]["max_value"]) == (2, 17, 23)
    assert "top_values" not in done["fields"]["age"]
    assert done["fields"]["district"]["distinct_values"] == 3 and "top_values" not in done["fields"]["district"]
    assert "Zzdistrict" not in text
    assert done["fields"]["gender"]["top_values"] == {"Female": 4}
    assert "150" not in text                                 # an unmapped (out of range) value is never printed


def test_the_run_summary_has_no_min_max_for_an_integer_field_nobody_answered():
    config = _config()
    agg = cli._new_aggregates("Youth_Voice", True)
    values = {}
    cli._add_outcome(agg, _outcome(config, _answer()), 15, values)
    done = cli._finish(agg, 15, values)
    assert done["fields"]["age"]["status"] == {"not_provided": 1} and "min_value" not in done["fields"]["age"]
    assert done["fields"]["age"]["distinct_values"] == 0


def test_neither_the_core_nor_the_cli_offers_a_preloaded_prompt_any_more():
    assert not hasattr(ops, "load_active_prompt") and not hasattr(cli, "load_active_prompt")
    assert "prompt_row" not in inspect.signature(ops.extract_submission).parameters


def test_a_failing_llm_call_surfaces_a_clean_error_and_chains_nothing(monkeypatch):
    """
    The provider's error text can carry respondent text. extract_submission replaces it by a sanitized error that
    is raised OUTSIDE the except block: no __cause__, no implicit __context__, so a traceback never prints the
    original message under "During handling of the above exception".
    """
    conn = _prompt_world()
    config = _config(prompt_name="Youth Prompt")
    rows = [{"sequence": 1, "parent_id": None, "question_key": "Q", "message": "How old are you?", "translated_message": None},
            {"sequence": 2, "parent_id": "p", "question_key": "Q", "message": "17", "translated_message": None}]

    async def fake_inputs(c, submission_id, tenant_code):
        return {"session_type": "Youth_Voice", "questionnaire_version": "V2", "rows": rows,
                "fingerprint": ops.rows_fingerprint([dict(r) for r in rows]), "loaded_at": None,
                "config_row": {"id": str(uuid.uuid4()), "config": config}, "closing_keys": []}

    async def fake_save(c, **kwargs):
        raise AssertionError("nothing is saved when the call failed")

    monkeypatch.setattr(ops, "load_submission_inputs", fake_inputs)
    monkeypatch.setattr(ops, "save_extraction", fake_save)

    def llm(prompt, model=None, max_tokens=None, timeout=None):
        raise RuntimeError("OpenRouter request failed with HTTP 500: echo 5550100123 respondent text")

    with pytest.raises(ops.LlmCallError) as caught:
        asyncio.run(ops.extract_submission(FakeEnginePool(conn), "s1", "mitra", llm_function=llm))
    error = caught.value
    assert str(error) == "LLM call failed (RuntimeError, HTTP 500)"
    assert error.__cause__ is None and error.__context__ is None
    assert "5550100123" not in "".join(traceback.format_exception(type(error), error, error.__traceback__))
