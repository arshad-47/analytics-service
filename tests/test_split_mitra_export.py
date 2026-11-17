"""Tests for tools/split_mitra_export.py. Synthetic, invented data only."""
import csv
import json
import logging
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tools"))

import split_mitra_export as sme  # noqa: E402

HEADER = [
    "bot_id", "bot_name", "session_type", "session_status", "session",
    "submission_id", "language", "sequence", "conversation_row_id", "message",
    "translated_message", "created_at", "updated_at", "stage", "receiver_id",
    "sender_id", "audio_file", "file_url", "other_params",
]


def make_row(stype, sess, seq, message="hello", blank=False):
    row = {c: "" for c in HEADER}
    row.update(
        bot_id="b1", bot_name="TestBot", session_type=stype, session_status="done",
        session=sess, submission_id="sub-" + sess, language="en", sequence=str(seq),
        conversation_row_id=f"{sess}-{seq}", message=message,
    )
    if not blank:
        row.update(created_at="2025-01-01 10:00:00", stage="s1", sender_id="u1")
    return [row[c] for c in HEADER]


def write_csv(path, rows, header=HEADER):
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)
    return path


def read_csv(path):
    with open(path, "r", encoding="utf-8", newline="") as fh:
        rows = list(csv.reader(fh))
    return rows[0], rows[1:]


def basic_rows():
    rows = []
    for stype, sessions in (("Youth_Voice", ["y1", "y2"]), ("Parent_Voice", ["p1"])):
        for s in sessions:
            rows.append(make_row(stype, s, 1, message="", blank=True))
            rows.append(make_row(stype, s, 2, message="first reply"))
            rows.append(make_row(stype, s, 3, message="second reply"))
    return rows


BIG = 10**9


def test_one_file_per_session_type(tmp_path):
    src = write_csv(tmp_path / "in.csv", basic_rows())
    out = tmp_path / "out"
    summary = sme.split_export(src, out, BIG)
    names = sorted(p.name for p in out.iterdir())
    assert names == ["Parent_Voice.csv", "Youth_Voice.csv", "split_summary.json"]
    _, youth = read_csv(out / "Youth_Voice.csv")
    _, parent = read_csv(out / "Parent_Voice.csv")
    assert len(youth) == 6 and len(parent) == 3
    assert {r[2] for r in youth} == {"Youth_Voice"}
    assert summary["input_rows"] == 9 and summary["input_sessions"] == 3


def test_prefix(tmp_path):
    src = write_csv(tmp_path / "in.csv", basic_rows())
    out = tmp_path / "out"
    sme.split_export(src, out, BIG, prefix="mitra_")
    assert (out / "mitra_Youth_Voice.csv").exists()
    assert (out / "mitra_Parent_Voice.csv").exists()


def test_split_into_parts_without_spreading_sessions(tmp_path):
    rows = []
    for i in range(6):
        sid = f"s{i}"
        rows += [make_row("Youth_Voice", sid, n, message=f"m{n}") for n in (1, 2, 3)]
    src = write_csv(tmp_path / "in.csv", rows)
    out = tmp_path / "out"
    one_session = sum(len(sme._encode_row(r)) for r in rows[:3])
    header_bytes = len(sme._encode_row(HEADER))
    limit = header_bytes + 2 * one_session + 5  # two sessions per part
    summary = sme.split_export(src, out, limit)
    files = sorted(p.name for p in out.glob("Youth_Voice_part*.csv"))
    assert files == ["Youth_Voice_part1.csv", "Youth_Voice_part2.csv", "Youth_Voice_part3.csv"]
    assert not (out / "Youth_Voice.csv").exists()
    seen = {}
    for f in files:
        _, data = read_csv(out / f)
        assert (out / f).stat().st_size <= limit
        for r in data:
            seen.setdefault(r[4], set()).add(f)
    assert all(len(v) == 1 for v in seen.values())
    assert len(seen) == 6
    assert [f["sessions"] for f in summary["files"]] == [2, 2, 2]


def test_non_contiguous_sessions_stay_together_in_file_order(tmp_path):
    rows = [
        make_row("Youth_Voice", "A", 1, message="a1"),
        make_row("Youth_Voice", "B", 1, message="b1"),
        make_row("Youth_Voice", "A", 2, message="a2"),
        make_row("Youth_Voice", "C", 1, message="c1"),
        make_row("Youth_Voice", "B", 2, message="b2"),
    ]
    src = write_csv(tmp_path / "in.csv", rows)
    out = tmp_path / "out"
    # Part fits A+B (4 rows) but not C as well.
    sizes = [len(sme._encode_row(r)) for r in rows]
    header_bytes = len(sme._encode_row(HEADER))
    limit = header_bytes + sizes[0] + sizes[2] + sizes[1] + sizes[4] + 1
    sme.split_export(src, out, limit)
    _, p1 = read_csv(out / "Youth_Voice_part1.csv")
    _, p2 = read_csv(out / "Youth_Voice_part2.csv")
    assert [r[9] for r in p1] == ["a1", "b1", "a2", "b2"]
    assert [r[9] for r in p2] == ["c1"]


def test_header_and_cells_byte_identical(tmp_path):
    tricky = 'line one,\nline "two" with, comma\r\nend'
    rows = [
        make_row("Parent_Voice", "p1", 1, message="", blank=True),
        make_row("Parent_Voice", "p1", 2, message=tricky),
        make_row("Parent_Voice", "p1", 3, message="  padded  "),
    ]
    rows[2][10] = "  Unicode नमस्ते  "
    src = write_csv(tmp_path / "in.csv", rows)
    out = tmp_path / "out"
    sme.split_export(src, out, BIG)
    header, data = read_csv(out / "Parent_Voice.csv")
    assert header == HEADER
    assert data == rows
    assert data[0][7] == "1" and data[0][15] == "" and data[0][13] == "" and data[0][11] == ""
    assert data[1][9] == tricky
    # whole file identical to the input when nothing is split
    assert (out / "Parent_Voice.csv").read_bytes() == src.read_bytes()


def test_oversize_session_gets_own_part_and_warns(tmp_path, caplog):
    rows = []
    rows += [make_row("Educator_Voice", "small1", n) for n in (1, 2)]
    rows += [make_row("Educator_Voice", "huge", n, message="x" * 200) for n in range(1, 6)]
    rows += [make_row("Educator_Voice", "small2", n) for n in (1, 2)]
    src = write_csv(tmp_path / "in.csv", rows)
    out = tmp_path / "out"
    small = sum(len(sme._encode_row(r)) for r in rows[:2])
    limit = len(sme._encode_row(HEADER)) + small + 10
    with caplog.at_level(logging.WARNING, logger="split_mitra_export"):
        summary = sme.split_export(src, out, limit)
    assert any("exceed" in r.getMessage() for r in caplog.records)
    assert not any("huge" in r.getMessage() for r in caplog.records)
    by_sessions = [(f["name"], f["sessions"], f["rows"]) for f in summary["files"]]
    assert by_sessions == [
        ("Educator_Voice_part1.csv", 1, 2),
        ("Educator_Voice_part2.csv", 1, 5),
        ("Educator_Voice_part3.csv", 1, 2),
    ]
    _, mid = read_csv(out / "Educator_Voice_part2.csv")
    assert {r[4] for r in mid} == {"huge"}


def _assert_nothing_written(out):
    assert not out.exists() or list(out.iterdir()) == []


def test_error_missing_columns(tmp_path):
    out = tmp_path / "out"
    for drop in ("session_type", "session"):
        header = [c for c in HEADER if c != drop]
        src = write_csv(tmp_path / f"no_{drop}.csv", [], header=header)
        with pytest.raises(sme.SplitError, match=drop):
            sme.split_export(src, out, BIG)
    _assert_nothing_written(out)


@pytest.mark.parametrize("col_idx,label", [(2, "session_type"), (4, "session")])
def test_error_blank_values(tmp_path, col_idx, label):
    rows = basic_rows()
    rows[4][col_idx] = ""
    src = write_csv(tmp_path / "in.csv", rows)
    out = tmp_path / "out"
    with pytest.raises(sme.SplitError, match=f"data row 5.*blank {label}"):
        sme.split_export(src, out, BIG)
    _assert_nothing_written(out)


def test_error_mixed_session_type_in_one_session(tmp_path):
    rows = [
        make_row("Youth_Voice", "m1", 1),
        make_row("Parent_Voice", "m1", 2),
    ]
    src = write_csv(tmp_path / "in.csv", rows)
    out = tmp_path / "out"
    with pytest.raises(sme.SplitError, match="more than one distinct session_type") as ei:
        sme.split_export(src, out, BIG)
    assert "m1" not in str(ei.value)
    _assert_nothing_written(out)


def test_error_wrong_field_count_names_row(tmp_path):
    rows = basic_rows()
    rows[2] = rows[2][:-1]
    src = write_csv(tmp_path / "in.csv", rows)
    out = tmp_path / "out"
    with pytest.raises(sme.SplitError, match=r"data row 3: expected 19 fields, got 18"):
        sme.split_export(src, out, BIG)
    _assert_nothing_written(out)


def test_summary_has_only_aggregates(tmp_path):
    rows = basic_rows()
    rows.append(make_row("Youth_Voice", "y3", 1, message="secret message text"))
    src = write_csv(tmp_path / "in.csv", rows)
    out = tmp_path / "out"
    sme.split_export(src, out, BIG)
    text = (out / "split_summary.json").read_text(encoding="utf-8")
    data = json.loads(text)
    for forbidden in ("secret message text", "y1", "y2", "y3", "p1", "sub-", "first reply"):
        assert forbidden not in text
    assert set(data) == {"input_rows", "input_sessions", "max_bytes", "files", "totals"}
    assert data["input_rows"] == 10 and data["input_sessions"] == 4
    assert data["totals"]["sessions"] == 4 and data["totals"]["rows"] == 10
    for f in data["files"]:
        assert set(f) == {"name", "session_type", "sessions", "rows", "bytes"}
        assert f["bytes"] == (out / f["name"]).stat().st_size
    assert data["totals"]["bytes"] == sum(f["bytes"] for f in data["files"])


def test_refuses_overwrite_then_force(tmp_path):
    src = write_csv(tmp_path / "in.csv", basic_rows())
    out = tmp_path / "out"
    sme.split_export(src, out, BIG)
    sentinel = out / "Youth_Voice.csv"
    before = sentinel.read_bytes()
    sentinel.write_text("changed")
    with pytest.raises(sme.SplitError, match="refusing to overwrite"):
        sme.split_export(src, out, BIG)
    assert sentinel.read_text() == "changed"
    sme.split_export(src, out, BIG, force=True)
    assert sentinel.read_bytes() == before
    assert not list(out.glob("*.tmp"))


def test_main_exit_codes(tmp_path, capsys):
    src = write_csv(tmp_path / "in.csv", basic_rows())
    out = tmp_path / "out"
    assert sme.main([str(src), "--out-dir", str(out), "--max-mb", "80"]) == 0
    table = capsys.readouterr().out
    assert "Youth_Voice.csv" in table and "TOTAL" in table
    assert "first reply" not in table and "y1" not in table
    # second run refuses
    assert sme.main([str(src), "--out-dir", str(out)]) == 1
    assert "refusing to overwrite" in capsys.readouterr().err
    assert sme.main([str(src), "--out-dir", str(out), "--force"]) == 0
    # validation error
    bad = write_csv(tmp_path / "bad.csv", [make_row("", "x", 1)])
    assert sme.main([str(bad), "--out-dir", str(tmp_path / "out2")]) == 1
    assert not (tmp_path / "out2").exists()
    # missing input
    assert sme.main([str(tmp_path / "nope.csv"), "--out-dir", str(tmp_path / "out3")]) == 1


def test_cli_subprocess(tmp_path):
    src = write_csv(tmp_path / "in.csv", basic_rows())
    out = tmp_path / "nested" / "out"
    script = ROOT / "tools" / "split_mitra_export.py"
    ok = subprocess.run(
        [sys.executable, str(script), str(src), "--out-dir", str(out), "--max-mb", "1"],
        capture_output=True, text=True,
    )
    assert ok.returncode == 0, ok.stderr
    assert (out / "Youth_Voice.csv").exists()
    again = subprocess.run(
        [sys.executable, str(script), str(src), "--out-dir", str(out)],
        capture_output=True, text=True,
    )
    assert again.returncode != 0
    assert "--force" in again.stderr


# ---------------------------------------------------------------------------------------------------------------
# Review findings B14 / C10 / A21
# ---------------------------------------------------------------------------------------------------------------

def _tree(root):
    return sorted(str(p.relative_to(root)) for p in Path(root).rglob("*"))


@pytest.mark.parametrize("bad", ["../ESCAPED", "../../x", "a/b", "a\\b", "x y", "", "a" * 65, "Youth\tVoice", "Youth\rVoice"])
def test_a_session_type_that_is_not_a_file_name_part_is_refused_and_nothing_is_written(tmp_path, bad):
    outer = tmp_path / "outer"
    outer.mkdir()
    rows = [make_row("Youth_Voice", "y1", 2), make_row(bad if bad != "" else "ok", "y2", 2)]
    if bad == "":
        rows[1] = make_row("   ", "y2", 2)
    src = write_csv(outer / "in.csv", rows)
    out = outer / "out_dir"
    before = _tree(outer)
    with pytest.raises(sme.SplitError) as raised:
        sme.split_export(src, out, BIG)
    assert bad not in str(raised.value) or bad == ""        # the value is never echoed
    assert _tree(outer) == before                           # no out dir, nothing escaped, nothing written


def test_the_old_traversal_reproduction_stays_inside_out_dir(tmp_path):
    outer = tmp_path / "outer"
    outer.mkdir()
    src = write_csv(outer / "in.csv", [make_row("../ESCAPED", "e1", 2)])
    with pytest.raises(sme.SplitError, match="file name"):
        sme.split_export(src, outer / "out", BIG)
    assert not (outer / "ESCAPED.csv").exists() and not (tmp_path / "ESCAPED.csv").exists()


def test_a_prefix_cannot_escape_either(tmp_path):
    src = write_csv(tmp_path / "in.csv", basic_rows())
    for prefix in ("../up_", "a/b_", "x y"):
        with pytest.raises(sme.SplitError, match="--prefix"):
            sme.split_export(src, tmp_path / "out", BIG, prefix=prefix)
    assert not (tmp_path / "out").exists()


def test_files_are_private(tmp_path):
    import os
    import stat
    src = write_csv(tmp_path / "in.csv", basic_rows())
    out = tmp_path / "out"
    sme.split_export(src, out, BIG)
    for f in out.iterdir():
        assert stat.S_IMODE(os.stat(f).st_mode) == 0o600, f.name
    assert stat.S_IMODE(os.stat(out).st_mode) & 0o077 == 0     # created 0700 (under the usual umask)


def test_files_stay_private_even_under_a_permissive_umask(tmp_path):
    import os
    import stat
    src = write_csv(tmp_path / "in.csv", basic_rows())
    old = os.umask(0)
    try:
        sme.split_export(src, tmp_path / "out", BIG)
    finally:
        os.umask(old)
    assert all(stat.S_IMODE(os.stat(f).st_mode) == 0o600 for f in (tmp_path / "out").iterdir())


def test_a_trailing_blank_line_is_skipped(tmp_path):
    src = write_csv(tmp_path / "in.csv", basic_rows())
    with open(src, "a", encoding="utf-8", newline="") as fh:
        fh.write("\n\n")
    summary = sme.split_export(src, tmp_path / "out", BIG)
    assert summary["input_rows"] == 9


def test_a_blank_line_in_the_middle_is_skipped_too(tmp_path):
    rows = basic_rows()
    src = tmp_path / "in.csv"
    write_csv(src, rows[:4])
    with open(src, "a", encoding="utf-8", newline="") as fh:
        fh.write("\n")
        csv.writer(fh, lineterminator="\n").writerows(rows[4:])
    assert sme.split_export(src, tmp_path / "out", BIG)["input_rows"] == 9


def test_headers_match_case_insensitively_and_a_bom_is_dropped(tmp_path):
    header = ["\ufeff Session_Type "] + HEADER[1:2] + ["SESSION"] + HEADER[3:4] + HEADER[5:]
    rows = []
    for r in basic_rows():
        rows.append([r[2], r[1], r[4], r[3]] + r[5:])
    src = tmp_path / "in.csv"
    with open(src, "w", encoding="utf-8", newline="") as fh:     # the BOM is in the file as UTF-8 bytes
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)
    summary = sme.split_export(src, tmp_path / "out", BIG)
    assert sorted(f["name"] for f in summary["files"]) == ["Parent_Voice.csv", "Youth_Voice.csv"]
    first_line = (tmp_path / "out" / "Youth_Voice.csv").read_text(encoding="utf-8").splitlines()[0]
    assert not first_line.startswith("\ufeff")    # the header row is written as read: no BOM character is kept


def test_a_bom_only_file_with_the_default_names_works(tmp_path):
    src = tmp_path / "in.csv"
    with open(src, "w", encoding="utf-8-sig", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(HEADER)
        w.writerows(basic_rows())
    assert sme.split_export(src, tmp_path / "out", BIG)["input_rows"] == 9


def test_session_ids_are_compared_stripped_like_the_uploader(tmp_path):
    rows = [make_row("Youth_Voice", "s1", 2), make_row("Youth_Voice", " s1 ", 3), make_row("Youth_Voice", "s2", 2)]
    src = write_csv(tmp_path / "in.csv", rows)
    summary = sme.split_export(src, tmp_path / "out", BIG)
    assert summary["input_sessions"] == 2                    # 's1' and ' s1 ' are ONE session
    # ... and they can never be split over two parts
    one = sum(len(sme._encode_row(r)) for r in rows[:2])
    limit = len(sme._encode_row(HEADER)) + one + 1
    summary = sme.split_export(src, tmp_path / "out2", limit)
    parts = {}
    for f in summary["files"]:
        _, data = read_csv(tmp_path / "out2" / f["name"])
        for r in data:
            parts.setdefault(r[4].strip(), set()).add(f["name"])
    assert all(len(v) == 1 for v in parts.values())


def test_session_types_that_differ_only_in_case_are_refused(tmp_path):
    src = write_csv(tmp_path / "in.csv", [make_row("Youth_Voice", "a", 2), make_row("youth_voice", "b", 2)])
    with pytest.raises(sme.SplitError, match="only in case"):
        sme.split_export(src, tmp_path / "out", BIG)
    assert not (tmp_path / "out").exists()


def test_part_names_that_differ_only_in_case_are_refused(tmp_path):
    rows = []
    for i in range(2):
        rows.append(make_row("X", f"x{i}", 2))
    rows.append(make_row("x_part1", "z", 2))
    src = write_csv(tmp_path / "in.csv", rows)
    one = len(sme._encode_row(rows[0]))
    limit = len(sme._encode_row(HEADER)) + one + 1             # one session per part: X_part1, X_part2
    with pytest.raises(sme.SplitError, match="differ only in case"):
        sme.split_export(src, tmp_path / "out", limit)


def test_column_names_are_options_with_the_current_names_as_defaults(tmp_path):
    header = ["persona", "conv", "text"]
    rows = [["Parent", "c1", "hi"], ["Parent", "c1", "again"], ["Teacher", "c2", "yo"]]
    src = write_csv(tmp_path / "in.csv", rows, header=header)
    with pytest.raises(sme.SplitError, match="'session_type' is missing"):
        sme.split_export(src, tmp_path / "default_out", BIG)
    summary = sme.split_export(src, tmp_path / "out", BIG, type_col="PERSONA", session_col="conv")
    assert sorted(f["name"] for f in summary["files"]) == ["Parent.csv", "Teacher.csv"]
    assert summary["input_sessions"] == 2


def test_cli_accepts_type_col_and_session_col(tmp_path, capsys):
    src = write_csv(tmp_path / "in.csv", [["Parent", "c1", "hi"]], header=["persona", "conv", "text"])
    out = tmp_path / "out"
    assert sme.main([str(src), "--out-dir", str(out), "--type-col", "persona", "--session-col", "conv"]) == 0
    assert (out / "Parent.csv").exists()


def test_a_rerun_with_fewer_parts_warns_about_the_old_part_files(tmp_path, caplog):
    rows = []
    for i in range(4):
        rows += [make_row("Youth_Voice", f"s{i}", n) for n in (1, 2)]
    src = write_csv(tmp_path / "in.csv", rows)
    out = tmp_path / "out"
    one = sum(len(sme._encode_row(r)) for r in rows[:2])
    sme.split_export(src, out, len(sme._encode_row(HEADER)) + one + 1)             # 4 parts
    with caplog.at_level(logging.WARNING, logger="split_mitra_export"):
        sme.split_export(src, out, BIG, force=True)                                  # 1 file now
    assert any("older part file" in r.getMessage() for r in caplog.records)


def test_a_part_that_fills_the_limit_exactly_is_allowed_and_one_byte_less_splits_it(tmp_path):
    """The limit is inclusive: header + two sessions == max_bytes fits in one part; max_bytes - 1 needs two."""
    rows = []
    for i in range(2):
        rows += [make_row("Youth_Voice", f"s{i}", n, message=f"m{n}") for n in (1, 2, 3)]
    src = write_csv(tmp_path / "in.csv", rows)
    header_bytes = len(sme._encode_row(HEADER))
    exact = header_bytes + sum(len(sme._encode_row(r)) for r in rows)

    fits = sme.split_export(src, tmp_path / "fits", exact)
    assert [f["name"] for f in fits["files"]] == ["Youth_Voice.csv"]                  # one file: not split into parts
    assert (tmp_path / "fits" / "Youth_Voice.csv").stat().st_size == exact

    split = sme.split_export(src, tmp_path / "split", exact - 1)
    assert sorted(p.name for p in (tmp_path / "split").glob("Youth_Voice*.csv")) == [
        "Youth_Voice_part1.csv", "Youth_Voice_part2.csv"]
    assert [f["sessions"] for f in split["files"]] == [1, 1]


# ------------------------------------------------------------------------------------ CodeRabbit round 2 (PR 36)

def test_python_310_csv_writer_alone_does_not_quote_a_bare_carriage_return():
    """The reproduction of the finding: with lineterminator '\\n' the stdlib writer leaves 'a\\rb' unquoted and the
    reader then sees two records. Kept as a canary: the tool's own encoder must not depend on it."""
    import io
    buf = io.StringIO(newline="")
    csv.writer(buf, lineterminator="\n").writerow(["a\rb", "x"])
    if buf.getvalue() == "a\rb,x\n":                                  # Python 3.10: the writer is the culprit
        assert len(list(csv.reader(io.StringIO(buf.getvalue(), newline="")))) == 2
    assert sme._encode_text(["a\rb", "x"]) == '"a\rb",x\n'


def write_csv_quoted(path, rows, header=HEADER):
    """An input file whose cells with CR / LF are properly quoted (the '\\r\\n' terminator makes the stdlib writer
    quote both); write_csv would write a bare '\\r' unquoted and produce a malformed input."""
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh, lineterminator="\r\n")
        w.writerow(header)
        w.writerows(rows)
    return path


@pytest.mark.parametrize("message", ["a\rb", "a\nb", "a\r\nb", "\r", "\n", 'say "hi"\r', "plain"])
def test_cells_with_cr_or_lf_round_trip_through_the_split(tmp_path, message):
    rows = [make_row("Youth_Voice", "y1", 2, message=message), make_row("Youth_Voice", "y1", 3, message="next")]
    src = write_csv_quoted(tmp_path / "in.csv", rows)
    summary = sme.split_export(src, tmp_path / "out", BIG)
    _, out_rows = read_csv(tmp_path / "out" / "Youth_Voice.csv")
    assert out_rows == rows                                           # the same records, the same cells
    assert summary["totals"]["rows"] == 2 and summary["files"][0]["rows"] == len(out_rows)
    # the size accounting counted exactly what was written
    assert summary["files"][0]["bytes"] == len(sme._encode_row(HEADER)) + sum(len(sme._encode_row(r)) for r in rows)


def test_a_header_cell_with_a_carriage_return_is_quoted_and_counted_too(tmp_path):
    header = list(HEADER)
    header[1] = "bot\rname"
    rows = [make_row("Youth_Voice", "y1", 2)]
    src = write_csv_quoted(tmp_path / "in.csv", rows, header=header)
    summary = sme.split_export(src, tmp_path / "out", BIG)
    out_header, out_rows = read_csv(tmp_path / "out" / "Youth_Voice.csv")
    assert out_header == header and out_rows == rows
    assert summary["files"][0]["bytes"] == (tmp_path / "out" / "Youth_Voice.csv").stat().st_size


def test_ordinary_rows_are_encoded_as_before_one_newline_per_row():
    assert sme._encode_text(["a", "b,c", 'd"e', ""]) == 'a,"b,c","d""e",\n'
    assert sme._encode_text([""]) == '""\n'


def _failing_close_fdopen(monkeypatch, parts):
    """os.fdopen wrapper: the files opened for the rows (after the `parts` header files) fail on close() the way a
    full disk does when the buffer is flushed (after really closing the descriptor). Returns the list of closes."""
    real = sme.os.fdopen
    calls = {"n": 0}
    closes = []

    class Handle:
        def __init__(self, handle, fails):
            self._h, self._fails = handle, fails

        def __getattr__(self, name):
            return getattr(self._h, name)

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            self._h.close()

        def close(self):
            self._h.close()
            closes.append(self._fails)
            if self._fails:
                raise OSError(28, "No space left on device")

    def fdopen(fd, *args, **kwargs):
        calls["n"] += 1
        return Handle(real(fd, *args, **kwargs), calls["n"] > parts)

    monkeypatch.setattr(sme.os, "fdopen", fdopen)
    return closes


def test_a_failing_close_removes_every_temporary_file_and_publishes_nothing(tmp_path, monkeypatch):
    rows = [make_row("Youth_Voice", "y1", 2), make_row("Parent_Voice", "p1", 2), make_row("Educator_Voice", "e1", 2)]
    src = write_csv(tmp_path / "in.csv", rows)
    closes = _failing_close_fdopen(monkeypatch, parts=3)
    with pytest.raises(OSError, match="No space left"):
        sme.split_export(src, tmp_path / "out", BIG)
    assert closes.count(True) == 3                       # every writer was closed, not only up to the first failure
    assert sorted(p.name for p in (tmp_path / "out").iterdir()) == []      # no .tmp, no part, no summary


def test_a_failing_close_is_an_error_exit_not_a_traceback(tmp_path, monkeypatch, capsys):
    src = write_csv(tmp_path / "in.csv", [make_row("Youth_Voice", "y1", 2)])
    _failing_close_fdopen(monkeypatch, parts=1)
    assert sme.main([str(src), "--out-dir", str(tmp_path / "out")]) == 1
    assert "No space left" in capsys.readouterr().err
    assert list((tmp_path / "out").iterdir()) == []


def test_a_routing_error_removes_the_temporary_files_even_when_closing_fails_too(tmp_path, monkeypatch):
    rows = [make_row("Youth_Voice", "y1", 2), make_row("Youth_Voice", "y2", 2)]
    src = write_csv(tmp_path / "in.csv", rows)
    _failing_close_fdopen(monkeypatch, parts=1)
    real_writerow = sme._RowWriter.writerow
    seen = {"n": 0}

    def boom(self, row):
        seen["n"] += 1
        if seen["n"] == 3:                                # header, y1 row, then the second data row fails
            raise RuntimeError("routing failed")
        return real_writerow(self, row)

    monkeypatch.setattr(sme._RowWriter, "writerow", boom)
    with pytest.raises(RuntimeError, match="routing failed"):             # the cause, not the close error
        sme.split_export(src, tmp_path / "out", BIG)
    assert list((tmp_path / "out").iterdir()) == []


def test_the_number_of_open_output_files_is_bounded_and_the_output_is_the_same(tmp_path, monkeypatch):
    sessions = [f"s{i}" for i in range(12)]
    rows = []
    for n in (1, 2):                                      # interleaved: every part gets a row in each round
        rows += [make_row("Youth_Voice", s, n) for s in sessions]
    src = write_csv(tmp_path / "in.csv", rows)
    one = len(sme._encode_row(rows[0]))
    limit = len(sme._encode_row(HEADER)) + 2 * one        # one session per part: 12 parts

    real = sme.os.fdopen
    live, peak = set(), {"n": 0}

    class Tracked:
        def __init__(self, handle):
            self._h = handle
            live.add(id(self))
            peak["n"] = max(peak["n"], len(live))

        def __getattr__(self, name):
            return getattr(self._h, name)

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            self.close()

        def close(self):
            live.discard(id(self))
            self._h.close()

    monkeypatch.setattr(sme.os, "fdopen", lambda fd, *a, **k: Tracked(real(fd, *a, **k)))
    bounded = sme.split_export(src, tmp_path / "bounded", limit, max_open_files=3)
    assert len(bounded["files"]) == 12 and peak["n"] <= 3
    monkeypatch.undo()
    wide = sme.split_export(src, tmp_path / "wide", limit, max_open_files=64)
    for f in bounded["files"]:
        assert (tmp_path / "bounded" / f["name"]).read_bytes() == (tmp_path / "wide" / f["name"]).read_bytes()
        out_header, out_rows = read_csv(tmp_path / "bounded" / f["name"])
        assert out_header == HEADER and len(out_rows) == 2            # one header, rows in input order
    assert wide["totals"] == bounded["totals"]


def test_a_session_type_naming_the_part_file_of_another_is_refused_before_anything_is_written(tmp_path):
    """Youth_Voice needs two files (Youth_Voice_part1.csv, _part2.csv) and a session type is literally Youth_Voice_part1."""
    rows = [make_row("Youth_Voice", "y1", 2), make_row("Youth_Voice", "y2", 2), make_row("Youth_Voice_part1", "z", 2)]
    src = write_csv(tmp_path / "in.csv", rows)
    limit = len(sme._encode_row(HEADER)) + len(sme._encode_row(rows[0]))
    with pytest.raises(sme.SplitError, match="same file name"):
        sme.split_export(src, tmp_path / "out", limit)
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize("bad", ["../outside", "/abs/path", "a/b", "a\\b"])
def test_a_traversing_session_type_creates_nothing_outside_out_dir(tmp_path, bad):
    src = write_csv(tmp_path / "in.csv", [make_row(bad, "y1", 2)])
    with pytest.raises(sme.SplitError):
        sme.split_export(src, tmp_path / "work" / "out", BIG, force=True)
    assert sorted(p.name for p in tmp_path.rglob("*") if p.is_file()) == ["in.csv"]


def test_collision_detection_is_one_pass(tmp_path):
    """8,000 distinct names: with the old per-name scan of every name this took minutes (4 x 10^8 comparisons)."""
    import time
    n = 8000
    session_types = [f"T{i:05d}" for i in range(n)]
    rows = [make_row(t, f"s{i}", 2) for i, t in enumerate(session_types)]
    src = write_csv(tmp_path / "in.csv", rows)
    started = time.monotonic()
    sme.split_export(src, tmp_path / "out", BIG, max_open_files=16)
    assert time.monotonic() - started < 30
