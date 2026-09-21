"""CLI output helpers.

Regression guard for a NameError the lab agent hit on real hardware:
cmd_lab called _format_summary() which was never defined, so every natural
suite completion (and every --keep-alive Ctrl+C) crashed while printing its
summary. Local end-to-end runs missed it because the worker was always
killed rather than allowed to finish.
"""

from orchestration.__main__ import _format_summary, build_parser


def test_format_summary_renders_a_completed_suite():
    text = _format_summary({
        "suite_id": "suite-1", "passed": 3, "failed": 1, "errored": 0, "skipped": 1,
        "runs": [
            {"scenario": "connectivity", "state": "pass", "run_id": "r1", "result": {}},
            {"scenario": "world_state", "state": "fail", "run_id": "r2",
             "result": {"assertions": [{"name": "tick_strictly_increasing",
                                        "passed": False}]}},
        ],
    })
    assert "passed=3 failed=1" in text
    assert "connectivity" in text and "PASS" in text
    assert "tick_strictly_increasing" in text, "failed assertions must be named"


def test_format_summary_handles_an_aborted_suite():
    text = _format_summary({"suite_id": "s", "aborted": "data_server_failed"})
    assert "aborted: data_server_failed" in text


def test_format_summary_handles_an_empty_suite():
    assert "passed=0" in _format_summary({"suite_id": "s", "runs": []})


def test_lab_parser_exposes_the_phase5_flags():
    args = build_parser().parse_args(["lab", "--auto-sync", "--max-retries", "3"])
    assert args.auto_sync is True and args.max_retries == 3


def test_auto_sync_is_off_unless_asked_for():
    assert build_parser().parse_args(["lab"]).auto_sync is False


def test_mail_send_and_read_parse():
    sent = build_parser().parse_args(["mail", "send", "--to", "lab", "--text", "hi"])
    assert (sent.action, sent.to, sent.text) == ("send", "lab", "hi")
    read = build_parser().parse_args(["mail", "read", "--to", "client", "--watch"])
    assert read.watch is True
