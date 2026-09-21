"""Safety rules for the automated code-sync path, against real git repos.

Phase 5 lets one machine pick up code another machine pushed without a human
in the loop, so these guardrails are the load-bearing part: fast-forward
only, never clobber local work, never touch diverged history.
"""

import subprocess

import pytest

from orchestration import gitsync


def _git(repo, *args):
    subprocess.run(["git", "-C", str(repo), *args], check=True,
                   capture_output=True, text=True)


def _commit(repo, name, content="x"):
    (repo / name).write_text(content)
    _git(repo, "add", name)
    _git(repo, "commit", "-m", f"add {name}")


@pytest.fixture
def repos(tmp_path):
    """An 'upstream' bare remote, plus a clone standing in for a machine."""
    upstream = tmp_path / "upstream.git"
    upstream.mkdir()
    _git(upstream, "init", "--bare", "-b", "main")

    seed = tmp_path / "seed"
    seed.mkdir()
    _git(seed, "init", "-b", "main")
    _git(seed, "config", "user.email", "t@example.com")
    _git(seed, "config", "user.name", "t")
    _commit(seed, "base.txt")
    _git(seed, "remote", "add", "origin", str(upstream))
    _git(seed, "push", "-u", "origin", "main")

    clone = tmp_path / "clone"
    subprocess.run(["git", "clone", str(upstream), str(clone)], check=True,
                   capture_output=True)
    _git(clone, "config", "user.email", "c@example.com")
    _git(clone, "config", "user.name", "c")
    return seed, clone


def test_no_op_when_already_up_to_date(repos):
    _seed, clone = repos
    report = gitsync.sync_to(str(clone))
    assert report["changed"] is False
    assert report["detail"] == "already up to date"


def test_fast_forwards_to_new_upstream_commits(repos):
    seed, clone = repos
    _commit(seed, "feature.txt")
    _git(seed, "push", "origin", "main")

    before = gitsync.head_sha(str(clone))
    report = gitsync.sync_to(str(clone))
    assert report["changed"] is True
    assert report["before"] == before
    assert report["after"] == gitsync.head_sha(str(clone))
    assert len(report["commits"]) == 1
    assert (clone / "feature.txt").exists()


def test_refuses_to_sync_over_uncommitted_work(repos):
    seed, clone = repos
    _commit(seed, "feature.txt")
    _git(seed, "push", "origin", "main")

    (clone / "local-work.txt").write_text("precious unsaved work")
    _git(clone, "add", "local-work.txt")
    before = gitsync.head_sha(str(clone))

    with pytest.raises(gitsync.GitError) as exc:
        gitsync.sync_to(str(clone))
    assert "uncommitted changes" in str(exc.value)
    # nothing moved, nothing lost
    assert gitsync.head_sha(str(clone)) == before
    assert (clone / "local-work.txt").read_text() == "precious unsaved work"


def test_refuses_to_sync_diverged_history(repos):
    seed, clone = repos
    _commit(seed, "upstream-change.txt")
    _git(seed, "push", "origin", "main")
    # the clone commits its own different work on top of the old base
    _commit(clone, "local-change.txt")
    before = gitsync.head_sha(str(clone))

    with pytest.raises(gitsync.GitError) as exc:
        gitsync.sync_to(str(clone))
    message = str(exc.value)
    assert "not a fast-forward" in message
    assert "will not merge, rebase or reset" in message
    assert gitsync.head_sha(str(clone)) == before


def test_unknown_target_is_rejected_without_changing_anything(repos):
    _seed, clone = repos
    before = gitsync.head_sha(str(clone))
    with pytest.raises(gitsync.GitError) as exc:
        gitsync.sync_to(str(clone), target="deadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
    assert "cannot resolve" in str(exc.value)
    assert gitsync.head_sha(str(clone)) == before


def test_can_sync_to_an_explicit_commit(repos):
    seed, clone = repos
    _commit(seed, "one.txt")
    first = gitsync.head_sha(str(seed))
    _commit(seed, "two.txt")
    _git(seed, "push", "origin", "main")

    gitsync.fetch(str(clone))
    report = gitsync.sync_to(str(clone), target=first)
    assert report["after"] == first
    assert (clone / "one.txt").exists()
    assert not (clone / "two.txt").exists(), "must stop at the requested commit"


def test_remote_is_ahead_is_read_only(repos):
    seed, clone = repos
    _commit(seed, "feature.txt")
    _git(seed, "push", "origin", "main")

    before = gitsync.head_sha(str(clone))
    status = gitsync.remote_is_ahead(str(clone))
    assert status["ahead"] is True
    assert len(status["commits"]) == 1
    assert gitsync.head_sha(str(clone)) == before, "checking must not move HEAD"


def test_remote_is_ahead_reports_divergence_instead_of_claiming_ahead(repos):
    seed, clone = repos
    _commit(seed, "upstream.txt")
    _git(seed, "push", "origin", "main")
    _commit(clone, "local.txt")

    status = gitsync.remote_is_ahead(str(clone))
    assert status["ahead"] is False
    assert "diverged" in status["reason"]


def test_detached_head_is_refused(repos):
    _seed, clone = repos
    _git(clone, "checkout", "--detach", "HEAD")
    with pytest.raises(gitsync.GitError) as exc:
        gitsync.sync_to(str(clone))
    assert "detached HEAD" in str(exc.value)
