"""Constrained git operations for the automated sync loop.

Phase 5 lets one machine pick up code the other pushed, without a human.
That is a remote-code-execution path by nature, so every operation here is
deliberately narrow:

  * fast-forward only - never merge, rebase, reset, checkout or force
  * the target must be a descendant of current HEAD on the tracked branch,
    so a sync can only move forward along history that already contains it
  * refuses to run with a dirty working tree, so local work is never
    clobbered (no stash, no discard - it stops and says so)
  * argv lists only, never a shell string
  * every call is time-bounded

What it deliberately cannot do: commit, push, change remotes, or check out
arbitrary refs. Authoring and publishing stay outside the automated loop.
"""

import subprocess

GIT_TIMEOUT = 60.0


class GitError(Exception):
    """A git command failed, or a safety precondition was not met."""


def _git(repo: str, *args, timeout: float = GIT_TIMEOUT) -> str:
    try:
        proc = subprocess.run(["git", "-C", repo, *args], capture_output=True,
                              text=True, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        raise GitError(f"git {' '.join(args)} timed out after {timeout}s") from exc
    except FileNotFoundError as exc:
        raise GitError("git executable not found") from exc
    if proc.returncode != 0:
        raise GitError(f"git {' '.join(args)} failed: "
                       f"{(proc.stderr or proc.stdout).strip()}")
    return proc.stdout.strip()


def head_sha(repo: str) -> str:
    return _git(repo, "rev-parse", "HEAD")


def current_branch(repo: str) -> str:
    return _git(repo, "rev-parse", "--abbrev-ref", "HEAD")


def is_dirty(repo: str) -> bool:
    return bool(_git(repo, "status", "--porcelain"))


def dirty_files(repo: str) -> list:
    return [line[3:] for line in _git(repo, "status", "--porcelain").splitlines()]


def fetch(repo: str, remote: str = "origin") -> str:
    return _git(repo, "fetch", remote, timeout=GIT_TIMEOUT)


def resolve(repo: str, ref: str) -> str:
    """Resolve a ref to a full SHA, erroring if it does not exist locally."""
    return _git(repo, "rev-parse", "--verify", f"{ref}^{{commit}}")


def is_ancestor(repo: str, maybe_ancestor: str, descendant: str) -> bool:
    try:
        _git(repo, "merge-base", "--is-ancestor", maybe_ancestor, descendant)
        return True
    except GitError:
        return False


def commits_between(repo: str, old: str, new: str) -> list:
    if old == new:
        return []
    out = _git(repo, "log", "--oneline", f"{old}..{new}")
    return out.splitlines() if out else []


def sync_to(repo: str, target: str = None, remote: str = "origin",
            branch: str = None, allow_dirty: bool = False) -> dict:
    """Fast-forward the repo to `target` (default: the tracked remote branch).

    Returns a report describing what moved. Raises GitError, without changing
    anything, if any safety precondition fails.
    """
    branch = branch or current_branch(repo)
    if branch == "HEAD":
        raise GitError("repo is in detached HEAD; refusing to sync")

    before = head_sha(repo)
    if is_dirty(repo) and not allow_dirty:
        raise GitError(
            "working tree has uncommitted changes, refusing to sync so nothing "
            f"is lost: {dirty_files(repo)[:10]}. Commit or stash them first.")

    fetch(repo, remote)
    target_ref = target or f"{remote}/{branch}"
    try:
        target_sha = resolve(repo, target_ref)
    except GitError as exc:
        raise GitError(f"cannot resolve {target_ref!r}: {exc}") from exc

    if target_sha == before:
        return {"changed": False, "before": before, "after": before,
                "branch": branch, "target": target_ref, "commits": [],
                "detail": "already up to date"}

    # Only ever move forward along history that already contains HEAD.
    if not is_ancestor(repo, before, target_sha):
        raise GitError(
            f"{target_ref} ({target_sha[:8]}) is not a fast-forward from HEAD "
            f"({before[:8]}) - histories diverged. A human should reconcile this; "
            f"the automated loop will not merge, rebase or reset.")

    commits = commits_between(repo, before, target_sha)
    _git(repo, "merge", "--ff-only", target_sha)
    after = head_sha(repo)
    if after != target_sha:
        raise GitError(f"post-sync HEAD {after[:8]} != requested {target_sha[:8]}")
    return {"changed": True, "before": before, "after": after, "branch": branch,
            "target": target_ref, "commits": commits,
            "detail": f"fast-forwarded {len(commits)} commit(s)"}


def remote_is_ahead(repo: str, remote: str = "origin", branch: str = None) -> dict:
    """Check, without changing anything, whether a sync would do something."""
    branch = branch or current_branch(repo)
    fetch(repo, remote)
    before = head_sha(repo)
    try:
        target = resolve(repo, f"{remote}/{branch}")
    except GitError as exc:
        return {"ahead": False, "reason": str(exc), "head": before}
    if target == before:
        return {"ahead": False, "reason": "up to date", "head": before,
                "remote_head": target}
    if not is_ancestor(repo, before, target):
        return {"ahead": False, "reason": "diverged - not a fast-forward",
                "head": before, "remote_head": target}
    return {"ahead": True, "head": before, "remote_head": target,
            "commits": commits_between(repo, before, target)}
