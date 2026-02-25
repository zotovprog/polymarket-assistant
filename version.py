"""Auto-generated version info. Updated by pre-commit hook."""
__version__ = "1.5.2"


def git_hash() -> str:
    """Get current git short hash at runtime."""
    try:
        import subprocess
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except Exception:
        return "unknown"
