"""Auto-generated version info. Updated by pre-commit hook."""
__version__ = "1.5.84"


def git_hash() -> str:
    """Get current git short hash at runtime.

    Tries git CLI first (local dev), then falls back to .git_hash file
    (Docker container where git is not available).
    """
    import os

    # Try git CLI
    try:
        import subprocess
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except Exception:
        pass

    # Fallback: .git_hash file written during Docker build
    hash_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".git_hash")
    try:
        with open(hash_file) as f:
            h = f.read().strip()
            if h and h != "unknown":
                return h
    except Exception:
        pass

    return "unknown"
