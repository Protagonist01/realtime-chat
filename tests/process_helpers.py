"""Process helpers that never inherit account credentials."""

import os
import subprocess
import sys


def child_environment(**values):
    allowed = (
        "PATH",
        "SystemRoot",
        "WINDIR",
        "COMSPEC",
        "TEMP",
        "TMP",
        "APPDATA",
        "LOCALAPPDATA",
        "USERPROFILE",
        "HOME",
    )
    env = {key: os.environ[key] for key in allowed if key in os.environ}
    env.update(
        ENVIRONMENT="test",
        JWT_SECRET="test-secret-with-at-least-32-characters",
        AUTO_CREATE_DB="false",
        PYTHONUNBUFFERED="1",
    )
    env.update(values)
    return env


def terminate_tree(process):
    if process.poll() is not None:
        return
    if sys.platform == "win32":
        subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    else:
        process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)
