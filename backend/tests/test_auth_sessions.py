import unittest
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import HTTPException

import app.routes.auth as auth


class _FakeQuery:
    def __init__(self, result):
        self._result = result

    def filter(self, *args, **kwargs):
        return self

    def first(self):
        return self._result

    def all(self):
        return self._result


class _FakeDB:
    def __init__(self, profile, roles):
        self.profile = profile
        self.roles = roles
        self.commit_calls = 0

    def query(self, model):
        if model is auth.Profile:
            return _FakeQuery(self.profile)
        if model is auth.UserRoleModel:
            return _FakeQuery(self.roles)
        raise AssertionError(f"Unexpected model query: {model}")

    def commit(self):
        self.commit_calls += 1


def _request(ip: str = "127.0.0.1"):
    return SimpleNamespace(headers={}, client=SimpleNamespace(host=ip))


def _profile():
    return SimpleNamespace(
        id="user-1",
        email="ann@example.com",
        hashed_password="hashed-password",
        first_name="Ann",
        last_name="Player",
        profile_setup_complete=True,
        token_version=4,
    )


def _roles():
    return [SimpleNamespace(role=SimpleNamespace(value="player"))]


class AuthSessionPolicyTests(unittest.TestCase):
    def test_login_terminates_all_sessions_when_concurrent_login_is_detected(self):
        profile = _profile()
        db = _FakeDB(profile, _roles())

        with (
            patch.object(auth, "verify_password", return_value=True),
            patch.object(auth, "_has_active_session", return_value=True),
            patch.object(auth, "_publish_kick") as publish_kick,
            patch.object(auth, "_clear_session") as clear_session,
            patch.object(auth, "_mark_session_active") as mark_session_active,
            patch.object(auth, "_log_audit") as log_audit,
        ):
            with self.assertRaises(HTTPException) as ctx:
                auth.login(auth.LoginRequest(email=profile.email, password="secret"), _request(), db)

        self.assertEqual(ctx.exception.status_code, 409)
        self.assertIn("all sessions", str(ctx.exception.detail).lower())
        self.assertEqual(profile.token_version, 5)
        self.assertEqual(db.commit_calls, 1)
        publish_kick.assert_called_once_with("user-1")
        clear_session.assert_called_once_with("user-1")
        mark_session_active.assert_not_called()
        log_audit.assert_called_once()
        self.assertEqual(log_audit.call_args.args[2], "ALL_SESSIONS_TERMINATED")

    def test_login_succeeds_when_no_other_session_is_active(self):
        profile = _profile()
        db = _FakeDB(profile, _roles())

        with (
            patch.object(auth, "verify_password", return_value=True),
            patch.object(auth, "_has_active_session", return_value=False),
            patch.object(auth, "_mark_session_active") as mark_session_active,
            patch.object(auth, "_log_audit") as log_audit,
            patch.object(auth, "create_access_token", return_value="signed-token"),
        ):
            response = auth.login(auth.LoginRequest(email=profile.email, password="secret"), _request(), db)

        self.assertEqual(response.access_token, "signed-token")
        self.assertEqual(response.user_id, "user-1")
        self.assertEqual(response.roles, ["player"])
        self.assertEqual(profile.token_version, 5)
        self.assertEqual(db.commit_calls, 1)
        mark_session_active.assert_called_once_with("user-1")
        log_audit.assert_called_once()
        self.assertEqual(log_audit.call_args.args[2], "LOGIN")


if __name__ == "__main__":
    unittest.main()
