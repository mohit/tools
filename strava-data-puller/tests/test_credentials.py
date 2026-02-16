import importlib.util
import sys
import types
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch


def load_module():
    sys.modules.setdefault("duckdb", types.SimpleNamespace(connect=lambda: None))
    sys.modules.setdefault("requests", types.SimpleNamespace(get=None, post=None))
    module_path = Path(__file__).resolve().parents[1] / "strava_pull.py"
    spec = importlib.util.spec_from_file_location("strava_pull", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


strava_pull = load_module()


class TestCredentials(TestCase):
    def test_parse_dotenv_supports_export_and_quotes(self):
        env_file = Path(__file__).resolve().parent / "tmp_parse.env"
        env_file.write_text(
            "\n".join(
                [
                    "# comment",
                    "export STRAVA_CLIENT_ID=12345",
                    'STRAVA_CLIENT_SECRET="secret-value"',
                    "STRAVA_REFRESH_TOKEN='refresh-token'",
                    "IGNORED_LINE",
                ]
            ),
            encoding="utf-8",
        )
        try:
            values = strava_pull.parse_dotenv(env_file)
        finally:
            env_file.unlink(missing_ok=True)

        self.assertEqual(values["STRAVA_CLIENT_ID"], "12345")
        self.assertEqual(values["STRAVA_CLIENT_SECRET"], "secret-value")
        self.assertEqual(values["STRAVA_REFRESH_TOKEN"], "refresh-token")

    @patch.object(strava_pull, "discover_env_files")
    @patch.object(strava_pull, "load_keychain_secret")
    @patch.dict("os.environ", {"STRAVA_CLIENT_ID": "env-client-id"}, clear=True)
    def test_resolve_precedence(
        self, mock_keychain_secret, mock_discover_env_files
    ):
        env_path = Path(__file__).resolve().parent / "tmp_resolve.env"
        env_path.write_text(
            "\n".join(
                [
                    "STRAVA_CLIENT_ID=dotenv-client-id",
                    "STRAVA_CLIENT_SECRET=dotenv-client-secret",
                ]
            ),
            encoding="utf-8",
        )
        mock_discover_env_files.return_value = [env_path]
        mock_keychain_secret.side_effect = (
            lambda var_name: "keychain-refresh-token"
            if var_name == "STRAVA_REFRESH_TOKEN"
            else None
        )

        try:
            values, sources, _ = strava_pull.resolve_strava_credentials()
        finally:
            env_path.unlink(missing_ok=True)

        self.assertEqual(values["STRAVA_CLIENT_ID"], "env-client-id")
        self.assertEqual(values["STRAVA_CLIENT_SECRET"], "dotenv-client-secret")
        self.assertEqual(values["STRAVA_REFRESH_TOKEN"], "keychain-refresh-token")
        self.assertEqual(sources["STRAVA_CLIENT_ID"], "environment")
        self.assertEqual(sources["STRAVA_CLIENT_SECRET"], f"dotenv:{env_path}")
        self.assertEqual(sources["STRAVA_REFRESH_TOKEN"], "keychain")

    @patch.object(strava_pull, "discover_env_files")
    @patch.object(strava_pull, "load_keychain_secret")
    @patch.dict(
        "os.environ",
        {
            "STRAVA_CLIENT_ID": "env-client-id",
            "STRAVA_CLIENT_SECRET": "env-client-secret",
            "STRAVA_REFRESH_TOKEN": "env-refresh-token",
        },
        clear=True,
    )
    def test_resolve_skips_directory_candidates_when_env_is_complete(
        self, mock_keychain_secret, mock_discover_env_files
    ):
        env_dir = Path(__file__).resolve().parent / "tmp_env_dir"
        env_dir.mkdir(exist_ok=True)
        mock_discover_env_files.return_value = [env_dir]
        original_parse_dotenv = strava_pull.parse_dotenv
        strava_pull.parse_dotenv = lambda _path: self.fail(
            "parse_dotenv should not run when env credentials are complete"
        )

        try:
            values, sources, _ = strava_pull.resolve_strava_credentials()
        finally:
            strava_pull.parse_dotenv = original_parse_dotenv
            env_dir.rmdir()

        self.assertEqual(values["STRAVA_CLIENT_ID"], "env-client-id")
        self.assertEqual(values["STRAVA_CLIENT_SECRET"], "env-client-secret")
        self.assertEqual(values["STRAVA_REFRESH_TOKEN"], "env-refresh-token")
        self.assertEqual(sources["STRAVA_CLIENT_ID"], "environment")
        self.assertEqual(sources["STRAVA_CLIENT_SECRET"], "environment")
        self.assertEqual(sources["STRAVA_REFRESH_TOKEN"], "environment")
        mock_keychain_secret.assert_not_called()

    @patch.object(strava_pull, "discover_env_files")
    @patch.object(strava_pull, "load_keychain_secret")
    @patch.dict("os.environ", {}, clear=True)
    def test_resolve_skips_directory_candidates_before_readable_dotenv(
        self, mock_keychain_secret, mock_discover_env_files
    ):
        env_dir = Path(__file__).resolve().parent / "tmp_env_dir_first"
        env_file = Path(__file__).resolve().parent / "tmp_resolve_after_dir.env"
        env_dir.mkdir(exist_ok=True)
        env_file.write_text(
            "\n".join(
                [
                    "STRAVA_CLIENT_ID=dotenv-client-id",
                    "STRAVA_CLIENT_SECRET=dotenv-client-secret",
                ]
            ),
            encoding="utf-8",
        )
        mock_discover_env_files.return_value = [env_dir, env_file]
        mock_keychain_secret.side_effect = (
            lambda var_name: "keychain-refresh-token"
            if var_name == "STRAVA_REFRESH_TOKEN"
            else None
        )

        try:
            values, sources, _ = strava_pull.resolve_strava_credentials()
        finally:
            env_file.unlink(missing_ok=True)
            env_dir.rmdir()

        self.assertEqual(values["STRAVA_CLIENT_ID"], "dotenv-client-id")
        self.assertEqual(values["STRAVA_CLIENT_SECRET"], "dotenv-client-secret")
        self.assertEqual(values["STRAVA_REFRESH_TOKEN"], "keychain-refresh-token")
        self.assertEqual(sources["STRAVA_CLIENT_ID"], f"dotenv:{env_file}")
        self.assertEqual(sources["STRAVA_CLIENT_SECRET"], f"dotenv:{env_file}")
        self.assertEqual(sources["STRAVA_REFRESH_TOKEN"], "keychain")

    @patch.object(strava_pull, "discover_env_files")
    @patch.object(strava_pull, "load_keychain_secret")
    @patch("os.access")
    @patch.dict("os.environ", {}, clear=True)
    def test_resolve_skips_unreadable_candidates_before_parsing(
        self, mock_access, mock_keychain_secret, mock_discover_env_files
    ):
        unreadable = Path(__file__).resolve().parent / "tmp_unreadable.env"
        readable = Path(__file__).resolve().parent / "tmp_readable.env"
        unreadable.write_text("STRAVA_CLIENT_ID=bad-value", encoding="utf-8")
        readable.write_text(
            "\n".join(
                [
                    "STRAVA_CLIENT_ID=dotenv-client-id",
                    "STRAVA_CLIENT_SECRET=dotenv-client-secret",
                    "STRAVA_REFRESH_TOKEN=dotenv-refresh-token",
                ]
            ),
            encoding="utf-8",
        )
        mock_discover_env_files.return_value = [unreadable, readable]
        mock_keychain_secret.return_value = None

        original_parse_dotenv = strava_pull.parse_dotenv

        def guarded_parse_dotenv(path):
            if path == unreadable:
                self.fail("Unreadable .env candidate should be skipped before parsing")
            return original_parse_dotenv(path)

        strava_pull.parse_dotenv = guarded_parse_dotenv

        def fake_access(path, mode):
            path_obj = Path(path)
            if path_obj == unreadable:
                return False
            return path_obj.is_file()

        mock_access.side_effect = fake_access

        try:
            values, sources, _ = strava_pull.resolve_strava_credentials()
        finally:
            strava_pull.parse_dotenv = original_parse_dotenv
            unreadable.unlink(missing_ok=True)
            readable.unlink(missing_ok=True)

        self.assertEqual(values["STRAVA_CLIENT_ID"], "dotenv-client-id")
        self.assertEqual(values["STRAVA_CLIENT_SECRET"], "dotenv-client-secret")
        self.assertEqual(values["STRAVA_REFRESH_TOKEN"], "dotenv-refresh-token")
        self.assertEqual(sources["STRAVA_CLIENT_ID"], f"dotenv:{readable}")
        self.assertEqual(sources["STRAVA_CLIENT_SECRET"], f"dotenv:{readable}")
        self.assertEqual(sources["STRAVA_REFRESH_TOKEN"], f"dotenv:{readable}")
