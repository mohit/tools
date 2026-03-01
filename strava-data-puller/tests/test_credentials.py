import importlib.util
import sys
import types
from pathlib import Path
from unittest import TestCase
from unittest.mock import call, patch


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
    def test_keychain_lookup_candidates_returns_ordered_fallbacks(self):
        self.assertEqual(
            strava_pull.keychain_account_lookup_candidates("STRAVA_CLIENT_SECRET"),
            [
                ("com.mohit.tools.strava-data-puller", "STRAVA_CLIENT_SECRET"),
                ("strava-data-puller", "STRAVA_CLIENT_SECRET"),
            ],
        )
        self.assertEqual(
            strava_pull.keychain_reversed_lookup_candidates("STRAVA_CLIENT_SECRET"),
            [
                ("STRAVA_CLIENT_SECRET", "com.mohit.tools.strava-data-puller"),
                ("STRAVA_CLIENT_SECRET", "strava-data-puller"),
            ],
        )
        self.assertEqual(
            strava_pull.keychain_service_only_lookup_candidates(),
            [
                ("com.mohit.tools.strava-data-puller", None),
                ("strava-data-puller", None),
            ],
        )
        self.assertEqual(
            strava_pull.keychain_lookup_candidates("STRAVA_CLIENT_SECRET"),
            [
                ("com.mohit.tools.strava-data-puller", "STRAVA_CLIENT_SECRET"),
                ("strava-data-puller", "STRAVA_CLIENT_SECRET"),
                ("STRAVA_CLIENT_SECRET", "com.mohit.tools.strava-data-puller"),
                ("STRAVA_CLIENT_SECRET", "strava-data-puller"),
                ("com.mohit.tools.strava-data-puller", None),
                ("strava-data-puller", None),
            ],
        )

    def test_keychain_lookup_candidates_puts_service_only_last(self):
        self.assertEqual(
            strava_pull.keychain_lookup_candidates("STRAVA_CLIENT_SECRET"),
            [
                ("com.mohit.tools.strava-data-puller", "STRAVA_CLIENT_SECRET"),
                ("strava-data-puller", "STRAVA_CLIENT_SECRET"),
                ("STRAVA_CLIENT_SECRET", "com.mohit.tools.strava-data-puller"),
                ("STRAVA_CLIENT_SECRET", "strava-data-puller"),
                ("com.mohit.tools.strava-data-puller", None),
                ("strava-data-puller", None),
            ],
        )

    def test_write_credentials_env_file(self):
        env_file = Path(__file__).resolve().parent / "tmp_written.env"
        credentials = {
            "STRAVA_CLIENT_ID": "abc123",
            "STRAVA_CLIENT_SECRET": "secret456",
            "STRAVA_REFRESH_TOKEN": "refresh789",
        }

        try:
            strava_pull.write_credentials_env_file(env_file, credentials)
            written = env_file.read_text(encoding="utf-8")
            mode = env_file.stat().st_mode & 0o777
        finally:
            env_file.unlink(missing_ok=True)

        self.assertIn('STRAVA_CLIENT_ID="abc123"', written)
        self.assertIn('STRAVA_CLIENT_SECRET="secret456"', written)
        self.assertIn('STRAVA_REFRESH_TOKEN="refresh789"', written)
        self.assertEqual(mode, 0o600)

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
    @patch.dict("os.environ", {}, clear=True)
    def test_resolve_keychain_lookup_uses_strict_lookup(
        self, mock_keychain_secret, mock_discover_env_files
    ):
        mock_discover_env_files.return_value = []
        mock_keychain_secret.side_effect = [
            "keychain-client-id",
            "keychain-client-secret",
            "keychain-refresh-token",
        ]

        values, sources, _ = strava_pull.resolve_strava_credentials()

        self.assertEqual(values["STRAVA_CLIENT_ID"], "keychain-client-id")
        self.assertEqual(values["STRAVA_CLIENT_SECRET"], "keychain-client-secret")
        self.assertEqual(values["STRAVA_REFRESH_TOKEN"], "keychain-refresh-token")
        self.assertEqual(sources["STRAVA_CLIENT_ID"], "keychain")
        self.assertEqual(sources["STRAVA_CLIENT_SECRET"], "keychain")
        self.assertEqual(sources["STRAVA_REFRESH_TOKEN"], "keychain")
        self.assertEqual(
            mock_keychain_secret.call_args_list,
            [
                call("STRAVA_CLIENT_ID"),
                call("STRAVA_CLIENT_SECRET"),
                call("STRAVA_REFRESH_TOKEN"),
            ],
        )

    @patch.object(strava_pull, "discover_env_files")
    @patch.object(strava_pull, "load_keychain_secret")
    @patch.dict("os.environ", {}, clear=True)
    def test_resolve_does_not_use_service_only_when_multiple_values_missing(
        self, mock_keychain_secret, mock_discover_env_files
    ):
        mock_discover_env_files.return_value = []
        mock_keychain_secret.return_value = None

        values, sources, _ = strava_pull.resolve_strava_credentials()

        self.assertEqual(values, {})
        self.assertEqual(sources, {})
        self.assertEqual(
            mock_keychain_secret.call_args_list,
            [
                call("STRAVA_CLIENT_ID"),
                call("STRAVA_CLIENT_SECRET"),
                call("STRAVA_REFRESH_TOKEN"),
            ],
        )

    @patch.object(strava_pull, "discover_env_files")
    @patch.object(strava_pull, "load_keychain_secret")
    @patch.dict("os.environ", {}, clear=True)
    def test_resolve_does_not_use_service_only_for_single_remaining_missing_value(
        self, mock_keychain_secret, mock_discover_env_files
    ):
        mock_discover_env_files.return_value = []

        def fake_keychain_lookup(var_name):
            if var_name == "STRAVA_CLIENT_ID":
                return "strict-client-id"
            if var_name == "STRAVA_CLIENT_SECRET":
                return "strict-client-secret"
            return None

        mock_keychain_secret.side_effect = fake_keychain_lookup

        values, sources, _ = strava_pull.resolve_strava_credentials()

        self.assertEqual(values["STRAVA_CLIENT_ID"], "strict-client-id")
        self.assertEqual(values["STRAVA_CLIENT_SECRET"], "strict-client-secret")
        self.assertNotIn("STRAVA_REFRESH_TOKEN", values)
        self.assertNotIn("STRAVA_REFRESH_TOKEN", sources)
        self.assertEqual(
            mock_keychain_secret.call_args_list,
            [
                call("STRAVA_CLIENT_ID"),
                call("STRAVA_CLIENT_SECRET"),
                call("STRAVA_REFRESH_TOKEN"),
            ],
        )

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

    @patch("subprocess.run")
    def test_load_keychain_secret_supports_service_account_reversed_format(self, mock_run):
        def fake_run(cmd, check, capture_output, text, timeout):
            if cmd == [
                "security",
                "find-generic-password",
                "-w",
                "-s",
                "STRAVA_CLIENT_ID",
                "-a",
                "strava-data-puller",
            ]:
                return types.SimpleNamespace(returncode=0, stdout="client-from-keychain\n")
            return types.SimpleNamespace(returncode=44, stdout="")

        mock_run.side_effect = fake_run

        value = strava_pull.load_keychain_secret("STRAVA_CLIENT_ID")

        self.assertEqual(value, "client-from-keychain")

    @patch("subprocess.run")
    def test_load_keychain_secret_tries_specific_lookups_by_default(self, mock_run):
        expected_calls = [
            [
                "security",
                "find-generic-password",
                "-w",
                "-s",
                "com.mohit.tools.strava-data-puller",
                "-a",
                "STRAVA_CLIENT_SECRET",
            ],
            [
                "security",
                "find-generic-password",
                "-w",
                "-s",
                "strava-data-puller",
                "-a",
                "STRAVA_CLIENT_SECRET",
            ],
            [
                "security",
                "find-generic-password",
                "-w",
                "-s",
                "STRAVA_CLIENT_SECRET",
                "-a",
                "com.mohit.tools.strava-data-puller",
            ],
            [
                "security",
                "find-generic-password",
                "-w",
                "-s",
                "STRAVA_CLIENT_SECRET",
                "-a",
                "strava-data-puller",
            ],
            [
                "security",
                "find-generic-password",
                "-w",
                "-s",
                "com.mohit.tools.strava-data-puller",
            ],
            [
                "security",
                "find-generic-password",
                "-w",
                "-s",
                "strava-data-puller",
            ],
        ]

        def fake_run(cmd, check, capture_output, text, timeout):
            call_index = len(mock_run.call_args_list) - 1
            self.assertEqual(cmd, expected_calls[call_index])
            return types.SimpleNamespace(returncode=44, stdout="")

        mock_run.side_effect = fake_run

        value = strava_pull.load_keychain_secret("STRAVA_CLIENT_SECRET")

        self.assertIsNone(value)
        self.assertEqual(len(mock_run.call_args_list), len(expected_calls))

    @patch("subprocess.run")
    def test_load_keychain_secret_prefers_namespaced_specific_match_over_legacy(
        self, mock_run
    ):
        namespaced_match = [
            "security",
            "find-generic-password",
            "-w",
            "-s",
            "com.mohit.tools.strava-data-puller",
            "-a",
            "STRAVA_CLIENT_ID",
        ]
        legacy_match = [
            "security",
            "find-generic-password",
            "-w",
            "-s",
            "strava-data-puller",
            "-a",
            "STRAVA_CLIENT_ID",
        ]

        def fake_run(cmd, check, capture_output, text, timeout):
            if cmd == namespaced_match:
                return types.SimpleNamespace(returncode=0, stdout="correct-client-id\n")
            if cmd == legacy_match:
                return types.SimpleNamespace(returncode=0, stdout="stale-client-id\n")
            return types.SimpleNamespace(returncode=44, stdout="")

        mock_run.side_effect = fake_run

        value = strava_pull.load_keychain_secret("STRAVA_CLIENT_ID")

        self.assertEqual(value, "correct-client-id")
        called_cmds = [call.args[0] for call in mock_run.call_args_list]
        self.assertNotIn(legacy_match, called_cmds)

    @patch("subprocess.run")
    def test_load_keychain_secret_uses_service_only_only_after_reversed_lookup(
        self, mock_run
    ):
        reversed_match = [
            "security",
            "find-generic-password",
            "-w",
            "-s",
            "STRAVA_CLIENT_SECRET",
            "-a",
            "com.mohit.tools.strava-data-puller",
        ]
        namespaced_service_only = [
            "security",
            "find-generic-password",
            "-w",
            "-s",
            "com.mohit.tools.strava-data-puller",
        ]

        def fake_run(cmd, check, capture_output, text, timeout):
            if cmd == reversed_match:
                return types.SimpleNamespace(returncode=0, stdout="correct-client-secret\n")
            if cmd == namespaced_service_only:
                return types.SimpleNamespace(returncode=0, stdout="wrong-refresh-token\n")
            return types.SimpleNamespace(returncode=44, stdout="")

        mock_run.side_effect = fake_run

        value = strava_pull.load_keychain_secret("STRAVA_CLIENT_SECRET")

        self.assertEqual(value, "correct-client-secret")
        called_cmds = [call.args[0] for call in mock_run.call_args_list]
        self.assertNotIn(namespaced_service_only, called_cmds)

    @patch("subprocess.run")
    def test_load_keychain_secret_uses_service_only_when_specific_lookups_fail(self, mock_run):
        namespaced_service_only = [
            "security",
            "find-generic-password",
            "-w",
            "-s",
            "com.mohit.tools.strava-data-puller",
        ]

        def fake_run(cmd, check, capture_output, text, timeout):
            if cmd == namespaced_service_only:
                return types.SimpleNamespace(returncode=0, stdout="fallback-client-secret\n")
            return types.SimpleNamespace(returncode=44, stdout="")

        mock_run.side_effect = fake_run

        value = strava_pull.load_keychain_secret("STRAVA_CLIENT_SECRET")

        self.assertEqual(value, "fallback-client-secret")

    @patch.object(strava_pull, "parse_args")
    @patch.object(strava_pull, "resolve_strava_credentials")
    @patch.object(strava_pull, "get_access_token")
    def test_main_check_credentials_does_not_exchange_token(
        self, mock_get_access_token, mock_resolve_credentials, mock_parse_args
    ):
        mock_parse_args.return_value = types.SimpleNamespace(
            install_credentials=False,
            check_credentials=True,
        )
        mock_resolve_credentials.return_value = (
            {
                "STRAVA_CLIENT_ID": "client-id",
                "STRAVA_CLIENT_SECRET": "client-secret",
                "STRAVA_REFRESH_TOKEN": "refresh-token",
            },
            {
                "STRAVA_CLIENT_ID": "keychain",
                "STRAVA_CLIENT_SECRET": "keychain",
                "STRAVA_REFRESH_TOKEN": "keychain",
            },
            [],
        )

        strava_pull.main()

        mock_get_access_token.assert_not_called()

    @patch.object(strava_pull, "export_parquet")
    @patch.object(strava_pull, "write_ndjson")
    @patch.object(strava_pull, "write_json")
    @patch.object(strava_pull, "fetch_activities")
    @patch.object(strava_pull, "fetch_stats")
    @patch.object(strava_pull, "fetch_athlete")
    @patch.object(strava_pull, "load_existing_activities")
    @patch.object(strava_pull, "get_access_token")
    @patch.object(strava_pull, "resolve_strava_credentials")
    @patch.object(strava_pull, "parse_args")
    def test_main_uses_resolved_values_for_token_exchange(
        self,
        mock_parse_args,
        mock_resolve_credentials,
        mock_get_access_token,
        mock_load_existing_activities,
        mock_fetch_athlete,
        mock_fetch_stats,
        mock_fetch_activities,
        mock_write_json,
        mock_write_ndjson,
        mock_export_parquet,
    ):
        out_dir = Path(__file__).resolve().parent / "tmp_main_out"
        mock_parse_args.return_value = types.SimpleNamespace(
            install_credentials=False,
            check_credentials=False,
            out_dir=str(out_dir),
            force=False,
            after=None,
            before=None,
            types=None,
            per_page=200,
            max_pages=1,
            include_streams=False,
            skip_parquet=False,
        )
        mock_resolve_credentials.return_value = (
            {
                "STRAVA_CLIENT_ID": "resolved-client-id",
                "STRAVA_CLIENT_SECRET": "resolved-client-secret",
                "STRAVA_REFRESH_TOKEN": "resolved-refresh-token",
            },
            {
                "STRAVA_CLIENT_ID": "keychain",
                "STRAVA_CLIENT_SECRET": "keychain",
                "STRAVA_REFRESH_TOKEN": "keychain",
            },
            [],
        )
        mock_get_access_token.return_value = "token"
        mock_load_existing_activities.return_value = ([], None)
        mock_fetch_athlete.return_value = 123
        mock_fetch_activities.return_value = []

        try:
            strava_pull.main()
        finally:
            out_dir.rmdir()

        mock_get_access_token.assert_called_once_with(
            "resolved-client-id",
            "resolved-client-secret",
            "resolved-refresh-token",
        )
        mock_fetch_stats.assert_called_once_with("token", 123, out_dir)
        mock_export_parquet.assert_called_once()
        mock_write_json.assert_called_once()
        mock_write_ndjson.assert_called_once()
