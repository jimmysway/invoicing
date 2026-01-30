from unittest import TestCase, mock
import tempfile
import pandas
import os
from textwrap import dedent
import pytest

from process_report.settings import invoice_settings
from process_report.loader import loader
from process_report import process_report, util


class TestMonthUtils(TestCase):
    def test_get_month_diff(self):
        testcases = [
            (("2024-12", "2024-03"), 9),
            (("2024-12", "2023-03"), 21),
            (("2024-11", "2024-12"), -1),
            (("2024-12", "2025-03"), -3),
        ]
        for arglist, answer in testcases:
            assert util.get_month_diff(*arglist) == answer
        with pytest.raises(ValueError):
            util.get_month_diff("2024-16", "2025-03")


class TestMergeCSV(TestCase):
    def setUp(self):
        self.header = ["Cost", "Name", "Rate"]
        self.data = [
            [1, "Alice", 25],
            [2, "Bob", 30],
            [3, "Charlie", 28],
        ]

        self.csv_files = []

        for _ in range(3):
            csv_file = tempfile.NamedTemporaryFile(
                delete=False, mode="w", suffix=".csv"
            )
            self.csv_files.append(csv_file)
            dataframe = pandas.DataFrame(self.data, columns=self.header)
            dataframe.to_csv(csv_file, index=False)
            csv_file.close()

    def tearDown(self):
        for csv_file in self.csv_files:
            os.remove(csv_file.name)

    def test_merge_csv(self):
        merged_dataframe = process_report.merge_csv(
            [csv_file.name for csv_file in self.csv_files]
        )

        expected_rows = len(self.data) * 3

        # `len` for a pandas dataframe excludes the header row
        assert len(merged_dataframe) == expected_rows

        # Assert that the headers in the merged DataFrame match the expected headers
        assert merged_dataframe.columns.tolist() == self.header


class TestTimedProjects(TestCase):
    def setUp(self):
        # Without the dedent method, our data will have leading spaces which
        # messes up the first key. Also the '\' is imporant to ignore the first
        # new line we added so it's more readable in code.
        self.csv_data = dedent(
            """\
        Project,Start Date,End Date
        ProjectA,2022-09,2023-08
        ProjectB,2022-09,2023-09
        ProjectC,2023-09,2024-08
        ProjectD,2022-09,2024-08
        """
        )

        self.csv_file = tempfile.NamedTemporaryFile(delete=False, mode="w")
        self.csv_file.write(self.csv_data)
        self.csv_file.close()

        invoice_settings.invoice_month = pandas.Timestamp("2023-09")
        invoice_settings.nonbillable_timed_projects_filepath = self.csv_file.name

    def tearDown(self):
        os.remove(self.csv_file.name)

    def test_timed_projects(self):
        excluded_projects = loader.get_nonbillable_timed_projects()

        expected_projects = ["ProjectB", "ProjectC", "ProjectD"]
        assert excluded_projects == expected_projects


class TestValidateRequiredEnvVars(TestCase):
    @mock.patch.dict(
        "os.environ", {"KEYCLOAK_CLIENT_ID": "test", "KEYCLOAK_CLIENT_SECRET": "test"}
    )
    def test_env_vars_valid(self):
        process_report.validate_required_env_vars(
            ["KEYCLOAK_CLIENT_ID", "KEYCLOAK_CLIENT_SECRET"]
        )

    @mock.patch.dict("os.environ", {"KEYCLOAK_CLIENT_ID": "test"})
    def test_env_vars_missing(self):
        with pytest.raises(SystemExit):
            process_report.validate_required_env_vars(
                ["KEYCLOAK_CLIENT_ID", "KEYCLOAK_CLIENT_SECRET"]
            )
