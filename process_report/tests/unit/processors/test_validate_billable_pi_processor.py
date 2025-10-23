from unittest import TestCase
import pandas
import uuid
import math

from process_report.tests import util as test_utils


class TestValidateBillablePIProcessor(TestCase):
    def test_remove_nonbillables(self):
        pis = [uuid.uuid4().hex for _ in range(10)]
        projects = [
            "P1",
            "P2",
            "P1",  # P1 is duplicated to test multiple projects with same name
            "P3",
            "P4",
            "P5",
            "P6",
            "P7",
            "P8",
            "P9",
        ]
        cluster_names = [
            "stack",
            "stack",
            "ocp-prod",
            "ocp-prod",
            "ocp-prod",
            "ocp-prod",
            "ocp-test",  # ocp-test is nonbillable
            "ocp-test",
            "bm",
            "bm",
        ]
        nonbillable_pis = [pis[1]]  # P2 (of second PI) is nonbillable
        nonbillable_projects = pandas.DataFrame(
            {
                "Project Name": ["p1", "p8", "p9"],  # Testing case insensitivity
                "Cluster": [
                    None,
                    "bm",
                    "ocp-prod",
                ],  # P1 is cluster-agnostic, P8-bm should be nonbillable, P9 should be billable because its on bm cluster in test invoice
                "Is Timed": [False, False, False],
            }
        )

        data = pandas.DataFrame(
            {
                "Manager (PI)": pis,
                "Project - Allocation": projects,
                "Cluster Name": cluster_names,
            }
        )

        validate_billable_pi_proc = test_utils.new_validate_billable_pi_processor(
            data=data,
            nonbillable_pis=nonbillable_pis,
            nonbillable_projects=nonbillable_projects,
        )
        validate_billable_pi_proc.process()
        output = validate_billable_pi_proc.data
        assert output[output["Is Billable"]].equals(data.iloc[[3, 4, 5, 9]])

    def test_empty_pi_name(self):
        test_data = pandas.DataFrame(
            {
                "Manager (PI)": ["PI1", math.nan, "PI1", "PI2", "PI2"],
                "Project - Allocation": [
                    "ProjectA",
                    "ProjectB",
                    "ProjectC",
                    "ProjectD",
                    "ProjectE",
                ],
                "Cluster Name": ["test-cluster"] * 5,
            }
        )
        assert len(test_data[pandas.isna(test_data["Manager (PI)"])]) == 1
        validate_billable_pi_proc = test_utils.new_validate_billable_pi_processor(
            data=test_data
        )
        validate_billable_pi_proc.process()
        output_data = validate_billable_pi_proc.data
        output_data = output_data[~output_data["Missing PI"]]
        assert len(output_data[pandas.isna(output_data["Manager (PI)"])]) == 0
