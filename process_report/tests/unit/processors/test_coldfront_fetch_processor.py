from unittest import TestCase, mock
import pandas
import pytest

from process_report.tests import util as test_utils


class TestColdfrontFetchProcessor(TestCase):
    def _get_test_invoice(
        self,
        allocation_project_id,
        allocation_project_name=None,
        pi=None,
        institute_code=None,
        cluster_name=None,
    ):
        if not pi:
            pi = [""] * len(allocation_project_id)

        if not institute_code:
            institute_code = [""] * len(allocation_project_id)

        if not allocation_project_name:
            allocation_project_name = allocation_project_id

        if not cluster_name:
            cluster_name = [""] * len(allocation_project_id)

        return pandas.DataFrame(
            {
                "Manager (PI)": pi,
                "Project - Allocation": allocation_project_name,
                "Project - Allocation ID": allocation_project_id,
                "Institution - Specific Code": institute_code,
                "Cluster Name": cluster_name,
            }
        )

    def _get_mock_allocation_data(
        self, project_id_list, pi_list, institute_code_list, cluster_list
    ):
        mock_data = []
        for i, project in enumerate(project_id_list):
            mock_data.append(
                {
                    "resource": {
                        "name": cluster_list[i],
                    },
                    "project": {
                        "pi": pi_list[i],
                    },
                    "attributes": {
                        "Allocated Project ID": project,
                        "Allocated Project Name": f"{project}-name",
                        "Institution-Specific Code": institute_code_list[i],
                    },
                }
            )

        return mock_data

    @mock.patch(
        "process_report.processors.coldfront_fetch_processor.ColdfrontFetchProcessor._fetch_coldfront_allocation_api",
    )
    def test_coldfront_fetch(self, mock_get_allocation_data):
        mock_get_allocation_data.return_value = self._get_mock_allocation_data(
            ["P1", "P2", "P3", "P4"],
            ["PI1", "PI1", "", "PI12"],
            ["IC1", "", "", "IC2"],
            ["stack"] * 4,
        )
        test_invoice = self._get_test_invoice(
            ["P1", "P1", "P2", "P3", "P4"], cluster_name=["stack"] * 5
        )
        answer_invoice = self._get_test_invoice(
            ["P1", "P1", "P2", "P3", "P4"],
            ["P1-name", "P1-name", "P2-name", "P3-name", "P4-name"],
            ["PI1", "PI1", "PI1", "", "PI12"],
            ["IC1", "IC1", "", "", "IC2"],
            ["stack"] * 5,
        )
        test_coldfront_fetch_proc = test_utils.new_coldfront_fetch_processor(
            data=test_invoice
        )
        test_coldfront_fetch_proc.process()
        output_invoice = test_coldfront_fetch_proc.data
        assert output_invoice.equals(answer_invoice)

    @mock.patch(
        "process_report.processors.coldfront_fetch_processor.ColdfrontFetchProcessor._fetch_coldfront_allocation_api",
    )
    def test_coldfront_project_not_found(self, mock_get_allocation_data):
        """What happens when an invoice project is not found in Coldfront."""
        mock_get_allocation_data.return_value = self._get_mock_allocation_data(
            ["P1", "P2"], ["PI1", "PI1"], ["IC1", "IC2"], ["stack"] * 2
        )
        test_nonbillable_projects = pandas.DataFrame(
            {
                "Project Name": ["P3"],
                "Cluster": [None],
                "Is Timed": [False],
            }
        )
        test_invoice = self._get_test_invoice(
            ["P1", "P2", "P3", "P4", "P5"], cluster_name=["stack"] * 5
        )
        answer_project_set = [("P4", "stack"), ("P5", "stack")]
        test_coldfront_fetch_proc = test_utils.new_coldfront_fetch_processor(
            data=test_invoice, nonbillable_projects=test_nonbillable_projects
        )

        with pytest.raises(ValueError) as cm:
            test_coldfront_fetch_proc.process()

        assert str(cm.value) == (
            f"Projects {answer_project_set} not found in Coldfront and are billable! Please check the project names"
        )

    @mock.patch(
        "process_report.processors.coldfront_fetch_processor.ColdfrontFetchProcessor._fetch_coldfront_allocation_api",
    )
    def test_nonbillable_clusters(self, mock_get_allocation_data):
        """No errors are raised when an invoice project belonging
        to a non billable cluster (ocp-test) is not found in Coldfront"""
        mock_get_allocation_data.return_value = self._get_mock_allocation_data(
            ["P1", "P2"],
            ["PI1", "PI1"],
            ["IC1", "IC2"],
            ["ocp-prod", "stack"],
        )
        test_invoice = self._get_test_invoice(
            allocation_project_id=["P1", "P2", "P3", "P4"],
            cluster_name=["ocp-prod", "stack", "ocp-test", "ocp-test"],
        )
        answer_invoice = self._get_test_invoice(
            ["P1", "P2", "P3", "P4"],
            ["P1-name", "P2-name", "P3", "P4"],
            ["PI1", "PI1", "", ""],
            ["IC1", "IC2", "", ""],
            ["ocp-prod", "stack", "ocp-test", "ocp-test"],
        )
        test_coldfront_fetch_proc = test_utils.new_coldfront_fetch_processor(
            data=test_invoice
        )
        test_coldfront_fetch_proc.process()
        output_invoice = test_coldfront_fetch_proc.data
        assert output_invoice.equals(answer_invoice)

    @mock.patch(
        "process_report.processors.coldfront_fetch_processor.ColdfrontFetchProcessor._fetch_coldfront_allocation_api",
    )
    def test_missing_project_cluster_tuples(self, mock_get_allocation_data):
        # API returns allocations for P1@clusterA and P2@clusterA only
        mock_get_allocation_data.return_value = self._get_mock_allocation_data(
            ["P1", "P2"],
            ["PI1", "PI2"],
            ["IC1", "IC2"],
            ["clusterA", "clusterA"],
        )

        # Invoice contains two rows for P1 on different clusters, plus P2 and P4
        test_invoice = self._get_test_invoice(
            allocation_project_id=["P1", "P1", "P2", "P4"],
            cluster_name=["clusterA", "clusterB", "clusterA", "clusterA"],
        )

        test_coldfront_fetch_proc = test_utils.new_coldfront_fetch_processor(
            data=test_invoice
        )

        with pytest.raises(ValueError) as cm:
            test_coldfront_fetch_proc.process()

        expected_missing = [("P1", "clusterB"), ("P4", "clusterA")]
        assert str(cm.value) == (
            f"Projects {expected_missing} not found in Coldfront and are billable! Please check the project names"
        )
