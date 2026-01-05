from decimal import Decimal
import functools
import os

import pandas
from nerc_rates import load_from_url

from process_report import util
from process_report.settings import invoice_settings

# List of service invoices processed by pipeline. Change if new services are added.
S3_SERVICE_INVOICE_LIST = [
    "ocp-test {invoice_month}.csv",
    "ocp-prod {invoice_month}.csv",
    "academic {invoice_month}.csv",
    "NERC OpenStack {invoice_month}.csv",
    "NERC Storage {invoice_month}.csv",
]


@functools.lru_cache
def get_rates_info():
    return load_from_url()


class Loader:
    @functools.lru_cache
    def get_csv_invoice_filepath_list(self) -> list[str]:
        """Fetch invoice CSV files from S3 if fetch_from_s3 is True. Returns local paths of files."""
        csv_invoice_filepath_list = []
        if invoice_settings.fetch_from_s3:
            s3_bucket = util.get_invoice_bucket()

            for invoice_name_template in S3_SERVICE_INVOICE_LIST:
                local_name = invoice_name_template.format(
                    invoice_month=invoice_settings.invoice_month
                )
                s3_name = (invoice_settings.invoice_path_template + local_name).format(
                    invoice_month=invoice_settings.invoice_month
                )
                csv_invoice_filepath_list.append(local_name)
                s3_bucket.download_file(s3_name, local_name)
        else:
            invoice_dir_path = invoice_settings.invoice_path_template.format(
                invoice_month=invoice_settings.invoice_month
            )
            for invoice in os.listdir(invoice_dir_path):
                invoice_absolute_path = os.path.join(invoice_dir_path, invoice)
                csv_invoice_filepath_list.append(invoice_absolute_path)

        return csv_invoice_filepath_list

    @functools.lru_cache
    def get_remote_filepath(self, remote_filepath: str) -> str:
        """Fetch a file from S3 if fetch_from_s3 is True. Returns local path of file."""
        if invoice_settings.fetch_from_s3:
            return util.fetch_s3(remote_filepath)
        return remote_filepath

    @functools.lru_cache
    def get_new_pi_credit_amount(self) -> Decimal:
        return invoice_settings.new_pi_credit_amount or get_rates_info().get_value_at(
            "New PI Credit", invoice_settings.invoice_month, Decimal
        )

    @functools.lru_cache
    def get_limit_new_pi_credit_to_partners(self) -> bool:
        return (
            invoice_settings.limit_new_pi_credit_to_partners
            or get_rates_info().get_value_at(
                "Limit New PI Credit to MGHPCC Partners",
                invoice_settings.invoice_month,
                bool,
            )
        )

    @functools.lru_cache
    def get_bu_subsidy_amount(self) -> Decimal:
        return invoice_settings.bu_subsidy_amount or get_rates_info().get_value_at(
            "BU Subsidy", invoice_settings.invoice_month, Decimal
        )

    @functools.lru_cache
    def get_lenovo_su_charge_info(self) -> dict[str, Decimal]:
        if invoice_settings.lenovo_charge_info:
            return invoice_settings.lenovo_charge_info

        lenovo_charge_info = {}
        for su_name in ["GPUA100SXM4", "GPUH100"]:
            lenovo_charge_info[su_name] = get_rates_info().get_value_at(
                f"Lenovo {su_name} Charge", invoice_settings.invoice_month, Decimal
            )
        return lenovo_charge_info

    @functools.lru_cache
    def get_alias_map(self) -> dict:
        alias_dict = dict()
        with open(
            self.get_remote_filepath(invoice_settings.alias_remote_filepath)
        ) as f:
            for line in f:
                pi_alias_info = line.strip().split(",")
                alias_dict[pi_alias_info[0]] = pi_alias_info[1:]

        return alias_dict

    @functools.lru_cache
    def load_dataframe(self, filepath: str) -> pandas.DataFrame:
        return pandas.read_csv(filepath)

    def get_nonbillable_pis(self) -> list[str]:
        with open(invoice_settings.nonbillable_pis_filepath) as file:
            return [line.rstrip() for line in file]

    def get_nonbillable_projects(self) -> list[str]:
        """Returns list of nonbillable projects for current invoice month"""
        with open(invoice_settings.nonbillable_projects_filepath) as file:
            projects = [line.rstrip() for line in file]

        timed_projects_list = self.get_nonbillable_timed_projects()
        return list(set(projects + timed_projects_list))

    def get_nonbillable_timed_projects(self) -> list[str]:
        """Returns list of projects that should be excluded based on dates"""
        dataframe = pandas.read_csv(
            invoice_settings.nonbillable_timed_projects_filepath
        )

        # convert to pandas timestamp objects
        dataframe["Start Date"] = pandas.to_datetime(
            dataframe["Start Date"], format="%Y-%m"
        )
        dataframe["End Date"] = pandas.to_datetime(
            dataframe["End Date"], format="%Y-%m"
        )

        mask = (dataframe["Start Date"] <= invoice_settings.invoice_month) & (
            invoice_settings.invoice_month <= dataframe["End Date"]
        )
        return dataframe[mask]["Project"].to_list()


loader = Loader()
