import sys
import logging
import os

import pandas
import pyarrow

from process_report.settings import invoice_settings
from process_report.loader import loader
from process_report import util
from process_report.invoices import (
    invoice,
    lenovo_invoice,
    nonbillable_invoice,
    billable_invoice,
    NERC_total_invoice,
    bu_internal_invoice,
    pi_specific_invoice,
    MOCA_prepaid_invoice,
    prepay_credits_snapshot,
    ocp_test_invoice,
)
from process_report.processors import (
    coldfront_fetch_processor,
    validate_pi_alias_processor,
    add_institution_processor,
    lenovo_processor,
    validate_billable_pi_processor,
    new_pi_credit_processor,
    bu_subsidy_processor,
    prepayment_processor,
    validate_cluster_name_processor,
)


PI_S3_FILEPATH = "PIs/PI.csv"
ALIAS_S3_FILEPATH = "PIs/alias.csv"
PREPAY_DEBITS_S3_FILEPATH = "Prepay/prepay_debits.csv"


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def validate_required_env_vars(required_env_vars):
    for required_env_var in required_env_vars:
        if required_env_var not in os.environ:
            sys.exit(f"Required environment variable {required_env_var} is not set")


def main():
    """Remove non-billable PIs and projects"""
    required_env_vars = []
    if not invoice_settings.coldfront_api_filepath:
        required_env_vars.extend(["KEYCLOAK_CLIENT_ID", "KEYCLOAK_CLIENT_SECRET"])
    validate_required_env_vars(required_env_vars)

    invoice_month = invoice_settings.invoice_month

    merged_dataframe = merge_csv(loader.get_csv_invoice_filepath_list())

    logger.info("Invoice date: " + str(invoice_month))
    logger.info("The following timed-projects will not be billed for this period: ")
    logger.info(loader.get_nonbillable_timed_projects())

    ### Preliminary processing
    processed_data = process_merged_dataframe(
        invoice_month,
        merged_dataframe,
        [
            validate_cluster_name_processor.ValidateClusterNameProcessor,
            coldfront_fetch_processor.ColdfrontFetchProcessor,
            validate_pi_alias_processor.ValidatePIAliasProcessor,
            add_institution_processor.AddInstitutionProcessor,
            lenovo_processor.LenovoProcessor,
            validate_billable_pi_processor.ValidateBillablePIsProcessor,
            new_pi_credit_processor.NewPICreditProcessor,
            bu_subsidy_processor.BUSubsidyProcessor,
            prepayment_processor.PrepaymentProcessor,
        ],
    )

    ### Export invoices
    process_and_export_invoices(
        invoice_month,
        processed_data,
        [
            lenovo_invoice.LenovoInvoice,
            nonbillable_invoice.NonbillableInvoice,
            billable_invoice.BillableInvoice,
            NERC_total_invoice.NERCTotalInvoice,
            bu_internal_invoice.BUInternalInvoice,
            pi_specific_invoice.PIInvoice,
            MOCA_prepaid_invoice.MOCAPrepaidInvoice,
            prepay_credits_snapshot.PrepayCreditsSnapshot,
            ocp_test_invoice.OcpTestInvoice,
        ],
        invoice_settings.upload_to_s3,
    )


def merge_csv(files):
    """Merge multiple CSV files and return a single pandas dataframe"""
    dataframes = []
    for file in files:
        dataframe = pandas.read_csv(
            file,
        )
        dataframe = dataframe.astype(
            {
                invoice.COST_FIELD: pandas.ArrowDtype(pyarrow.decimal128(21, 2)),
                invoice.RATE_FIELD: str,
            }
        )
        dataframes.append(dataframe)

    merged_dataframe = pandas.concat(dataframes, ignore_index=True)
    merged_dataframe.reset_index(drop=True, inplace=True)
    return merged_dataframe


def process_merged_dataframe(
    invoice_month, dataframe: pandas.DataFrame, processors: list
) -> pandas.DataFrame:
    for processor in processors:
        proc_instance = processor(name="", invoice_month=invoice_month, data=dataframe)
        proc_instance.process()
        dataframe = proc_instance.data
    return dataframe


def process_and_export_invoices(
    invoice_month, processed_data, invoice_list, upload_to_s3
):
    for inv in invoice_list:
        inv_instance = inv(invoice_month=invoice_month, data=processed_data)
        inv_instance.process()
        inv_instance.export()
        if upload_to_s3:
            bucket = util.get_invoice_bucket()
            inv_instance.export_s3(bucket)


def backup_to_s3_old_pi_file(old_pi_file):
    invoice_bucket = util.get_invoice_bucket()
    invoice_bucket.upload_file(
        old_pi_file, f"PIs/Archive/PI {util.get_iso8601_time()}.csv"
    )


if __name__ == "__main__":
    main()
