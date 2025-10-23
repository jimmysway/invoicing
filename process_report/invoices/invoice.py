from dataclasses import dataclass
import pandas

import process_report.util as util


### PI file field names
PI_PI_FIELD = "PI"
PI_FIRST_MONTH = "First Invoice Month"
PI_INITIAL_CREDITS = "Initial Credits"
PI_1ST_USED = "1st Month Used"
PI_2ND_USED = "2nd Month Used"
###

### Prepay files fields
PREPAY_MONTH_FIELD = "Month"
PREPAY_CREDIT_FIELD = "Credit"
PREPAY_DEBIT_FIELD = "Debit"
PREPAY_GROUP_NAME_FIELD = "Group Name"
PREPAY_GROUP_CONTACT_FIELD = "Group Contact Email"
PREPAY_MANAGED_FIELD = "MGHPCC Managed"
PREPAY_PROJECT_FIELD = "Project"
PREPAY_START_DATE_FIELD = "Start Date"
PREPAY_END_DATE_FIELD = "End Date"
###

### Nonbillable projects file fields
NONBILLABLE_PROJECT_NAME = "Project Name"
NONBILLABLE_CLUSTER_NAME = "Cluster"
NONBILLABLE_IS_TIMED = "Timed"

### Invoice field names
INVOICE_DATE_FIELD = "Invoice Month"
PROJECT_FIELD = "Project - Allocation"
PROJECT_ID_FIELD = "Project - Allocation ID"
PI_FIELD = "Manager (PI)"
INVOICE_EMAIL_FIELD = "Invoice Email"
INVOICE_ADDRESS_FIELD = "Invoice Address"
INSTITUTION_FIELD = "Institution"
INSTITUTION_ID_FIELD = "Institution - Specific Code"
GROUP_NAME_FIELD = "Prepaid Group Name"
GROUP_INSTITUTION_FIELD = "Prepaid Group Institution"
GROUP_BALANCE_FIELD = "Prepaid Group Balance"
GROUP_BALANCE_USED_FIELD = "Prepaid Group Used"
SU_HOURS_FIELD = "SU Hours (GBhr or SUhr)"
SU_TYPE_FIELD = "SU Type"
SU_CHARGE_FIELD = "SU Charge"
LENOVO_CHARGE_FIELD = "Charge"
RATE_FIELD = "Rate"
COST_FIELD = "Cost"
CREDIT_FIELD = "Credit"
CREDIT_CODE_FIELD = "Credit Code"
SUBSIDY_FIELD = "Subsidy"
BALANCE_FIELD = "Balance"
###

### Internally used field names
IS_BILLABLE_FIELD = "Is Billable"
MISSING_PI_FIELD = "Missing PI"
PI_BALANCE_FIELD = "PI Balance"
PROJECT_NAME_FIELD = "Project"
GROUP_MANAGED_FIELD = "MGHPCC Managed"
CLUSTER_NAME_FIELD = "Cluster Name"
###


@dataclass
class Invoice:
    export_columns_list = list()
    exported_columns_map = dict()

    invoice_month: str
    data: pandas.DataFrame
    name: str = ""
    export_data = None

    def process(self):
        self._prepare()
        self._process()
        self._prepare_export()

    @property
    def output_path(self) -> str:
        return f"{self.name} {self.invoice_month}.csv"

    @property
    def output_s3_key(self) -> str:
        return f"Invoices/{self.invoice_month}/{self.name} {self.invoice_month}.csv"

    @property
    def output_s3_archive_key(self):
        return f"Invoices/{self.invoice_month}/Archive/{self.name} {self.invoice_month} {util.get_iso8601_time()}.csv"

    def _prepare(self):
        """Prepares the data for processing.

        Implement in subclass if necessary. May add or remove columns
        necessary for processing, add or remove rows, validate the data, or
        perform simple substitutions.
        """
        pass

    def _process(self):
        """Processes the data.

        Implement in subclass if necessary. Performs necessary calculations
        on the data, e.g. applying subsidies or credits.
        """
        pass

    def _prepare_export(self):
        """Prepares the data for export.

        Implement in subclass if necessary. May add or remove columns or rows
        that should or should not be exported after processing."""
        pass

    def _filter_columns(self):
        """Filters and renames columns before exporting"""
        self.export_data = self.export_data[self.export_columns_list].rename(
            columns=self.exported_columns_map
        )

    def export(self):
        self._filter_columns()
        self.export_data.to_csv(self.output_path, index=False)

    def export_s3(self, s3_bucket):
        s3_bucket.upload_file(self.output_path, self.output_s3_key)
        s3_bucket.upload_file(self.output_path, self.output_s3_archive_key)
