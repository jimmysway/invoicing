import logging
from dataclasses import dataclass, field

from pyiceberg.table import Table
from pyiceberg.catalog import Catalog, load_catalog
import pyarrow

import process_report.invoices.invoice as invoice
from process_report.loader import loader
from process_report.settings import invoice_settings

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def get_iceberg_catalog(config: dict, catalog_name: str) -> Catalog:
    return load_catalog(name=catalog_name, **config)


def get_iceberg_table(catalog: Catalog, table_path) -> Table:
    return catalog.load_table(table_path)


@dataclass
class IcebergInvoice(invoice.Invoice):
    export_columns_list = [
        invoice.INVOICE_DATE_FIELD,
        invoice.PROJECT_FIELD,
        invoice.PROJECT_ID_FIELD,
        invoice.PI_FIELD,
        invoice.CLUSTER_NAME_FIELD,
        invoice.INVOICE_EMAIL_FIELD,
        invoice.INVOICE_ADDRESS_FIELD,
        invoice.INSTITUTION_FIELD,
        invoice.INSTITUTION_ID_FIELD,
        invoice.IS_BILLABLE_FIELD,
        invoice.SU_HOURS_FIELD,
        invoice.SU_TYPE_FIELD,
        invoice.RATE_FIELD,
        invoice.GROUP_NAME_FIELD,
        invoice.GROUP_INSTITUTION_FIELD,
        invoice.GROUP_BALANCE_FIELD,
        invoice.COST_FIELD,
        invoice.GROUP_BALANCE_USED_FIELD,
        invoice.CREDIT_FIELD,
        invoice.CREDIT_CODE_FIELD,
        invoice.BALANCE_FIELD,
    ]

    iceberg_catalog_name: str = invoice_settings.iceberg_catalog_name
    iceberg_catalog_config: dict = field(
        default_factory=lambda: loader.get_iceberg_config()
    )
    iceberg_table_path: str = invoice_settings.iceberg_table_path

    def _prepare(self):
        iceberg_catalog = get_iceberg_catalog(
            self.iceberg_catalog_config, self.iceberg_catalog_name
        )
        self.iceberg_table = get_iceberg_table(iceberg_catalog, self.iceberg_table_path)
        self.export_data = self.data

    def export(self):
        # Overrides base invoice export behavior
        self._filter_columns()

        # Update table schema, only allows "possible" migrations (i.e raises on str -> Decimal)
        # TODO (Quan) When we implement typing validation for dataframes, change this to raise errors
        with self.iceberg_table.update_schema() as update_schema:
            try:
                update_schema.union_by_name(
                    pyarrow.Table.from_pandas(self.export_data).schema
                )
            except ValueError as e:
                logger.warning(
                    f"Dataframe contains columns not convertable to PyIceberg: {e}"
                )

        self.iceberg_table.append(pyarrow.Table.from_pandas(self.export_data))

    def export_s3(self, s3_bucket):
        return
