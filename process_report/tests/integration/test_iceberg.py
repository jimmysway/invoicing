from pyiceberg import schema, catalog

from process_report.invoices.iceberg_invoice import IcebergInvoice
from process_report.tests.base import BaseTestCaseWithTempDir


class TestIceberg(BaseTestCaseWithTempDir):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Create in-memory catalog
        cls.catalog_name = "catalog_foo"
        cls.table_path = "namespace_foo.table_foo"

        config_dict = {
            "type": "sql",
            "warehouse": str(cls.tempdir),
            "uri": f"sqlite:///{str(cls.tempdir)}/foo.db",
        }
        cls.catalog_config = config_dict

        # Initialize test schema that's used in setUp()
        cls.catalog = catalog.load_catalog(name=cls.catalog_name, **config_dict)
        cls.test_schema = schema.Schema(
            schema.NestedField(1, "Invoice Month", schema.StringType()),
            schema.NestedField(2, "Cost", schema.DecimalType(21, 2)),
            schema.NestedField(3, "PI", schema.StringType()),
        )

    def setUp(self):
        self.catalog.create_namespace_if_not_exists("namespace_foo")
        self.catalog.create_table_if_not_exists(self.table_path, self.test_schema)

    def tearDown(self):
        self.catalog.drop_table(self.table_path)

    def test_upload_one_dataframe(self):
        # Create test dataframe matching table schema
        test_df = self.create_test_invoice(
            {
                "Invoice Month": ["2024-01", "2024-01"],
                "Cost": [100.0, 200.0],
                "PI": ["PI1", "PI2"],
            }
        )

        # Create IcebergInvoice instance
        inv = IcebergInvoice(
            invoice_month="2024-01",
            data=test_df,
            iceberg_catalog_name=self.catalog_name,
            iceberg_catalog_config=self.catalog_config,
            iceberg_table_path=self.table_path,
        )
        # Ensure only test columns are filtered
        inv.export_columns_list = ["Invoice Month", "Cost", "PI"]
        inv.process()
        inv.export()

        # Verify data was uploaded, and Iceberg cost column can be casted to Decimal
        table = self.catalog.load_table(self.table_path)
        uploaded_df = table.scan().to_pandas().astype(test_df.dtypes)
        assert uploaded_df.equals(test_df)

    def test_upload_new_column(self):
        # Create test dataframe with an extra column
        test_df = self.create_test_invoice(
            {
                "Invoice Month": ["2024-02", "2024-02"],
                "Cost": [150.0, 250.0],
                "PI": ["PI3", "PI4"],
                "extra_column": ["extra1", "extra2"],  # New column
            }
        )

        # Create IcebergInvoice instance
        inv = IcebergInvoice(
            invoice_month="2024-02",
            data=test_df,
            iceberg_catalog_name=self.catalog_name,
            iceberg_catalog_config=self.catalog_config,
            iceberg_table_path=self.table_path,
        )
        inv.export_columns_list = ["Invoice Month", "Cost", "PI", "extra_column"]
        inv.process()
        inv.export()

        # Verify data was uploaded with new column (schema evolution)
        table = self.catalog.load_table(self.table_path)
        uploaded_df = table.scan().to_pandas().astype(test_df.dtypes)
        assert uploaded_df.equals(test_df)

    def test_schema_evolution_with_existing_data(self):
        # First, upload initial data without extra column
        first_df = self.create_test_invoice(
            {
                "Invoice Month": ["2024-01", "2024-01"],
                "Cost": [100.0, 200.0],
                "PI": ["PI1", "PI2"],
            }
        )

        inv = IcebergInvoice(
            invoice_month="2024-01",
            data=first_df,
            iceberg_catalog_name=self.catalog_name,
            iceberg_catalog_config=self.catalog_config,
            iceberg_table_path=self.table_path,
        )
        inv.export_columns_list = ["Invoice Month", "Cost", "PI"]
        inv.process()
        inv.export()

        # Now upload data with an extra column
        second_df = self.create_test_invoice(
            {
                "Invoice Month": ["2024-02", "2024-02"],
                "Cost": [150.0, 250.0],
                "PI": ["PI3", "PI4"],
                "extra_column": ["new1", "new2"],  # New column
            }
        )

        inv2 = IcebergInvoice(
            invoice_month="2024-02",
            data=second_df,
            iceberg_catalog_name=self.catalog_name,
            iceberg_catalog_config=self.catalog_config,
            iceberg_table_path=self.table_path,
        )
        inv2.export_columns_list = ["Invoice Month", "Cost", "PI", "extra_column"]
        inv2.process()
        inv2.export()

        table = self.catalog.load_table(self.table_path)
        result_df = table.scan().to_pandas().astype(second_df.dtypes)

        # Verify the table has schema evolved with the new column
        # Old rows should have None for the new column
        expected_df = self.create_test_invoice(
            {
                "Invoice Month": ["2024-02", "2024-02", "2024-01", "2024-01"],
                "Cost": [150.0, 250.0, 100.0, 200.0],
                "PI": ["PI3", "PI4", "PI1", "PI2"],
                "extra_column": ["new1", "new2", None, None],
            }
        )
        assert result_df.equals(expected_df)
