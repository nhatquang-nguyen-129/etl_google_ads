import os
import sys
import logging
import uuid
import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), "../../"
            )
        )
    )

class GoogleBigqueryLoader:
    """
    Google BigQuery Loader
    ----------------------
    Workflow:
        1. Initialize BigQuery client
        2. Check dataset existence
        3. Create dataset if not exist
        4. Check table existence
        5. Create table if not exist
        6. Apply INSERT/UPSERT DML
        7. Append data into table
    """

# 1.1. Initialize
    def __init__(self) -> None:
        super().__init__(backend="bigquery")
        self.client: bigquery.Client | None = None
        self.project: str | None = None


# 1.2. Workflow

    # 1.2.1. Initialize Google BigQuery client
    def _init_client(self, direction: str) -> None:
        if self.client:
            return

        parts = direction.split(".")
        if len(parts) != 3:
            raise ValueError(
                "❌ [PLUGIN] Failed to initialize Google BigQuery client due to direction "
                f"{direction} does not comply with project.dataset.table format. "
            )

        project, _, _ = parts
        self.project = project
        self.client = bigquery.Client(project=project)
        msg = (
            "✅ [PLUGIN] Successfull initialized Google BigQuery client for project "
            f"{project}."
            )
        print(msg)
        logging.info(msg)

    # 1.2.2. Check dataset existence
    def _check_dataset_exists(self, project: str, dataset: str) -> bool:
        dataset_id = f"{project}.{dataset}"

        try:
            msg = f"🔍 [PLUGIN] Checking Google BigQuery dataset {dataset_id} existence..."
            print(msg)
            logging.info(msg)

            self.client.get_dataset(dataset_id)

            msg = f"✅ [PLUGIN] Dataset {dataset_id} exists."
            print(msg)
            logging.info(msg)
            return True

        except NotFound:
            msg = f"⚠️ [INGEST] Dataset {dataset_id} not found."
            print(msg)
            logging.warning(msg)
            return False

    # 1.2.3. Create dataset if not exist
    def _create_dataset(
        self,
        project: str,
        dataset: str,
        location: str = "US",
    ) -> None:
        dataset_id = f"{project}.{dataset}"

        msg = f"🔄 [INGEST] Creating BigQuery dataset {dataset_id} at location {location}..."
        print(msg)
        logging.info(msg)

        dataset_obj = bigquery.Dataset(dataset_id)
        dataset_obj.location = location

        self.client.create_dataset(dataset_obj, exists_ok=True)

        msg = f"✅ [INGEST] Successfully created BigQuery dataset {dataset_id}."
        print(msg)
        logging.info(msg)



    # 1.2.2. Infer DataFrame schema for Google BigQuery table
    @staticmethod
    def _infer_table_schema(df: pd.DataFrame) -> list[bigquery.SchemaField]:
        schema = []
        for col, dtype in df.dtypes.items():
            if pd.api.types.is_integer_dtype(dtype):
                bq_type = "INT64"
            elif pd.api.types.is_float_dtype(dtype):
                bq_type = "FLOAT64"
            elif pd.api.types.is_bool_dtype(dtype):
                bq_type = "BOOL"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                bq_type = "TIMESTAMP"
            else:
                bq_type = "STRING"
            schema.append(bigquery.SchemaField(col, bq_type))
        return schema

    # 1.2.3. Check table existence
    def _check_table_exists(self, direction: str) -> bool:
        try:
            self._init_client(direction)
            self.client.get_table(direction)
            return True
        except NotFound:
            return False

    # 1.2.3. 
    def _create_table(
        self,
        *,
        direction: str,
        df: pd.DataFrame,
        partition: dict | None = None,
        cluster: list[str] | None = None,
    ) -> None:
        table = bigquery.Table(
            direction,
            schema=self._infer_schema(df),
        )

        if partition:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition["field"],
            )

        if cluster:
            table.clustering_fields = cluster

        self.client.create_table(table)
        logging.info(f"✅ [INGEST] Created table {direction}")

    # ---------- Conflict ----------
    def _apply_upsert(
        self,
        *,
        direction: str,
        df: pd.DataFrame,
        upsert_keys: list[str],
    ) -> None:
        if not upsert_keys:
            raise ValueError("❌ upsert_keys is required for UPSERT")

        missing = [k for k in upsert_keys if k not in df.columns]
        if missing:
            raise ValueError(f"❌ Missing upsert keys: {missing}")

        df_keys = df[upsert_keys].dropna().drop_duplicates()
        if df_keys.empty:
            logging.warning("⚠️ No keys found for UPSERT, skip delete")
            return

        # Single-key delete
        if len(upsert_keys) == 1:
            key = upsert_keys[0]
            values = df_keys[key].tolist()

            query = f"""
            DELETE FROM `{direction}`
            WHERE {key} IN UNNEST(@values)
            """

            self.client.query(
                query,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ArrayQueryParameter("values", "STRING", values)
                    ]
                ),
            ).result()

            logging.info(f"🔄 Deleted existing rows by key {key}")
            return

        # Multi-key delete (temp table)
        project, dataset, _ = direction.split(".")
        temp_table = f"{project}.{dataset}._tmp_upsert_{uuid.uuid4().hex[:8]}"

        self.client.load_table_from_dataframe(
            df_keys,
            temp_table,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
        ).result()

        join_cond = " AND ".join([f"main.{k} = temp.{k}" for k in upsert_keys])

        self.client.query(
            f"""
            DELETE FROM `{direction}` AS main
            WHERE EXISTS (
                SELECT 1 FROM `{temp_table}` AS temp
                WHERE {join_cond}
            )
            """
        ).result()

        logging.info("🔄 Deleted existing rows by composite keys")

    # ---------- Public API ----------
    def load(
        self,
        *,
        df: pd.DataFrame,
        direction: str,
        load_mode: str,                 # insert | upsert
        upsert_keys: list[str] | None = None,
        partition: dict | None = None,
        cluster: list[str] | None = None,
    ) -> None:

        self._init_client(direction)
        exists = self._table_exists(direction)

        if not exists:
            self._create_table(
                direction=direction,
                df=df,
                partition=partition,
                cluster=cluster,
            )

        if load_mode == "upsert" and exists:
            self._apply_upsert(
                direction=direction,
                df=df,
                upsert_keys=upsert_keys or [],
            )

        self.client.load_table_from_dataframe(
            df,
            direction,
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND"
            ),
        ).result()

        logging.info(f"✅ Loaded {len(df)} rows into {direction}")