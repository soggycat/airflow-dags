"""
Airflow v3 DAG: SMB Share → OpenShift NooBaa S3 Bucket

Dependencies:
    pip install apache-airflow>=3.0.0 pysmb boto3

Airflow Connections required:
    - smb_default (type: generic / custom)
        host      : SMB server hostname or IP
        login     : SMB username
        password  : SMB password
        extra     : {"share_name": "MyShare", "remote_path": "/data/incoming"}

Airflow Variables required (or replace with your preferred secrets backend):
    - NOOBAA_ENDPOINT   : e.g. https://s3.openshift-storage.svc:443
    - NOOBAA_ACCESS_KEY : NooBaa / OBC access key
    - NOOBAA_SECRET_KEY : NooBaa / OBC secret key
    - NOOBAA_BUCKET     : target bucket name
"""

from __future__ import annotations

import io
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

import boto3
from botocore.client import Config
from smb.SMBConnection import SMBConnection

from airflow import DAG
from airflow.sdk import Variable

# Airflow 3.x imports
from airflow.sdk.bases.hook import BaseHook
from airflow.providers.standard.operators.python import PythonOperator

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default DAG arguments
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# Helper: build an SMB connection from an Airflow Connection object
# ---------------------------------------------------------------------------

def _get_smb_connection(conn_id: str = "smb_default") -> tuple[SMBConnection, dict]:
    conn = BaseHook.get_connection(conn_id)
    extra = json.loads(conn.extra or "{}")

    share_name = extra.get("share_name")
    if not share_name:
        raise ValueError(
            f"Connection '{conn_id}' extra must contain 'share_name'."
        )

    client_name = extra.get("client_name", "airflow-client")
    domain = extra.get("domain", "")

    smb_conn = SMBConnection(
        username=conn.login,
        password=conn.password,
        my_name=client_name,
        remote_name=conn.host,
        domain=domain,
        use_ntlm_v2=True,
        is_direct_tcp=True,
    )

    connected = smb_conn.connect(conn.host, 445, timeout=10)
    if not connected:
        raise ConnectionError(
            f"Could not connect to SMB server '{conn.host}' using connection '{conn_id}'."
        )

    return smb_conn, extra


# ---------------------------------------------------------------------------
# Helper: build a boto3 S3 client pointing at NooBaa
# ---------------------------------------------------------------------------

def _get_noobaa_client() -> tuple[object, str]:
    """Returns (boto3 S3 client, bucket_name)."""
    endpoint = Variable.get("NOOBAA_ENDPOINT")
    access_key = Variable.get("NOOBAA_ACCESS_KEY")
    secret_key = Variable.get("NOOBAA_SECRET_KEY")
    bucket = Variable.get("NOOBAA_BUCKET")

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        verify=False,  # set to CA bundle path in production
    )
    return client, bucket


# ---------------------------------------------------------------------------
# Task 1: List files on the SMB share — return value auto-pushed as XCom
# ---------------------------------------------------------------------------

def list_smb_files(smb_conn_id: str, **context) -> list[str]:
    """
    Lists all files under remote_path on the SMB share.
    The returned list is automatically pushed to XCom as 'return_value'.
    """
    smb_conn, extra = _get_smb_connection(smb_conn_id)
    share_name = extra["share_name"]
    remote_path = extra.get("remote_path", "/")

    try:
        entries = smb_conn.listPath(share_name, remote_path)
        files = [
            str(Path(remote_path) / e.filename)
            for e in entries
            if not e.isDirectory and not e.filename.startswith(".")
        ]
    finally:
        smb_conn.close()

    log.info("Found %d file(s) on SMB share path '%s'.", len(files), remote_path)
    for f in files:
        log.info("  • %s", f)

    # Return value is automatically stored as XCom 'return_value' in Airflow 3.x
    return files


# ---------------------------------------------------------------------------
# Task 2: Download files from SMB and upload to NooBaa
# ---------------------------------------------------------------------------

def transfer_files_to_noobaa(smb_conn_id: str, s3_prefix: str, **context) -> None:
    """
    Pulls the file list from XCom return_value, downloads each file from
    the SMB share into a temporary buffer, and streams it to NooBaa S3.
    """
    ti = context["ti"]
    # Pull from return_value (set automatically by Airflow 3.x from the return of list_smb_files)
    files: list[str] = ti.xcom_pull(task_ids="list_smb_files")

    if not files:
        log.warning("No files to transfer. Skipping.")
        return

    smb_conn, extra = _get_smb_connection(smb_conn_id)
    share_name = extra["share_name"]
    s3_client, bucket = _get_noobaa_client()

    transferred, failed = 0, 0

    try:
        for remote_file_path in files:
            file_name = Path(remote_file_path).name
            s3_key = f"{s3_prefix.rstrip('/')}/{file_name}" if s3_prefix else file_name

            log.info("Transferring '%s' → s3://%s/%s", remote_file_path, bucket, s3_key)

            try:
                buffer = io.BytesIO()
                smb_conn.retrieveFile(share_name, remote_file_path, buffer)
                buffer.seek(0)
                file_size = buffer.getbuffer().nbytes
                s3_client.upload_fileobj(buffer, bucket, s3_key)
                buffer.close()
                transferred += 1
                log.info("  ✓ Uploaded %s (%d bytes)", s3_key, file_size)

            except Exception as exc:  # noqa: BLE001
                failed += 1
                log.error("  ✗ Failed to transfer '%s': %s", remote_file_path, exc)

    finally:
        smb_conn.close()

    log.info(
        "Transfer complete. Succeeded: %d | Failed: %d | Total: %d",
        transferred,
        failed,
        len(files),
    )

    if failed:
        raise RuntimeError(
            f"{failed} file(s) failed to transfer. See logs for details."
        )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

SMB_CONN_ID = "smb_default"
S3_PREFIX = "folder/landing"

with DAG(
    dag_id="smb_to_noobaa",
    description="Collect files from an SMB share and upload them to an OpenShift NooBaa S3 bucket.",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["smb", "noobaa", "openshift", "s3"],
) as dag:

    task_list_files = PythonOperator(
        task_id="list_smb_files",
        python_callable=list_smb_files,
        op_kwargs={"smb_conn_id": SMB_CONN_ID},
    )

    task_transfer = PythonOperator(
        task_id="transfer_files_to_noobaa",
        python_callable=transfer_files_to_noobaa,
        op_kwargs={
            "smb_conn_id": SMB_CONN_ID,
            "s3_prefix": S3_PREFIX,
        },
    )

    task_list_files >> task_transfer
