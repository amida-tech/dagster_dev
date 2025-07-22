# utils/adls_sftp.py
import os
import io
import paramiko
from dagster import resource, InitResourceContext

@resource(description="SFTP client to Azure ADLS via Paramiko; key loaded from ADLSSFTP_PEM_CONTENT.")
def adls_sftp_resource(context: InitResourceContext):
    host     = os.getenv("SFTP_HOST")
    port     = int(os.getenv("SFTP_PORT", "22"))
    user     = os.getenv("SFTP_USERNAME")
    pem_body = os.getenv("ADLSSFTP_PEM_CONTENT")

    if not all([host, user, pem_body]):
        raise Exception("Must set SFTP_HOST, SFTP_USERNAME, and ADLSSFTP_PEM_CONTENT")

    context.log.info("Loading private key from ADLSSFTP_PEM_CONTENT")
    key_stream = io.StringIO(pem_body)

    # Load as RSAKey
    try:
        key = paramiko.RSAKey.from_private_key(key_stream)
    except paramiko.SSHException as e:
        raise Exception(f"Failed to parse private key from ADLSSFTP_PEM_CONTENT: {e}")
    finally:
        key_stream.close()

    # Open SSH/SFTP
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.load_system_host_keys()

    context.log.info(f"🔑 Connecting to {user}@{host}:{port}")
    client.connect(
        hostname=host,
        port=port,
        username=user,
        pkey=key,
        look_for_keys=False,
        allow_agent=False,
        timeout=10,
    )

    sftp = client.open_sftp()
    context.log.info("✅ SFTP session established")
    return sftp
