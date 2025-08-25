
import socket

def get_primary_ip():
    try:
        # UDP connect doesn't send packets but lets OS choose the right interface
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except Exception:
        # fallback
        try:
            return socket.gethostbyname(socket.gethostname())
        except Exception:
            return "127.0.0.1"


JDBC_URL = (
    f"jdbc:sqlserver://{get_primary_ip()}:1433;"
    "databaseName=UberAnalysis;"
    "encrypt=true;"
    "trustServerCertificate=true;"
)
JDBC_PROPERTIES = {
    "user": "rinthya",
    "password": "StrongPassword123",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}