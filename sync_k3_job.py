
# K3 → starnet_products.volume sync (Python)
# -----------------------------------------
# Tarea programada en Windows que:
# 1) Lee IDs desde SQL Server (tabla dbo.starnet_products)
# 2) Llama al SDK Python de Kingdee (View) por cada Id
# 3) Actualiza el campo 'volume' en dbo.starnet_products

# Requisitos:
# - Python 3.10/3.11/3.13
# - pyodbc, python-dotenv
# - Microsoft ODBC Driver 17/18 for SQL Server
# - SDK Kingdee (.whl) instalado (kingdee.cdp.webapi.sdk-*.whl)

# # crear y activar venv (si aún no lo tienes)
# python -m venv .venv
# .\.venv\Scripts\Activate.ps1

# # instalar dependencias
# pip install python-dotenv pyodbc

# # instalar el SDK Kingdee desde tu .whl
# pip install "C:\Users\PCX\kingdee-sdk\SDK_Python3.0_V8.2.0\SDK_Python3.0_V8.2.0\python_sdk_v8.2.0\kingdee.cdp.webapi.sdk-8.2.0-py3-none-any.whl"

# Ejecución manual:
# > python sync_k3_job.py


from __future__ import annotations
import json
import os
import sys
import time
from typing import List, Optional, Any
from pathlib import Path
from dotenv import load_dotenv
import pyodbc

# SDK oficial Python
from k3cloud_webapi_sdk.main import K3CloudApiSdk

# =============== Configuración ==================
load_dotenv()

# SQL Server
SQL_SERVER   = os.getenv("SQL_SERVER",   "localhost")
SQL_DB       = os.getenv("SQL_DB",       "YourDB")
SQL_USER     = os.getenv("SQL_USER",     "sa")
SQL_PASSWORD = os.getenv("SQL_PASSWORD", "P@ssw0rd!")
# Driver ODBC recomendado: ODBC Driver 17 o 18
ODBC_DRIVER  = os.getenv("ODBC_DRIVER",  "ODBC Driver 18 for SQL Server")

# K3/Kingdee
K3_SERVER_URL = os.getenv("K3_SERVER_URL", "http://47.251.72.133:8090/K3Cloud/")
K3_ACCTID     = os.getenv("K3_ACCTID",     "")
K3_USERNAME   = os.getenv("K3_USERNAME",   "")
K3_APPID      = os.getenv("K3_APPID",      "")
K3_APPSEC     = os.getenv("K3_APPSEC",     "")
K3_LCID       = int(os.getenv("K3_LCID",   "2052"))
K3_ORGNUM     = os.getenv("K3_ORGNUM",     "100")
K3_CREATE_ORG = int(os.getenv("K3_CREATE_ORG", "0"))  # si necesitas enviar CreateOrgId en el body
K3_CONFIG_PATH = os.getenv("K3_CONFIG_PATH", "").strip()
K3_CONFIG_NODE = os.getenv("K3_CONFIG_NODE", "config").strip()


# Job
BATCH_SIZE    = int(os.getenv("BATCH_SIZE", "100"))
FORM_ID       = os.getenv("FORM_ID", "BD_MATERIAL")

SQL_SEL_IDS = os.getenv("SQL_SEL_IDS", """
SELECT TOP (?) id
FROM dbo.starnet_products
ORDER BY id
""")

# =============== Utilidades =====================

def sql_connect() -> pyodbc.Connection:
    conn_str = (
        f"DRIVER={{{ODBC_DRIVER}}};SERVER={SQL_SERVER};DATABASE={SQL_DB};"
        f"UID={SQL_USER};PWD={SQL_PASSWORD};TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, autocommit=False)

def k3_client() -> K3CloudApiSdk:
    sdk = K3CloudApiSdk(server_url=K3_SERVER_URL)
    if K3_CONFIG_PATH:
        conf_path = Path(os.path.expandvars(os.path.expanduser(K3_CONFIG_PATH))).resolve()
        if conf_path.exists():
            sdk.Init(config_path=str(conf_path), config_node=K3_CONFIG_NODE)
            return sdk
        else:
            raise FileNotFoundError(f"conf.ini no encontrado: {conf_path}")

    # Fallback: usar InitConfig con variables de entorno
    sdk.InitConfig(
        acct_id=K3_ACCTID,
        user_name=K3_USERNAME,
        app_id=K3_APPID,
        app_secret=K3_APPSEC,
        server_url=K3_SERVER_URL,
        lcid=K3_LCID,
        org_num=K3_ORGNUM,
    )
    return sdk


def fetch_ids(con: pyodbc.Connection, top: int) -> List[int]:
    with con.cursor() as cur:
        cur.execute(SQL_SEL_IDS, top)
        rows = cur.fetchall()
        return [int(r[0]) for r in rows]

def build_view_payload(k3_id: int) -> dict:
    return {
        "CreateOrgId": K3_CREATE_ORG,  # si tu entorno requiere este valor para View
        "Number": "",                  # consultamos por Id interno
        "Id": str(k3_id),
        "IsSortBySeq": "false",
    }

def extract_box_volume(parsed: dict) -> Optional[float]:
    """
    Extrae F_BOX_VOLUME desde el JSON de View.
    En tu respuesta real, viene en: parsed["Result"]["Result"]["F_BOX_VOLUME"]
    """
    try:
        inner = parsed.get("Result", {}).get("Result", {})
        if not isinstance(inner, dict):
            return None

        val = inner.get("F_BOX_VOLUME", None)
        if val is None:
            return None

        # Normaliza a float
        if isinstance(val, (int, float)):
            return float(val)
        if isinstance(val, str):
            return float(val.replace(",", "."))
        return None
    except Exception:
        return None

def update_product_volume(con: pyodbc.Connection, product_id: int, volume: Optional[float]) -> None:
    with con.cursor() as cur:
        cur.execute(
            "UPDATE dbo.starnet_products SET volume = ? WHERE id = ?",
            (volume, product_id)
        )

# =============== Proceso principal ==============

def process_batch() -> int:
    sdk = k3_client()
    ok, fail = 0, 0

    with sql_connect() as con:
        ids = fetch_ids(con, BATCH_SIZE)
        if not ids:
            print("[INFO] No hay IDs para procesar en starnet_products")
            return 0

        for pid in ids:
            try:
                payload = build_view_payload(pid)
                req_json = json.dumps(payload, ensure_ascii=False)
                resp = sdk.View(FORM_ID, req_json)  # string JSON
                parsed = json.loads(resp)

                status = parsed.get("Result", {}).get("ResponseStatus", {})
                success = status.get("IsSuccess", False)

                if not success:
                    # Mensaje de error
                    msg = ""
                    errs = status.get("Errors", []) or []
                    if errs:
                        msg = errs[0].get("Message", "")
                    print(f"[WARN] id={pid} View ERROR: {msg}")
                    fail += 1
                    continue

                volume = extract_box_volume(parsed)
                update_product_volume(con, pid, volume)
                print(f"[OK] id={pid} volume={volume!r}")
                ok += 1

            except Exception as ex:
                print(f"[ERR] id={pid} excepción: {ex}")
                fail += 1

        con.commit()

    print(f"[DONE] OK={ok} FAIL={fail}")
    return ok

# =============== Entry point ====================

if __name__ == "__main__":
    attempts = 1
    try:
        attempts = int(os.getenv("ATTEMPTS", "1"))
    except Exception:
        attempts = 1

    for i in range(1, attempts + 1):
        rc = process_batch()
        if rc:
            sys.exit(0)
        if i < attempts:
            time.sleep(5)

    sys.exit(0)
