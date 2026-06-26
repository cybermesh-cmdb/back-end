import os
import logging
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from ipaddress import IPv4Address, IPv6Address
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator
import psycopg
from psycopg.rows import dict_row

# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Carrega primeiro backend/.env para evitar pegar credenciais antigas da raiz.
backend_env = Path(__file__).resolve().parents[1] / ".env"
root_env = Path(__file__).resolve().parents[2] / ".env"
if backend_env.exists():
    load_dotenv(dotenv_path=backend_env)
    logger.debug("[ENV] carregado arquivo: %s", backend_env)
elif root_env.exists():
    load_dotenv(dotenv_path=root_env)
    logger.debug("[ENV] carregado arquivo fallback: %s", root_env)
else:
    logger.debug("[ENV] nenhum .env encontrado em backend/ ou raiz")

def _resolve_pghost() -> str:
    """Se PGHOST for host.docker.internal, tenta conectar.
    Se a conexão falhar (fora do Docker), usa 127.0.0.1."""
    import socket
    host = os.getenv("PGHOST", "localhost")
    port = int(os.getenv("PGPORT", "5433"))
    logger.debug(f"[PGHOST] valor em .env: {host}")
    
    if host == "host.docker.internal":
        # Testa conectividade real, não apenas DNS
        try:
            sock = socket.create_connection((host, port), timeout=2)
            sock.close()
            logger.debug(f"[PGHOST] host.docker.internal está acessível, usando")
            return host
        except (socket.timeout, socket.gaierror, OSError) as e:
            logger.debug(f"[PGHOST] host.docker.internal não acessível ({type(e).__name__}), caindo para 127.0.0.1")
            return "127.0.0.1"
    
    # Se for localhost (fallback antigo), troca para 127.0.0.1 também
    if host == "localhost":
        logger.debug(f"[PGHOST] era localhost, mudando para 127.0.0.1")
        return "127.0.0.1"
    
    logger.debug(f"[PGHOST] usando como está: {host}")
    return host

resolved_host = _resolve_pghost()
logger.info(f"[DATABASE] Host resolvido: {resolved_host}")

DB_CONFIG = {
    "host": resolved_host,
    "port": int(os.getenv("PGPORT", "5433")),
    "dbname": os.getenv("PGDATABASE", "cybermesh_cmdb"),
    "user": os.getenv("PGUSER", "cmdb_cybermesh_user"),
    "password": os.getenv("PGPASSWORD", "cmdb_cybermesh_10"),
    "connect_timeout": 10,  # aumenta timeout para 10s
}

logger.info(f"[DATABASE] Config: host={DB_CONFIG['host']} port={DB_CONFIG['port']} db={DB_CONFIG['dbname']}")

STATUS_MAP = {
    "ativo": "active",
    "active": "active",
    "inativo": "disconnected",
    "inactive": "disconnected",
    "desconectado": "disconnected",
    "disconnected": "disconnected",
    "manutencao": "pending",
    "maintenance": "pending",
    "planejado": "pending",
    "planned": "pending",
    "pendente": "pending",
    "pending": "pending",
    "desativado": "never_connected",
    "retired": "never_connected",
    "never connected": "never_connected",
    "never-connected": "never_connected",
    "never_connected": "never_connected",
}

STATUS_OPTIONS = [
    {"value": "active", "label": "Active"},
    {"value": "disconnected", "label": "Disconnected"},
    {"value": "pending", "label": "Pending"},
    {"value": "never_connected", "label": "Never connected"},
]


class TagCreate(BaseModel):
    tag_name: str = Field(min_length=1, max_length=150)
    tag_type: str = Field(min_length=1, max_length=150)


class TenantCreate(BaseModel):
    customer_name: str = Field(min_length=1, max_length=150)
    customer_domain: str | None = Field(default=None, max_length=255)


class TenantContactCreate(BaseModel):
    contact_name: str = Field(min_length=1, max_length=150)
    contact_phone: str | None = Field(default=None, max_length=30)
    contact_mail: str = Field(min_length=1, max_length=150)
    contact_priority: int | None = Field(default=1, ge=1, le=10)
    notification_channel: str | None = Field(default=None, max_length=30)
    contact_type_id: int = Field(gt=0)


class AssetCreate(BaseModel):
    asset_name: str = Field(min_length=1, max_length=150)
    asset_type_id: int = Field(gt=0)
    asset_criticality_id: int = Field(gt=0)
    tenant_id: int = Field(gt=0)
    product_name_id: int | None = None
    product_vendor_id: int | None = None
    hostname_fqdn: str | None = Field(default=None, max_length=255)
    ip_address: str | None = None
    version_information: str | None = Field(default=None, max_length=255)
    mac_address: str | None = None
    operational_status: str | None = Field(default=None, max_length=50)
    product_name: str | None = Field(default=None, max_length=150)
    id_asset_external: str | None = Field(default=None, max_length=10)
    observations: str | None = None
    lec: int | None = Field(default=0, ge=0, le=1)
    tags: list[TagCreate] = Field(default_factory=list)

    @validator('asset_type_id', 'asset_criticality_id', 'product_name_id', 'product_vendor_id', pre=True)
    def convert_empty_string_to_none(cls, v):
        if isinstance(v, str):
            if v.strip() == '':
                return None
            try:
                return int(v)
            except ValueError:
                return None
        return v


class LECLogCreate(BaseModel):
    tenant_id: int = Field(gt=0)
    file_location: str | None = Field(default=None, max_length=500)
    last_event: str | None = None
    threshold_minutes: int = Field(default=60, ge=1)
    ip_lec: str | None = None
    device_name: str | None = Field(default=None, max_length=255)
    status_lec: str | None = Field(default=None, max_length=20)


class LECLogUpdate(BaseModel):
    file_location: str | None = Field(default=None, max_length=500)
    last_event: str | None = None
    threshold_minutes: int | None = Field(default=None, ge=1)
    ip_lec: str | None = None
    device_name: str | None = Field(default=None, max_length=255)
    status_lec: str | None = Field(default=None, max_length=20)


class LECLog(BaseModel):
    id_lec: int
    fk_tenant: int
    customer_name: str | None
    file_location: str | None
    last_event: datetime | None
    seconds_since: int | None
    minutes_since: int | None
    hours_since: int | None
    threshold_minutes: int | None
    ip_lec: str | IPv4Address | IPv6Address | None
    device_name: str | None
    status_lec: str | None
    status: str | None
    created_at: datetime | None
    updated_at: datetime | None


@contextmanager
def get_conn():
    logger.debug(f"[CONN] Tentando conectar com config: {DB_CONFIG}")
    try:
        conn = psycopg.connect(**DB_CONFIG, row_factory=dict_row)
        logger.debug(f"[CONN] Conexão bem-sucedida!")
        try:
            yield conn
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"[CONN] Erro ao conectar: {type(e).__name__}: {e}")
        raise


def normalize_status(value: str | None) -> str:
    if not value:
        return "active"
    return STATUS_MAP.get(value.strip().lower(), value.strip().lower())


def resolve_lec_status_column(cur: psycopg.Cursor) -> str:
    """Retorna a coluna de status existente em lec para compatibilidade de schema."""
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'lec'
          AND column_name IN ('status_lec', 'status')
        ORDER BY CASE WHEN column_name = 'status_lec' THEN 0 ELSE 1 END
        LIMIT 1
        """
    )
    row = cur.fetchone()
    if isinstance(row, dict):
        column_name = row.get("column_name")
        if column_name in {"status_lec", "status"}:
            return column_name
    return "status_lec"


def get_lec_logs_columns(cur: psycopg.Cursor) -> set[str]:
    """Retorna o conjunto de colunas existentes em lec."""
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'lec'
        """
    )
    rows = cur.fetchall() or []
    columns: set[str] = set()
    for row in rows:
        if isinstance(row, dict):
            name = row.get("column_name")
            if name:
                columns.add(name)
    return columns


def resolve_lec_ip_column(columns: set[str]) -> str | None:
    if "ip_host" in columns:
        return "ip_host"
    if "ip_lec" in columns:
        return "ip_lec"
    return None


def resolve_lec_ip_select(columns: set[str]) -> str:
    ip_column = resolve_lec_ip_column(columns)
    if ip_column:
        return f"ll.{ip_column} AS ip_lec"
    return "NULL::text AS ip_lec"


def sync_product_name_identity(cur: psycopg.Cursor) -> None:
    cur.execute(
        """
        SELECT setval(
            pg_get_serial_sequence('product_name', 'id_product'),
            COALESCE((SELECT MAX(id_product) FROM product_name), 1),
            true
        )
        """
    )


def sync_cmdb_assets_identity(cur: psycopg.Cursor) -> None:
    cur.execute(
        """
        SELECT setval(
            pg_get_serial_sequence('cmdb_assets', 'id_asset'),
            COALESCE((SELECT MAX(id_asset) FROM cmdb_assets), 1),
            true
        )
        """
    )


def sync_asset_tags_identity(cur: psycopg.Cursor) -> None:
    cur.execute(
        """
        SELECT setval(
            pg_get_serial_sequence('asset_tags', 'id_tag'),
            COALESCE((SELECT MAX(id_tag) FROM asset_tags), 1),
            true
        )
        """
    )


def resolve_product_id(cur: psycopg.Cursor, product_name_id: int | None, product_name: str | None) -> int:
    sync_product_name_identity(cur)

    if product_name_id is not None:
        return product_name_id

    normalized_product_name = (product_name or "").strip()
    if normalized_product_name:
        cur.execute(
            """
            INSERT INTO product_name (product_model)
            SELECT %s
            WHERE NOT EXISTS (SELECT 1 FROM product_name WHERE product_model = %s)
            """,
            (normalized_product_name, normalized_product_name),
        )
        cur.execute("SELECT id_product FROM product_name WHERE product_model = %s", (normalized_product_name,))
    else:
        cur.execute(
            """
            INSERT INTO product_name (product_model)
            SELECT 'generic'
            WHERE NOT EXISTS (SELECT 1 FROM product_name WHERE product_model = 'generic')
            """
        )
        cur.execute("SELECT id_product FROM product_name WHERE product_model = 'generic'")

    product_row = cur.fetchone()
    if not product_row:
        raise HTTPException(status_code=500, detail="Nao foi possivel resolver product_name_id")
    return product_row["id_product"]


def fetch_asset_with_catalogs(cur: psycopg.Cursor, asset_id: int) -> dict[str, Any]:
    cur.execute(
        """
        SELECT
            a.*, 
            t.type_name,
            c.level_name,
            p.product_model,
            pv.vendor_name,
            tn.customer_name AS tenant_name
        FROM cmdb_assets a
        LEFT JOIN asset_type t ON t.id_type = a.fk_asset_type
        LEFT JOIN asset_criticality c ON c.id_criticality = a.fk_asset_criticality
        LEFT JOIN product_name p ON p.id_product = a.fk_product_name
        LEFT JOIN product_vendor pv ON pv.id_vendor = a.fk_product_vendor
        LEFT JOIN tenants tn ON tn.id_tenants = a.fk_tenant
        WHERE a.id_asset = %s
        """,
        (asset_id,),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Ativo nao encontrado apos criacao")
    return row


def map_asset_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row.get("id_asset"),
        "id_asset_external": row.get("id_asset_external"),
        "asset_name": row.get("asset_name"),
        "asset_type_id": row.get("fk_asset_type"),
        "asset_type_name": row.get("type_name"),
        "asset_criticality_id": row.get("fk_asset_criticality"),
        "asset_criticality_name": row.get("level_name"),
        "tenant_id": row.get("fk_tenant"),
        "tenant_name": row.get("tenant_name"),
        "product_name_id": row.get("fk_product_name"),
        "product_vendor_id": row.get("fk_product_vendor"),
        "hostname_fqdn": row.get("hostname_fqdn"),
        "ip_address": str(row.get("ip_address")) if row.get("ip_address") is not None else None,
        "version_information": row.get("version_information"),
        "mac_address": str(row.get("mac_address")) if row.get("mac_address") is not None else None,
        "operational_status": row.get("operational_status"),
        "observations": row.get("observations"),
        "lec": int(row.get("lec", 0)) if row.get("lec") is not None else 0,
        "product_name": row.get("product_model") or row.get("product_name"),
        "product_vendor": row.get("vendor_name") or row.get("product_vendor"),
        "created_at": row.get("created_at"),
        "updated_at": row.get("last_updated") or row.get("updated_at"),
    }



app = FastAPI(title="CyberMesh CMDB Backend", version="1.0.0")

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8080",
        "http://127.0.0.1:8080",
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=3600,
)


@app.get("/api/health")
def health() -> dict[str, Any]:
    return {"ok": True, "service": "cybermesh-cmdb-backend-python"}


@app.get("/api/catalogs")
def catalogs() -> dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id_type AS id, type_name AS name FROM asset_type ORDER BY id_type")
            asset_types = cur.fetchall()

            cur.execute("SELECT id_criticality AS id, level_name AS name FROM asset_criticality ORDER BY id_criticality")
            criticalities = cur.fetchall()

            cur.execute("SELECT id_tenants AS id, customer_name AS name FROM tenants ORDER BY id_tenants")
            tenants = cur.fetchall()

            cur.execute("SELECT id_contact_type AS id, type_name AS name FROM contact_type ORDER BY id_contact_type")
            contact_types = cur.fetchall()

            cur.execute("SELECT id_vendor AS id, vendor_name AS name FROM product_vendor ORDER BY vendor_name")
            product_vendors = cur.fetchall()

            cur.execute("SELECT id_product AS id, product_model AS name FROM product_name ORDER BY product_model")
            product_names = cur.fetchall()

    return {
        "assetTypes": asset_types,
        "assetCriticalities": criticalities,
        "tenants": tenants,
        "contactTypes": contact_types,
        "productVendors": product_vendors,
        "productNames": product_names,
        "statuses": STATUS_OPTIONS,
    }


@app.get("/api/tenants")
def list_tenants() -> list[dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id_tenants AS id, customer_name AS name, customer_domain AS domain, created_at
                FROM tenants
                ORDER BY customer_name
                """
            )
            rows = cur.fetchall()
    return [dict(row) for row in rows]


@app.post("/api/tenants", status_code=201)
def create_tenant(payload: TenantCreate) -> dict[str, Any]:
    tenant_name = payload.customer_name.strip()
    if not tenant_name:
        raise HTTPException(status_code=400, detail="Nome do tenant nao pode ser vazio")

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO tenants (customer_name, customer_domain, created_at)
                    VALUES (%s, %s, NOW())
                    RETURNING id_tenants AS id, customer_name AS name, customer_domain AS domain, created_at
                    """,
                    (tenant_name, (payload.customer_domain.strip() if payload.customer_domain else None)),
                )
                row = cur.fetchone()
            conn.commit()
        return dict(row)
    except psycopg.Error as e:
        db_message = getattr(getattr(e, "diag", None), "message_primary", None) or str(e)
        raise HTTPException(status_code=400, detail=f"Erro ao salvar tenant: {db_message}")


@app.put("/api/tenants/{tenant_id}")
def update_tenant(tenant_id: int, payload: TenantCreate) -> dict[str, Any]:
    tenant_name = payload.customer_name.strip()
    if not tenant_name:
        raise HTTPException(status_code=400, detail="Nome do tenant nao pode ser vazio")

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE tenants
                    SET customer_name = %s, customer_domain = %s
                    WHERE id_tenants = %s
                    RETURNING id_tenants AS id, customer_name AS name, customer_domain AS domain, created_at
                    """,
                    (tenant_name, (payload.customer_domain.strip() if payload.customer_domain else None), tenant_id),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Tenant nao encontrado")
            conn.commit()
        return dict(row)
    except HTTPException:
        raise
    except psycopg.Error as e:
        db_message = getattr(getattr(e, "diag", None), "message_primary", None) or str(e)
        raise HTTPException(status_code=400, detail=f"Erro ao atualizar tenant: {db_message}")


@app.delete("/api/tenants/{tenant_id}", status_code=204)
def delete_tenant(tenant_id: int) -> Response:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id_tenants FROM tenants WHERE id_tenants = %s", (tenant_id,))
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="Tenant nao encontrado")
                cur.execute("DELETE FROM tenants WHERE id_tenants = %s", (tenant_id,))
            conn.commit()
        return Response(status_code=204)
    except HTTPException:
        raise
    except psycopg.errors.ForeignKeyViolation:
        raise HTTPException(
            status_code=400,
            detail="Nao foi possivel excluir tenant: existem ativos vinculados.",
        )
    except psycopg.Error as e:
        db_message = getattr(getattr(e, "diag", None), "message_primary", None) or str(e)
        raise HTTPException(status_code=400, detail=f"Erro ao excluir tenant: {db_message}")


@app.get("/api/tenants/{tenant_id}/contacts")
def list_tenant_contacts(tenant_id: int) -> list[dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id_tenants FROM tenants WHERE id_tenants = %s", (tenant_id,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Tenant nao encontrado")

            cur.execute(
                """
                SELECT
                    tc.id_tenant_contact,
                    tc.fk_tenant,
                    tc.contact_name,
                    tc.contact_phone,
                    tc.contact_mail,
                    tc.contact_priority,
                    tc.notification_channel,
                    tc.fk_contact_type,
                    ct.type_name AS contact_type_name,
                    tc.created_at
                FROM tenant_contacts tc
                JOIN contact_type ct ON ct.id_contact_type = tc.fk_contact_type
                WHERE tc.fk_tenant = %s
                ORDER BY tc.contact_priority NULLS LAST, tc.id_tenant_contact
                """,
                (tenant_id,),
            )
            rows = cur.fetchall()
    return [dict(row) for row in rows]


@app.post("/api/tenants/{tenant_id}/contacts", status_code=201)
def create_tenant_contact(tenant_id: int, payload: TenantContactCreate) -> dict[str, Any]:
    contact_name = payload.contact_name.strip()
    contact_mail = payload.contact_mail.strip()
    if not contact_name:
        raise HTTPException(status_code=400, detail="Nome do contato nao pode ser vazio")
    if not contact_mail:
        raise HTTPException(status_code=400, detail="Email do contato nao pode ser vazio")

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id_tenants FROM tenants WHERE id_tenants = %s", (tenant_id,))
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="Tenant nao encontrado")

                cur.execute(
                    """
                    INSERT INTO tenant_contacts (
                        contact_name,
                        contact_phone,
                        contact_mail,
                        contact_priority,
                        notification_channel,
                        fk_tenant,
                        fk_contact_type,
                        created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                    RETURNING
                        id_tenant_contact,
                        contact_name,
                        contact_phone,
                        contact_mail,
                        contact_priority,
                        notification_channel,
                        fk_tenant,
                        fk_contact_type,
                        created_at
                    """,
                    (
                        contact_name,
                        payload.contact_phone.strip() if payload.contact_phone else None,
                        contact_mail,
                        payload.contact_priority,
                        payload.notification_channel.strip() if payload.notification_channel else None,
                        tenant_id,
                        payload.contact_type_id,
                    ),
                )
                row = cur.fetchone()
            conn.commit()
        return dict(row)
    except HTTPException:
        raise
    except psycopg.Error as e:
        db_message = getattr(getattr(e, "diag", None), "message_primary", None) or str(e)
        raise HTTPException(status_code=400, detail=f"Erro ao salvar contato do tenant: {db_message}")


@app.get("/api/assets")
def list_assets(
    q: str | None = Query(default=None),
    q_field: str | None = Query(default="name"),
    asset_type_id: int | None = Query(default=None),
    asset_criticality_id: int | None = Query(default=None),
    tenant_id: int | None = Query(default=None),
    product_vendor_id: int | None = Query(default=None),
    lec: int | None = Query(default=None, ge=0, le=1),
    sort_by: str | None = Query(default=None),
    sort_dir: str | None = Query(default="asc"),
    quality_issue: str | None = Query(default=None),
    priority_focus: str | None = Query(default=None),
    criticality_band: str | None = Query(default=None),
    status: str | None = Query(default=None),
    attention_only: bool = Query(default=False),
    hostname: str | None = Query(default=None),
    ip_address: str | None = Query(default=None),
    product_name: str | None = Query(default=None),
    created_from: date | None = Query(default=None),
    created_to: date | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> list[dict[str, Any]] | dict[str, Any]:
    where_clauses: list[str] = ["a.fk_tenant IS NOT NULL"]
    params: list[Any] = []

    if q:
        normalized_q_field = str(q_field or "name").strip().lower()
        q_field_map = {
            "name": "a.asset_name",
            "ip": "a.ip_address::text",
            "hostname": "a.hostname_fqdn",
            "product": "p.product_model",
            "id_external": "a.id_asset_external",
        }
        q_column = q_field_map.get(normalized_q_field, "a.asset_name")
        where_clauses.append(f"{q_column} ILIKE %s")
        params.append(f"%{q.strip()}%")

    if asset_type_id is not None:
        where_clauses.append("a.fk_asset_type = %s")
        params.append(asset_type_id)

    if asset_criticality_id is not None:
        where_clauses.append("a.fk_asset_criticality = %s")
        params.append(asset_criticality_id)

    if tenant_id is not None:
        where_clauses.append("a.fk_tenant = %s")
        params.append(tenant_id)

    if product_vendor_id is not None:
        where_clauses.append("a.fk_product_vendor = %s")
        params.append(product_vendor_id)

    if lec is not None:
        where_clauses.append("a.lec = %s")
        params.append(lec)

    if quality_issue:
        quality_map = {
            "missing_criticality": "a.fk_asset_criticality IS NULL",
            "missing_tenant": "a.fk_tenant IS NULL",
            "missing_type": "a.fk_asset_type IS NULL",
            "missing_connectivity": "(a.ip_address IS NULL AND (a.hostname_fqdn IS NULL OR BTRIM(a.hostname_fqdn) = ''))",
        }
        quality_clause = quality_map.get(quality_issue.strip().lower())
        if quality_clause:
            where_clauses.append(quality_clause)

    if priority_focus:
        focus_map = {
            "critical_outage": "(a.fk_asset_criticality >= 9 AND a.operational_status <> 'active')",
            "pending": "a.operational_status = 'pending'",
            "disconnected_or_never_connected": "a.operational_status IN ('disconnected', 'never_connected')",
            "no_criticality": "a.fk_asset_criticality IS NULL",
            "no_connectivity": "(a.ip_address IS NULL AND (a.hostname_fqdn IS NULL OR BTRIM(a.hostname_fqdn) = ''))",
        }
        focus_clause = focus_map.get(priority_focus.strip().lower())
        if focus_clause:
            where_clauses.append(focus_clause)

    if criticality_band:
        band_map = {
            "critical": "a.fk_asset_criticality BETWEEN 15 AND 16",
            "high": "a.fk_asset_criticality BETWEEN 12 AND 14",
            "medium": "a.fk_asset_criticality BETWEEN 7 AND 11",
            "low": "a.fk_asset_criticality BETWEEN 1 AND 6",
        }
        band_clause = band_map.get(criticality_band.strip().lower())
        if band_clause:
            where_clauses.append(band_clause)


    if attention_only:
        where_clauses.append("a.operational_status IN ('disconnected', 'pending', 'never_connected')")

    if status:
        where_clauses.append("a.operational_status = %s")
        params.append(normalize_status(status))

    if hostname:
        where_clauses.append("a.hostname_fqdn ILIKE %s")
        params.append(f"%{hostname.strip()}%")

    if ip_address:
        where_clauses.append("a.ip_address::text ILIKE %s")
        params.append(f"%{ip_address.strip()}%")

    if product_name:
        where_clauses.append("p.product_model ILIKE %s")
        params.append(f"%{product_name.strip()}%")

    if created_from is not None:
        where_clauses.append("a.created_at >= %s")
        params.append(created_from)

    if created_to is not None:
        where_clauses.append("a.created_at < %s")
        params.append(created_to + timedelta(days=1))

    normalized_sort_dir = "DESC" if str(sort_dir or "asc").strip().lower() == "desc" else "ASC"
    sort_columns = {
        "name": "LOWER(COALESCE(a.asset_name, ''))",
        "idExternal": "CASE WHEN COALESCE(a.id_asset_external, '') ~ '^[0-9]+$' THEN LPAD(a.id_asset_external, 20, '0') ELSE LOWER(COALESCE(a.id_asset_external, '')) END",
        "type": "LOWER(COALESCE(t.type_name, ''))",
        "criticality": "COALESCE(a.fk_asset_criticality, -1)",
        "ipHost": "LOWER(COALESCE(a.ip_address::text, a.hostname_fqdn, ''))",
        "status": "LOWER(COALESCE(a.operational_status, ''))",
        "product": "LOWER(COALESCE(pv.vendor_name, '') || ' ' || COALESCE(p.product_model, ''))",
        "createdAt": "COALESCE(a.created_at, NOW())",
    }
    sort_expr = sort_columns.get(str(sort_by or "").strip(), "a.created_at")
    order_sql = f"ORDER BY {sort_expr} {normalized_sort_dir}, a.id_asset ASC"

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT COUNT(*) AS total
                FROM cmdb_assets a
                LEFT JOIN product_name p ON p.id_product = a.fk_product_name
                {where_sql}
                """,
                params,
            )
            total_row = cur.fetchone()
            total = int(total_row["total"]) if total_row else 0

            query_params = list(params)
            pagination_sql = ""
            if limit is not None:
                pagination_sql = " LIMIT %s OFFSET %s"
                query_params.extend([limit, offset])

            cur.execute(
                f"""
                SELECT
                    a.*,
                    t.type_name,
                    c.level_name,
                    p.product_model,
                    pv.vendor_name,
                    tn.customer_name AS tenant_name
                FROM cmdb_assets a
                LEFT JOIN asset_type t ON t.id_type = a.fk_asset_type
                LEFT JOIN asset_criticality c ON c.id_criticality = a.fk_asset_criticality
                LEFT JOIN product_name p ON p.id_product = a.fk_product_name
                LEFT JOIN product_vendor pv ON pv.id_vendor = a.fk_product_vendor
                LEFT JOIN tenants tn ON tn.id_tenants = a.fk_tenant
                {where_sql}
                {order_sql}
                {pagination_sql}
                """,
                query_params,
            )
            rows = cur.fetchall()

    items = [map_asset_row(row) for row in rows]
    if limit is None:
        return items

    return {
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@app.post("/api/assets", status_code=201)
def create_asset(payload: AssetCreate) -> dict[str, Any]:
    try:
        status = normalize_status(payload.operational_status)
        asset_name = payload.asset_name.strip()
        if not asset_name:
            raise HTTPException(status_code=400, detail="Nome do ativo nao pode ser vazio")

        with get_conn() as conn:
            with conn.cursor() as cur:
                sync_cmdb_assets_identity(cur)
                fk_product_id = resolve_product_id(cur, payload.product_name_id, payload.product_name)

                cur.execute(
                    """
                    INSERT INTO cmdb_assets (
                        id_asset_external,
                        fk_tenant,
                        fk_asset_type,
                        fk_product_name,
                        fk_product_vendor,
                        fk_asset_criticality,
                        asset_name,
                        hostname_fqdn,
                        ip_address,
                        version_information,
                        mac_address,
                        operational_status,
                        observations,
                        lec,
                        created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    RETURNING *
                    """,
                    (
                        payload.id_asset_external,
                        payload.tenant_id,
                        payload.asset_type_id,
                        fk_product_id,
                        payload.product_vendor_id,
                        payload.asset_criticality_id,
                        asset_name,
                        payload.hostname_fqdn,
                        payload.ip_address,
                        payload.version_information,
                        payload.mac_address,
                        status,
                        payload.observations,
                        payload.lec or 0,
                    ),
                )
                row = cur.fetchone()

                for tag in payload.tags:
                    tag_name = tag.tag_name.strip()
                    tag_type = tag.tag_type.strip()
                    if not tag_name or not tag_type:
                        continue
                    cur.execute(
                        """
                        INSERT INTO asset_tags (tag_name, tag_type, fk_asset)
                        VALUES (%s, %s, %s)
                        RETURNING id_tag, tag_name, tag_type, created_at
                        """,
                        (tag_name, tag_type, row["id_asset"]),
                    )

                full_row = fetch_asset_with_catalogs(cur, row["id_asset"])

            conn.commit()

        return map_asset_row(full_row)
    except HTTPException:
        raise
    except psycopg.Error as e:
        db_message = getattr(getattr(e, "diag", None), "message_primary", None) or str(e)
        raise HTTPException(status_code=400, detail=f"Erro de dados ao salvar ativo: {db_message}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao salvar ativo: {str(e)}")


@app.put("/api/assets/{asset_id}", status_code=200)
def update_asset(asset_id: int, payload: AssetCreate) -> dict[str, Any]:
    try:
        status = normalize_status(payload.operational_status)
        asset_name = payload.asset_name.strip()
        if not asset_name:
            raise HTTPException(status_code=400, detail="Nome do ativo nao pode ser vazio")

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id_asset FROM cmdb_assets WHERE id_asset = %s", (asset_id,))
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="Ativo nao encontrado")

                fk_product_id = resolve_product_id(cur, payload.product_name_id, payload.product_name)

                cur.execute(
                    """
                    UPDATE cmdb_assets
                    SET
                        id_asset_external = %s,
                        fk_tenant = %s,
                        fk_asset_type = %s,
                        fk_product_name = %s,
                        fk_product_vendor = %s,
                        fk_asset_criticality = %s,
                        asset_name = %s,
                        hostname_fqdn = %s,
                        ip_address = %s,
                        version_information = %s,
                        mac_address = %s,
                        operational_status = %s,
                        observations = %s,
                        lec = %s,
                        last_updated = NOW()
                    WHERE id_asset = %s
                    RETURNING *
                    """,
                    (
                        payload.id_asset_external,
                        payload.tenant_id,
                        payload.asset_type_id,
                        fk_product_id,
                        payload.product_vendor_id,
                        payload.asset_criticality_id,
                        asset_name,
                        payload.hostname_fqdn,
                        payload.ip_address,
                        payload.version_information,
                        payload.mac_address,
                        status,
                        payload.observations,
                        payload.lec or 0,
                        asset_id,
                    ),
                )
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Ativo nao encontrado")

                full_row = fetch_asset_with_catalogs(cur, row["id_asset"])

            conn.commit()

        return map_asset_row(full_row)
    except HTTPException:
        raise
    except psycopg.Error as e:
        db_message = getattr(getattr(e, "diag", None), "message_primary", None) or str(e)
        raise HTTPException(status_code=400, detail=f"Erro de dados ao atualizar ativo: {db_message}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao atualizar ativo: {str(e)}")


@app.delete("/api/assets/{asset_id}", status_code=204)
def delete_asset(asset_id: int) -> Response:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM cmdb_assets WHERE id_asset = %s", (asset_id,))
            deleted = cur.rowcount
        conn.commit()

    if deleted == 0:
        raise HTTPException(status_code=404, detail="Ativo nao encontrado")
    return Response(status_code=204)


@app.get("/api/assets/{asset_id}/tags")
def list_tags(asset_id: int) -> list[dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id_tag, tag_name, tag_type, created_at FROM asset_tags WHERE fk_asset = %s ORDER BY id_tag",
                (asset_id,),
            )
            rows = cur.fetchall()
    return [dict(row) for row in rows]


@app.post("/api/assets/{asset_id}/tags", status_code=201)
def create_tag(asset_id: int, payload: TagCreate) -> dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id_asset FROM cmdb_assets WHERE id_asset = %s", (asset_id,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Ativo nao encontrado")
            sync_asset_tags_identity(cur)
            cur.execute(
                """
                INSERT INTO asset_tags (tag_name, tag_type, fk_asset)
                VALUES (%s, %s, %s)
                RETURNING id_tag, tag_name, tag_type, created_at
                """,
                (payload.tag_name.strip(), payload.tag_type.strip(), asset_id),
            )
            row = cur.fetchone()
        conn.commit()
    return dict(row)


@app.delete("/api/tags/{tag_id}", status_code=204)
def delete_tag(tag_id: int) -> Response:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM asset_tags WHERE id_tag = %s", (tag_id,))
            deleted = cur.rowcount
        conn.commit()
    if deleted == 0:
        raise HTTPException(status_code=404, detail="Tag nao encontrada")
    return Response(status_code=204)


@app.put("/api/tenants/{tenant_id}/contacts/{contact_id}", status_code=200)
def update_tenant_contact(tenant_id: int, contact_id: int, payload: TenantContactCreate) -> dict[str, Any]:
    contact_name = payload.contact_name.strip()
    contact_mail = payload.contact_mail.strip()
    if not contact_name:
        raise HTTPException(status_code=400, detail="Nome do contato nao pode ser vazio")
    if not contact_mail:
        raise HTTPException(status_code=400, detail="Email do contato nao pode ser vazio")

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id_tenants FROM tenants WHERE id_tenants = %s", (tenant_id,))
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="Tenant nao encontrado")

                cur.execute(
                    "SELECT id_tenant_contact FROM tenant_contacts WHERE id_tenant_contact = %s AND fk_tenant = %s",
                    (contact_id, tenant_id)
                )
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="Contato nao encontrado para este tenant")

                cur.execute(
                    """
                    UPDATE tenant_contacts
                    SET
                        contact_name = %s,
                        contact_phone = %s,
                        contact_mail = %s,
                        contact_priority = %s,
                        notification_channel = %s,
                        fk_contact_type = %s
                    WHERE id_tenant_contact = %s
                    """,
                    (
                        contact_name,
                        payload.contact_phone,
                        contact_mail,
                        payload.contact_priority,
                        payload.notification_channel,
                        payload.contact_type_id,
                        contact_id
                    )
                )
            conn.commit()

            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT
                        tc.id_tenant_contact,
                        tc.fk_tenant,
                        tc.contact_name,
                        tc.contact_phone,
                        tc.contact_mail,
                        tc.contact_priority,
                        tc.notification_channel,
                        tc.fk_tenant,
                        tc.fk_contact_type,
                        ct.type_name AS contact_type_name
                    FROM tenant_contacts tc
                    LEFT JOIN contact_type ct ON tc.fk_contact_type = ct.id_contact_type
                    WHERE tc.id_tenant_contact = %s
                    """,
                    (contact_id,)
                )
                row = cur.fetchone()
        return dict(row)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao atualizar contato: {str(e)}")


@app.delete("/api/tenants/{tenant_id}/contacts/{contact_id}", status_code=204)
def delete_tenant_contact(tenant_id: int, contact_id: int) -> Response:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id_tenants FROM tenants WHERE id_tenants = %s", (tenant_id,))
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="Tenant nao encontrado")

                cur.execute(
                    "DELETE FROM tenant_contacts WHERE id_tenant_contact = %s AND fk_tenant = %s",
                    (contact_id, tenant_id)
                )
                deleted = cur.rowcount
            conn.commit()
        if deleted == 0:
            raise HTTPException(status_code=404, detail="Contato nao encontrado para este tenant")
        return Response(status_code=204)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao deletar contato: {str(e)}")


# ============================================================================
# Endpoints de Cobertura de Monitoramento
# ============================================================================

@app.get("/api/coverage/by-tenant")
def get_coverage_by_tenant() -> list[dict[str, Any]]:
    """Retorna cobertura de monitoramento por tenant: agentless (LEC) vs com agente (Wazuh)"""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        tn.id_tenants,
                        tn.customer_name,
                        COUNT(CASE WHEN a.lec = 1 THEN 1 END)::int AS agentless_count,
                        COUNT(CASE WHEN a.lec = 0 OR a.lec IS NULL THEN 1 END)::int AS agent_count,
                        COUNT(a.id_asset)::int AS total_count,
                        COUNT(CASE WHEN a.operational_status = 'active' THEN 1 END)::int AS active_count,
                        COUNT(CASE WHEN a.operational_status != 'active' THEN 1 END)::int AS attention_count
                    FROM tenants tn
                    LEFT JOIN cmdb_assets a ON a.fk_tenant = tn.id_tenants
                    GROUP BY tn.id_tenants, tn.customer_name
                    ORDER BY tn.customer_name
                    """
                )
                rows = cur.fetchall()
        
        return [dict(row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar cobertura: {str(e)}")


# ============================================================================
# Endpoints de LEC (Log Event Collection)
# ============================================================================

@app.get("/api/lec-logs")
def list_lec_logs(
    tenant_id: int | None = None,
    status: str | None = None,
    status_lec: str | None = None,
) -> list[LECLog]:
    """Lista todos os logs LEC com filtros opcionais"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            lec_columns = get_lec_logs_columns(cur)
            status_column = resolve_lec_status_column(cur)
            ip_select = resolve_lec_ip_select(lec_columns)
            device_select = "ll.device_name" if "device_name" in lec_columns else "NULL::text AS device_name"
            query = """
                SELECT
                    ll.id_lec,
                    ll.fk_tenant,
                    t.customer_name,
                    ll.file_location,
                    ll.last_event,
                    ll.seconds_since,
                    ll.minutes_since,
                    ll.hours_since,
                    ll.threshold_minutes,
                    {ip_select},
                    {device_select},
                    ll.{status_column} AS status_lec,
                    ll.{status_column} AS status,
                    ll.created_at,
                    ll.updated_at
                FROM lec ll
                LEFT JOIN tenants t ON ll.fk_tenant = t.id_tenants
                WHERE 1=1
            """.format(status_column=status_column, ip_select=ip_select, device_select=device_select)
            params = []
            
            if tenant_id:
                query += " AND ll.fk_tenant = %s"
                params.append(tenant_id)
            
            resolved_status_lec = status_lec or status
            if resolved_status_lec:
                query += f" AND ll.{status_column} = %s"
                params.append(resolved_status_lec)
            
            query += " ORDER BY ll.updated_at DESC"
            
            cur.execute(query, params)
            rows = cur.fetchall()
    
    return [dict(row) for row in rows]


@app.get("/api/lec-logs/{lec_id}")
def get_lec_log(lec_id: int) -> LECLog:
    """Obtém detalhes de um log LEC específico"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            lec_columns = get_lec_logs_columns(cur)
            status_column = resolve_lec_status_column(cur)
            ip_select = resolve_lec_ip_select(lec_columns)
            device_select = "ll.device_name" if "device_name" in lec_columns else "NULL::text AS device_name"
            cur.execute(
                """
                SELECT
                    ll.id_lec,
                    ll.fk_tenant,
                    t.customer_name,
                    ll.file_location,
                    ll.last_event,
                    ll.seconds_since,
                    ll.minutes_since,
                    ll.hours_since,
                    ll.threshold_minutes,
                    {ip_select},
                    {device_select},
                    ll.{status_column} AS status_lec,
                    ll.{status_column} AS status,
                    ll.created_at,
                    ll.updated_at
                FROM lec ll
                LEFT JOIN tenants t ON ll.fk_tenant = t.id_tenants
                WHERE ll.id_lec = %s
                """.format(ip_select=ip_select, status_column=status_column, device_select=device_select),
                (lec_id,)
            )
            row = cur.fetchone()
    
    if not row:
        raise HTTPException(status_code=404, detail="Log LEC nao encontrado")
    return dict(row)


@app.post("/api/lec-logs", status_code=201)
def create_lec_log(payload: LECLogCreate) -> LECLog:
    """Cria um novo log LEC"""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                lec_columns = get_lec_logs_columns(cur)
                status_column = resolve_lec_status_column(cur)
                ip_column = resolve_lec_ip_column(lec_columns)
                has_ip_column = ip_column is not None
                has_device_column = "device_name" in lec_columns
                # Verifica se o tenant existe
                cur.execute("SELECT id_tenants FROM tenants WHERE id_tenants = %s", (payload.tenant_id,))
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="Tenant nao encontrado")
                
                # Insere o log LEC
                insert_columns = ["fk_tenant", "file_location", "last_event", "threshold_minutes", status_column]
                insert_values = [
                    payload.tenant_id,
                    payload.file_location,
                    payload.last_event or datetime.now(UTC),
                    payload.threshold_minutes,
                    payload.status_lec,
                ]
                if has_ip_column:
                    insert_columns.insert(4, ip_column)
                    insert_values.insert(4, payload.ip_lec)
                if has_device_column:
                    status_idx = insert_columns.index(status_column)
                    insert_columns.insert(status_idx, "device_name")
                    insert_values.insert(status_idx, payload.device_name)

                returning_ip = f"{ip_column} AS ip_lec" if has_ip_column else "NULL::text AS ip_lec"
                returning_device = "device_name" if has_device_column else "NULL::text AS device_name"
                cur.execute(
                    f"""
                    INSERT INTO lec ({', '.join(insert_columns)})
                    VALUES ({', '.join(['%s'] * len(insert_columns))})
                    RETURNING
                        id_lec, fk_tenant, file_location, last_event,
                        seconds_since, minutes_since, hours_since, threshold_minutes,
                        {returning_ip}, {returning_device},
                        {status_column} AS status_lec,
                        {status_column} AS status,
                        created_at, updated_at
                    """,
                    tuple(insert_values)
                )
                row = cur.fetchone()
            conn.commit()
        
        # Busca o registro com dados do tenant e asset
        with get_conn() as conn:
            with conn.cursor() as cur:
                lec_columns = get_lec_logs_columns(cur)
                status_column = resolve_lec_status_column(cur)
                ip_column = resolve_lec_ip_column(lec_columns)
                has_ip_column = ip_column is not None
                has_device_column = "device_name" in lec_columns
                ip_select = resolve_lec_ip_select(lec_columns)
                device_select = "ll.device_name" if has_device_column else "NULL::text AS device_name"
                cur.execute(
                    """
                    SELECT
                        ll.id_lec,
                        ll.fk_tenant,
                        t.customer_name,
                        ll.file_location,
                        ll.last_event,
                        ll.seconds_since,
                        ll.minutes_since,
                        ll.hours_since,
                        ll.threshold_minutes,
                        {ip_select},
                        {device_select},
                        ll.{status_column} AS status_lec,
                        ll.{status_column} AS status,
                        ll.created_at,
                        ll.updated_at
                    FROM lec ll
                    LEFT JOIN tenants t ON ll.fk_tenant = t.id_tenants
                    WHERE ll.id_lec = %s
                    """.format(ip_select=ip_select, status_column=status_column, device_select=device_select),
                    (row["id_lec"],)
                )
                result = cur.fetchone()
        
        return dict(result)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao criar log LEC: {str(e)}")


@app.put("/api/lec-logs/{lec_id}")
def update_lec_log(lec_id: int, payload: LECLogUpdate) -> LECLog:
    """Atualiza um log LEC existente"""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                lec_columns = get_lec_logs_columns(cur)
                status_column = resolve_lec_status_column(cur)
                ip_column = resolve_lec_ip_column(lec_columns)
                has_ip_column = ip_column is not None
                has_device_column = "device_name" in lec_columns
                ip_select = resolve_lec_ip_select(lec_columns)
                device_select = "ll.device_name" if has_device_column else "NULL::text AS device_name"
                # Verifica se o log existe
                cur.execute("SELECT id_lec FROM lec WHERE id_lec = %s", (lec_id,))
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="Log LEC nao encontrado")
                
                # Atualiza apenas os campos fornecidos
                updates = []
                params = []
                
                if payload.file_location is not None:
                    updates.append("file_location = %s")
                    params.append(payload.file_location)
                
                if payload.last_event is not None:
                    updates.append("last_event = %s")
                    params.append(payload.last_event)
                
                if payload.threshold_minutes is not None:
                    updates.append("threshold_minutes = %s")
                    params.append(payload.threshold_minutes)

                if payload.ip_lec is not None and has_ip_column:
                    updates.append(f"{ip_column} = %s")
                    params.append(payload.ip_lec)

                if payload.device_name is not None and has_device_column:
                    updates.append("device_name = %s")
                    params.append(payload.device_name)

                if payload.status_lec is not None:
                    updates.append(f"{status_column} = %s")
                    params.append(payload.status_lec)
                
                if updates:
                    updates.append("updated_at = NOW()")
                    params.append(lec_id)
                    
                    cur.execute(
                        f"UPDATE lec SET {', '.join(updates)} WHERE id_lec = %s",
                        params
                    )
                    conn.commit()
            
            # Retorna o registro atualizado
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        ll.id_lec,
                        ll.fk_tenant,
                        t.customer_name,
                        ll.file_location,
                        ll.last_event,
                        ll.seconds_since,
                        ll.minutes_since,
                        ll.hours_since,
                        ll.threshold_minutes,
                        {ip_select},
                        {device_select},
                        ll.{status_column} AS status_lec,
                        ll.{status_column} AS status,
                        ll.created_at,
                        ll.updated_at
                    FROM lec ll
                    LEFT JOIN tenants t ON ll.fk_tenant = t.id_tenants
                    WHERE ll.id_lec = %s
                    """.format(ip_select=ip_select, status_column=status_column, device_select=device_select),
                    (lec_id,)
                )
                result = cur.fetchone()
        
        return dict(result)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao atualizar log LEC: {str(e)}")


@app.delete("/api/lec-logs/{lec_id}", status_code=204)
def delete_lec_log(lec_id: int) -> Response:
    """Deleta um log LEC"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM lec WHERE id_lec = %s", (lec_id,))
            deleted = cur.rowcount
        conn.commit()
    
    if deleted == 0:
        raise HTTPException(status_code=404, detail="Log LEC nao encontrado")
    return Response(status_code=204)

