from .audit_repository import AuditRepository
from .postgres_audit_repository import PostgresAuditRepository


def get_audit_repository(pg_config) -> AuditRepository:
    return PostgresAuditRepository(pg_config)
