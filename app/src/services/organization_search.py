from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from django.db.models.signals import post_delete, post_save
from django.dispatch import receiver

from src.models.organization_models import Organization
from src.services.elasticsearch_client import (
    ElasticsearchNotConfigured,
    get_es_client,
)

logger = logging.getLogger(__name__)

ORGANIZATION_INDEX = "organizations"


ORGANIZATION_INDEX_MAPPING: Dict[str, Any] = {
    "mappings": {
        "properties": {
            "id": {"type": "integer"},
            "title": {"type": "text", "analyzer": "standard"},
            "address": {"type": "text", "analyzer": "standard"},
            "contact_phone": {"type": "keyword"},
            "work_schedule": {"type": "text"},
            "organization_type_id": {"type": "integer"},
            "organization_type_title": {"type": "keyword"},
            "is_verified": {"type": "boolean"},
            "create_at": {"type": "date"},
        }
    }
}


def _get_client_or_none():
    try:
        return get_es_client()
    except ElasticsearchNotConfigured as exc:  # pragma: no cover - зависит от окружения
        logger.warning("Elasticsearch not configured: %s", exc)
        return None


def ensure_organization_index():
    """
    Создает индекс для организаций при его отсутствии.
    Идempotентно, безопасно вызывать многократно.
    """
    es = _get_client_or_none()
    if es is None:
        return

    if not es.indices.exists(index=ORGANIZATION_INDEX):
        es.indices.create(index=ORGANIZATION_INDEX, **ORGANIZATION_INDEX_MAPPING)
        logger.info("Created Elasticsearch index '%s'", ORGANIZATION_INDEX)


def organization_to_document(org: Organization) -> Dict[str, Any]:
    """
    Преобразование Organization в документ для ES.
    """
    org_type = org.organization_type
    return {
        "id": org.id,
        "title": org.title,
        "address": org.address,
        "contact_phone": org.contact_phone,
        "work_schedule": org.work_schedule,
        "organization_type_id": org_type.id if org_type else None,
        "organization_type_title": getattr(org_type, "title", None),
        "is_verified": org.is_verified,
        "create_at": org.create_at,
    }


def index_organization(org: Organization) -> None:
    """
    Индексирует или переиндексирует организацию.
    """
    es = _get_client_or_none()
    if es is None or org.id is None:
        return

    ensure_organization_index()
    doc = organization_to_document(org)
    es.index(index=ORGANIZATION_INDEX, id=org.id, document=doc)
    logger.debug("Indexed organization %s in ES", org.id)


def delete_organization_from_index(org_id: int) -> None:
    """
    Удаляет организацию из индекса.
    """
    es = _get_client_or_none()
    if es is None:
        return

    try:
        es.delete(index=ORGANIZATION_INDEX, id=org_id)
        logger.debug("Deleted organization %s from ES index", org_id)
    except Exception as exc:  # pragma: no cover
        # Если документа нет или другая мягкая ошибка — логируем и идем дальше
        logger.warning(
            "Error deleting organization %s from ES index: %s", org_id, exc
        )


def search_organizations(
    query: str,
    *,
    is_verified: Optional[bool] = None,
    organization_type_id: Optional[int] = None,
    limit: int = 20,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    """
    Поиск организаций в ES.

    - query: строка для поиска по title и address
    - is_verified: фильтр по верификации
    - organization_type_id: фильтр по типу организации
    """
    es = _get_client_or_none()
    if es is None:
        return []

    must = []
    if query:
        must.append(
            {
                "multi_match": {
                    "query": query,
                    "fields": ["title^3", "address"],
                    "fuzziness": "AUTO",
                }
            }
        )

    filter_clauses: List[Dict[str, Any]] = []
    if is_verified is not None:
        filter_clauses.append({"term": {"is_verified": is_verified}})
    if organization_type_id is not None:
        filter_clauses.append({"term": {"organization_type_id": organization_type_id}})

    body: Dict[str, Any] = {
        "from": offset,
        "size": limit,
        "query": {
            "bool": {
                "must": must or {"match_all": {}},
                "filter": filter_clauses,
            }
        },
    }

    resp = es.search(index=ORGANIZATION_INDEX, body=body)
    hits = resp.get("hits", {}).get("hits", [])
    return [h.get("_source", {}) for h in hits]


@receiver(post_save, sender=Organization)
def organization_post_save(sender, instance: Organization, **kwargs):
    """
    Автоматическая индексация при сохранении Organization.
    """
    index_organization(instance)


@receiver(post_delete, sender=Organization)
def organization_post_delete(sender, instance: Organization, **kwargs):
    """
    Автоматическое удаление из индекса при удалении Organization.
    """
    if instance.id:
        delete_organization_from_index(instance.id)
