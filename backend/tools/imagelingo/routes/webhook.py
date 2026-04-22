import hashlib
import hmac
import os

from fastapi import APIRouter, Header, HTTPException, Request

router = APIRouter()


def verify_webhook(body: bytes, signature: str) -> bool:
    secret = os.getenv("SHOPLINE_APP_SECRET", "")
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


@router.post("/")
async def webhook(
    request: Request,
    x_shopline_hmac_sha256: str = Header(default=""),
):
    body = await request.body()
    if not verify_webhook(body, x_shopline_hmac_sha256):
        raise HTTPException(status_code=401, detail="Invalid webhook signature")

    payload = await request.json()
    event_id = payload.get("id") or payload.get("event_id")

    from backend.db.connection import get_connection
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM imagelingo.webhook_events WHERE event_id = %s",
                (event_id,),
            )
            if cur.fetchone():
                return {"status": "already_processed"}
            cur.execute(
                "INSERT INTO imagelingo.webhook_events (event_id) VALUES (%s) ON CONFLICT DO NOTHING",
                (event_id,),
            )
        conn.commit()

    return {"status": "ok"}


@router.post("/gdpr/customers-data-erasure")
async def customers_data_erasure(request: Request):
    """GDPR: Customer data erasure request.
    ImageLingo does not store customer PII, so we acknowledge and return OK.
    """
    return {"status": "ok", "message": "No customer data stored"}


@router.post("/gdpr/shop-data-erasure")
async def shop_data_erasure(request: Request):
    """GDPR: Shop data erasure request.
    When a merchant uninstalls, delete their store data.
    """
    payload = await request.json()
    handle = payload.get("domain", "").replace(".myshopline.com", "")
    if handle:
        from backend.db.connection import get_connection
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM imagelingo.stores WHERE handle = %s", (handle,))
                row = cur.fetchone()
                if row:
                    store_id = str(row[0])
                    cur.execute("DELETE FROM imagelingo.usage_logs WHERE store_id = %s", (store_id,))
                    cur.execute("DELETE FROM imagelingo.subscriptions WHERE store_id = %s", (store_id,))
                    cur.execute(
                        """DELETE FROM imagelingo.translated_images
                           WHERE job_id IN (SELECT id FROM imagelingo.translation_jobs WHERE store_id = %s)""",
                        (store_id,),
                    )
                    cur.execute("DELETE FROM imagelingo.translation_jobs WHERE store_id = %s", (store_id,))
                    cur.execute("DELETE FROM imagelingo.stores WHERE id = %s", (store_id,))
            conn.commit()
    return {"status": "ok", "message": "Shop data erased"}
