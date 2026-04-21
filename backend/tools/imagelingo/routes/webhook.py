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
