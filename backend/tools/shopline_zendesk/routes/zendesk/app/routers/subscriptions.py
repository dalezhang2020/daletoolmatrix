from fastapi import APIRouter, Request, Query, HTTPException, Depends
from typing import Optional, List
from sqlalchemy.ext.asyncio import AsyncSession
from ..models.base import ApiResponse, SubscriptionTier
from ..database import get_db
from ..models.base import SubscriptionModel
from sqlalchemy import select
import logging
from datetime import datetime, timedelta
import uuid

from backend.tools.shopline_zendesk.db import binding_repo

logger = logging.getLogger(__name__)
router = APIRouter()


def _resolve_tenant_id(request: Request) -> str:
    """Resolve the active tenant/store id for subscription operations."""
    tenant_store_id = getattr(request.state, "tenant_store_id", None)
    if tenant_store_id:
        return str(tenant_store_id)

    zendesk_subdomain = request.headers.get("X-Zendesk-Subdomain")
    if not zendesk_subdomain:
        raise HTTPException(status_code=400, detail="Zendesk subdomain not found")

    requested_handle = (
        request.headers.get("X-Shopline-Handle")
        or request.query_params.get("shopline_handle")
    )

    if requested_handle:
        binding = binding_repo.get_binding_by_subdomain_and_handle(
            zendesk_subdomain,
            requested_handle,
        )
        if not binding:
            raise HTTPException(
                status_code=404,
                detail="Store is not linked to this Zendesk subdomain",
            )
        return str(binding["store_id"])

    bindings = binding_repo.list_bindings_by_subdomain(zendesk_subdomain)
    if not bindings:
        raise HTTPException(status_code=404, detail="Tenant not found")

    if len(bindings) == 1:
        return str(bindings[0]["store_id"])

    raise HTTPException(
        status_code=409,
        detail={
            "code": "STORE_SELECTION_REQUIRED",
            "error": (
                "Multiple Shopline stores are linked to this Zendesk account. "
                "Provide X-Shopline-Handle or shopline_handle."
            ),
            "available_stores": [binding["handle"] for binding in bindings],
        },
    )

# 订阅计划配置
SUBSCRIPTION_PLANS = {
    SubscriptionTier.BASIC: {
        "name": "基础版",
        "price_per_agent": 7.0,
        "features": [
            "核心订单读取",
            "客户匹配",
            "基础搜索功能",
            "订单历史查看"
        ]
    },
    SubscriptionTier.PROFESSIONAL: {
        "name": "专业版", 
        "price_per_agent": 13.0,
        "features": [
            "包含基础版所有功能",
            "高级搜索和筛选",
            "物流时间线",
            "客户详细信息",
            "订单状态跟踪"
        ]
    },
    SubscriptionTier.ENTERPRISE: {
        "name": "企业版",
        "price_per_agent": 21.0,
        "features": [
            "包含专业版所有功能",
            "订单操作功能",
            "退款处理",
            "订单取消",
            "创建订单",
            "高级分析"
        ]
    }
}

@router.get("/tiers", response_model=ApiResponse)
async def get_subscription_tiers():
    """获取订阅计划列表"""
    try:
        return ApiResponse(
            success=True,
            data=SUBSCRIPTION_PLANS
        )
    except Exception as e:
        logger.error(f"Error getting subscription tiers: {e}")
        return ApiResponse(
            success=False,
            error=str(e)
        )

@router.get("/current", response_model=ApiResponse)
async def get_current_subscription(
    request: Request,
    db: AsyncSession = Depends(get_db)
):
    """获取当前订阅信息"""
    try:
        tenant_id = _resolve_tenant_id(request)
        
        # 查询当前活跃的订阅
        subscription_result = await db.execute(
            select(SubscriptionModel).where(
                SubscriptionModel.tenant_id == tenant_id,
                SubscriptionModel.status == "active"
            )
        )
        subscription = subscription_result.scalar_one_or_none()
        
        if not subscription:
            return ApiResponse(
                success=True,
                data=None,
                message="No active subscription found"
            )
        
        # 构造响应数据
        subscription_data = {
            "id": subscription.id,
            "plan_type": subscription.plan_type,
            "agent_count": subscription.agent_count,
            "monthly_price": subscription.monthly_price,
            "status": subscription.status,
            "starts_at": subscription.starts_at,
            "expires_at": subscription.expires_at,
            "plan_details": SUBSCRIPTION_PLANS.get(subscription.plan_type, {})
        }
        
        return ApiResponse(
            success=True,
            data=subscription_data
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting current subscription: {e}")
        return ApiResponse(
            success=False,
            error=str(e)
        )

@router.post("/create", response_model=ApiResponse)
async def create_subscription(
    request: Request,
    plan_type: SubscriptionTier,
    agent_count: int = Query(..., ge=1),
    db: AsyncSession = Depends(get_db)
):
    """创建新订阅"""
    try:
        tenant_id = _resolve_tenant_id(request)
        
        # 计算价格
        plan_config = SUBSCRIPTION_PLANS.get(plan_type)
        if not plan_config:
            raise HTTPException(
                status_code=400,
                detail="Invalid subscription plan"
            )
        
        monthly_price = plan_config["price_per_agent"] * agent_count
        
        # 创建订阅
        subscription = SubscriptionModel(
            id=str(uuid.uuid4()),
            tenant_id=tenant_id,
            plan_type=plan_type.value,
            agent_count=agent_count,
            monthly_price=monthly_price,
            status="active",
            starts_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(days=30)
        )
        
        db.add(subscription)
        await db.commit()
        await db.refresh(subscription)
        
        return ApiResponse(
            success=True,
            data={
                "id": subscription.id,
                "plan_type": subscription.plan_type,
                "agent_count": subscription.agent_count,
                "monthly_price": subscription.monthly_price,
                "status": subscription.status,
                "starts_at": subscription.starts_at,
                "expires_at": subscription.expires_at
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating subscription: {e}")
        return ApiResponse(
            success=False,
            error=str(e)
        )

@router.put("/{subscription_id}/cancel", response_model=ApiResponse)
async def cancel_subscription(
    subscription_id: str,
    db: AsyncSession = Depends(get_db)
):
    """取消订阅"""
    try:
        # 查询订阅
        subscription_result = await db.execute(
            select(SubscriptionModel).where(SubscriptionModel.id == subscription_id)
        )
        subscription = subscription_result.scalar_one_or_none()
        
        if not subscription:
            raise HTTPException(
                status_code=404,
                detail="Subscription not found"
            )
        
        # 更新订阅状态
        subscription.status = "inactive"
        subscription.updated_at = datetime.utcnow()
        
        await db.commit()
        
        return ApiResponse(
            success=True,
            message="Subscription cancelled successfully"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error cancelling subscription: {e}")
        return ApiResponse(
            success=False,
            error=str(e)
        ) 
