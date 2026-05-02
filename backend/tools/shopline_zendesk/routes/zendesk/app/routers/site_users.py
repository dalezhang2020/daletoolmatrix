from fastapi import APIRouter, HTTPException, Depends
from typing import Optional, List
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
from ..models.base import ApiResponse
from ..models.user import SiteUserModel, UserStripeSubscription
from ..database import get_db
from pydantic import BaseModel, EmailStr
import logging
import uuid
from datetime import datetime, timedelta
import bcrypt
import secrets
from ..services.email_service import email_service

logger = logging.getLogger(__name__)
router = APIRouter()

# Request Models
class UserCreate(BaseModel):
    email: EmailStr
    name: Optional[str] = None
    password: Optional[str] = None  # For email/password registration
    google_id: Optional[str] = None
    image_url: Optional[str] = None

class UserUpdate(BaseModel):
    name: Optional[str] = None
    image_url: Optional[str] = None
    stripe_customer_id: Optional[str] = None

class CompanyInfoUpdate(BaseModel):
    company_name: Optional[str] = None
    company_address: Optional[str] = None
    company_city: Optional[str] = None
    company_state: Optional[str] = None
    company_postal_code: Optional[str] = None
    company_country: Optional[str] = None

class TenantBinding(BaseModel):
    zendesk_subdomain: str
    shopline_handle: Optional[str] = None
    is_owner: bool = False

class LoginRequest(BaseModel):
    email: EmailStr
    password: Optional[str] = None

class ResetPasswordRequest(BaseModel):
    email: EmailStr

class ConfirmResetRequest(BaseModel):
    token: str
    new_password: str

class VerifyEmailRequest(BaseModel):
    token: str

class SetPasswordRequest(BaseModel):
    password: str

class SubscriptionCreate(BaseModel):
    stripe_subscription_id: str
    stripe_customer_id: str
    plan_name: str
    status: str
    current_period_start: datetime
    current_period_end: datetime
    amount: int
    currency: str = "usd"
    interval: str = "month"

class SubscriptionUpdate(BaseModel):
    status: Optional[str] = None
    current_period_end: Optional[datetime] = None
    cancel_at_period_end: Optional[bool] = None
    amount: Optional[int] = None
    plan_name: Optional[str] = None

@router.post("/register", response_model=ApiResponse)
async def register_user(
    user_data: UserCreate,
    db: AsyncSession = Depends(get_db)
):
    """注册新用户（网站用户）"""
    try:
        # 检查用户是否已存在
        existing_user = await db.execute(
            select(SiteUserModel).where(SiteUserModel.email == user_data.email)
        )
        if existing_user.scalar_one_or_none():
            return ApiResponse(
                success=False,
                error="User with this email already exists"
            )
        
        # 处理密码哈希
        password_hash = None
        if user_data.password:
            password_hash = bcrypt.hashpw(user_data.password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        # 生成邮箱验证令牌
        verification_token = None
        verification_expires = None
        if not user_data.google_id and user_data.password:
            verification_token = secrets.token_urlsafe(32)
            verification_expires = datetime.utcnow() + timedelta(hours=24)
        
        # 创建新用户
        new_user = SiteUserModel(
            id=str(uuid.uuid4()),
            email=user_data.email,
            name=user_data.name,
            password_hash=password_hash,
            google_id=user_data.google_id,
            image_url=user_data.image_url,
            email_verification_token=verification_token,
            email_verification_expires=verification_expires,
            is_verified=bool(user_data.google_id)  # Google 登录自动验证
        )
        
        db.add(new_user)
        await db.commit()
        await db.refresh(new_user)
        
        # 发送验证邮件（仅限密码注册用户）
        if verification_token:
            await email_service.send_verification_email(
                to_email=new_user.email,
                verification_token=verification_token,
                user_name=new_user.name
            )
        
        return ApiResponse(
            success=True,
            data=new_user.to_dict(),
            message="User registered successfully"
        )
    except Exception as e:
        logger.error(f"Error registering user: {e}")
        await db.rollback()
        return ApiResponse(
            success=False,
            error=str(e)
        )

@router.get("/email/{email}", response_model=ApiResponse)
async def get_user_by_email(
    email: str,
    db: AsyncSession = Depends(get_db)
):
    """通过邮箱获取用户信息"""
    try:
        result = await db.execute(
            select(SiteUserModel).where(SiteUserModel.email == email)
        )
        user = result.scalar_one_or_none()
        
        if not user:
            return ApiResponse(
                success=True,
                data=None,
                message="User not found"
            )
        
        return ApiResponse(
            success=True,
            data=user.to_dict()
        )
    except Exception as e:
        logger.error(f"Error fetching user: {e}")
        return ApiResponse(
            success=False,
            error=str(e)
        )

@router.put("/{user_id}", response_model=ApiResponse)
async def update_user(
    user_id: str,
    user_data: UserUpdate,
    db: AsyncSession = Depends(get_db)
):
    """更新用户信息"""
    try:
        # 查找用户
        result = await db.execute(
            select(SiteUserModel).where(SiteUserModel.id == user_id)
        )
        user = result.scalar_one_or_none()
        
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        # 更新字段
        if user_data.name is not None:
            user.name = user_data.name
        if user_data.image_url is not None:
            user.image_url = user_data.image_url
        if user_data.stripe_customer_id is not None:
            user.stripe_customer_id = user_data.stripe_customer_id
        
        user.updated_at = datetime.utcnow()
        
        await db.commit()
        await db.refresh(user)
        
        return ApiResponse(
            success=True,
            data=user.to_dict(),
            message="User updated successfully"
        )
    except Exception as e:
        logger.error(f"Error updating user: {e}")
        await db.rollback()
        return ApiResponse(
            success=False,
            error=str(e)
        )

@router.post("/{user_id}/bind-tenant", response_model=ApiResponse)
async def bind_tenant_to_user(
    user_id: str,
    binding_data: TenantBinding,
    db: AsyncSession = Depends(get_db)
):
    """绑定 Zendesk 租户到用户"""
    try:
        # 查找用户
        user_result = await db.execute(
            select(SiteUserModel).where(SiteUserModel.id == user_id)
        )
        user = user_result.scalar_one_or_none()
        
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        # 查找或创建租户 — use explicit store handle when multiple bindings exist
        from backend.tools.shopline_zendesk.db import binding_repo

        if binding_data.shopline_handle:
            store_binding = binding_repo.get_binding_by_subdomain_and_handle(
                binding_data.zendesk_subdomain,
                binding_data.shopline_handle,
            )
        else:
            bindings = binding_repo.list_bindings_by_subdomain(binding_data.zendesk_subdomain)
            if not bindings:
                store_binding = None
            elif len(bindings) == 1:
                store_binding = bindings[0]
            else:
                return ApiResponse(
                    success=False,
                    error=(
                        "Multiple Shopline stores are linked to this Zendesk "
                        "account. Provide shopline_handle to bind the user."
                    ),
                    data={
                        "code": "STORE_SELECTION_REQUIRED",
                        "available_stores": [binding["handle"] for binding in bindings],
                    },
                )

        if not store_binding:
            # No binding exists for this subdomain yet — cannot bind
            return ApiResponse(
                success=False,
                error=f"No Shopline store is linked to {binding_data.zendesk_subdomain}. "
                      f"Please connect a Shopline store first."
            )
        
        tenant_id = str(store_binding["store_id"])
        
        # 使用SQL直接插入关联表，避免ORM懒加载问题
        from sqlalchemy import text
        
        # 检查是否已经存在关联
        existing_result = await db.execute(
            text("SELECT 1 FROM user_tenants WHERE user_id = :user_id AND tenant_id = :tenant_id"),
            {"user_id": user.id, "tenant_id": tenant_id}
        )
        existing = existing_result.scalar()
        
        if not existing:
            # 插入关联
            await db.execute(
                text("INSERT INTO user_tenants (user_id, tenant_id, is_owner) VALUES (:user_id, :tenant_id, :is_owner)"),
                {"user_id": user.id, "tenant_id": tenant_id, "is_owner": binding_data.is_owner}
            )
        
        await db.commit()
        
        return ApiResponse(
            success=True,
            message=f"Tenant {binding.zendesk_subdomain} bound to user successfully"
        )
    except Exception as e:
        logger.error(f"Error binding tenant: {e}")
        await db.rollback()
        return ApiResponse(
            success=False,
            error=str(e)
        )

@router.get("/{user_id}/tenants", response_model=ApiResponse)
async def get_user_tenants(
    user_id: str,
    db: AsyncSession = Depends(get_db)
):
    """获取用户绑定的所有 Zendesk 租户"""
    try:
        # 查找用户
        result = await db.execute(
            select(SiteUserModel).where(SiteUserModel.id == user_id)
        )
        user = result.scalar_one_or_none()
        
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        # 使用SQL直接查询关联的租户，避免懒加载
        from sqlalchemy import text
        tenant_result = await db.execute(
            text("""
                SELECT s.id, b.zendesk_subdomain, ut.is_owner
                FROM shopline_zendesk.stores s
                JOIN shopline_zendesk.bindings b ON b.store_id = s.id
                JOIN user_tenants ut ON ut.tenant_id = CAST(s.id AS text)
                WHERE ut.user_id = :user_id
            """),
            {"user_id": user_id}
        )
        
        # 获取租户信息
        tenants_data = []
        for row in tenant_result:
            tenants_data.append({
                "id": str(row.id),
                "zendesk_subdomain": row.zendesk_subdomain,
                "is_active": True,
                "is_owner": row.is_owner,
                "has_active_subscription": False,
                "subscription": None
            })
        
        return ApiResponse(
            success=True,
            data=tenants_data
        )
    except Exception as e:
        logger.error(f"Error fetching user tenants: {e}")
        return ApiResponse(
            success=False,
            error=str(e)
        )

@router.post("/{user_id}/subscriptions", response_model=ApiResponse)
async def create_user_subscription(
    user_id: str,
    subscription_data: SubscriptionCreate,
    db: AsyncSession = Depends(get_db)
):
    """创建用户的 Stripe 订阅记录"""
    try:
        # 验证用户存在
        user_result = await db.execute(
            select(SiteUserModel).where(SiteUserModel.id == user_id)
        )
        user = user_result.scalar_one_or_none()
        
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        # 创建订阅记录 - 处理时区问题
        # 如果日期有时区信息，去掉它（PostgreSQL TIMESTAMP WITHOUT TIME ZONE）
        period_start = subscription_data.current_period_start
        period_end = subscription_data.current_period_end
        
        if hasattr(period_start, 'replace') and period_start.tzinfo:
            period_start = period_start.replace(tzinfo=None)
        if hasattr(period_end, 'replace') and period_end.tzinfo:
            period_end = period_end.replace(tzinfo=None)
        
        subscription = UserStripeSubscription(
            id=str(uuid.uuid4()),
            user_id=user_id,
            stripe_subscription_id=subscription_data.stripe_subscription_id,
            stripe_customer_id=subscription_data.stripe_customer_id,
            plan_name=subscription_data.plan_name,
            status=subscription_data.status,
            current_period_start=period_start,
            current_period_end=period_end,
            amount=subscription_data.amount,
            currency=subscription_data.currency,
            interval=subscription_data.interval
        )
        
        db.add(subscription)
        await db.commit()
        await db.refresh(subscription)
        
        return ApiResponse(
            success=True,
            data=subscription.to_dict(),
            message="Subscription created successfully"
        )
    except Exception as e:
        logger.error(f"Error creating subscription: {e}")
        await db.rollback()
        return ApiResponse(
            success=False,
            error=str(e)
        )

@router.get("/{user_id}/subscriptions", response_model=ApiResponse)
async def get_user_subscriptions(
    user_id: str,
    db: AsyncSession = Depends(get_db)
):
    """获取用户的所有 Stripe 订阅"""
    try:
        # 查询用户的订阅
        result = await db.execute(
            select(UserStripeSubscription).where(
                UserStripeSubscription.user_id == user_id
            ).order_by(UserStripeSubscription.created_at.desc())
        )
        subscriptions = result.scalars().all()
        
        subscriptions_data = [sub.to_dict() for sub in subscriptions]
        
        return ApiResponse(
            success=True,
            data=subscriptions_data
        )
    except Exception as e:
        logger.error(f"Error fetching subscriptions: {e}")
        return ApiResponse(
            success=False,
            error=str(e)
        )

class LoginRequest(BaseModel):
    email: EmailStr
    password: Optional[str] = None  # For email/password login

@router.post("/login", response_model=ApiResponse)
async def login_user(
    login_data: LoginRequest,
    db: AsyncSession = Depends(get_db)
):
    """用户登录（支持密码或OAuth）"""
    try:
        result = await db.execute(
            select(SiteUserModel).where(SiteUserModel.email == login_data.email)
        )
        user = result.scalar_one_or_none()
        
        if not user:
            return ApiResponse(
                success=False,
                error="Invalid email or password"
            )
        
        # 如果提供了密码，验证密码
        if login_data.password:
            if not user.password_hash:
                return ApiResponse(
                    success=False,
                    error="This account uses social login. Please sign in with Google."
                )
            
            if not bcrypt.checkpw(login_data.password.encode('utf-8'), user.password_hash.encode('utf-8')):
                return ApiResponse(
                    success=False,
                    error="Invalid email or password"
                )
            
            # 检查邮箱是否已验证
            if not user.is_verified:
                return ApiResponse(
                    success=False,
                    error="Please verify your email before logging in"
                )
        
        # 更新最后登录时间
        user.last_login = datetime.utcnow()
        await db.commit()
        await db.refresh(user)
        
        return ApiResponse(
            success=True,
            data=user.to_dict(),
            message="Login successful"
        )
    except Exception as e:
        logger.error(f"Error during login: {e}")
        return ApiResponse(
            success=False,
            error=str(e)
        )

@router.post("/request-password-reset", response_model=ApiResponse)
async def request_password_reset(
    reset_data: ResetPasswordRequest,
    db: AsyncSession = Depends(get_db)
):
    """请求密码重置"""
    try:
        # 查找用户
        result = await db.execute(
            select(SiteUserModel).where(SiteUserModel.email == reset_data.email)
        )
        user = result.scalar_one_or_none()
        
        if not user:
            # 为了安全，即使用户不存在也返回成功
            return ApiResponse(
                success=True,
                message="If the email exists, a reset link has been sent"
            )
        
        # 生成重置令牌
        reset_token = secrets.token_urlsafe(32)
        user.reset_token = reset_token
        user.reset_token_expires = datetime.utcnow() + timedelta(hours=1)
        
        await db.commit()
        
        # 发送密码重置邮件
        await email_service.send_password_reset_email(
            to_email=user.email,
            reset_token=reset_token,
            user_name=user.name
        )
        
        logger.info(f"Password reset email sent to {user.email}")
        
        return ApiResponse(
            success=True,
            message="If the email exists, a reset link has been sent"
        )
    except Exception as e:
        logger.error(f"Error requesting password reset: {e}")
        return ApiResponse(
            success=False,
            error="Failed to process request"
        )

@router.post("/reset-password", response_model=ApiResponse)
async def reset_password(
    reset_data: ConfirmResetRequest,
    db: AsyncSession = Depends(get_db)
):
    """重置密码"""
    try:
        # 查找具有此重置令牌的用户
        result = await db.execute(
            select(SiteUserModel).where(
                SiteUserModel.reset_token == reset_data.token,
                SiteUserModel.reset_token_expires > datetime.utcnow()
            )
        )
        user = result.scalar_one_or_none()
        
        if not user:
            return ApiResponse(
                success=False,
                error="Invalid or expired reset token"
            )
        
        # 更新密码
        password_hash = bcrypt.hashpw(reset_data.new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        user.password_hash = password_hash
        user.reset_token = None
        user.reset_token_expires = None
        
        await db.commit()
        
        return ApiResponse(
            success=True,
            message="Password reset successfully"
        )
    except Exception as e:
        logger.error(f"Error resetting password: {e}")
        return ApiResponse(
            success=False,
            error="Failed to reset password"
        )

@router.put("/{user_id}/set-password", response_model=ApiResponse)
async def set_user_password(
    user_id: str,
    password_data: SetPasswordRequest,
    db: AsyncSession = Depends(get_db)
):
    """为用户设置密码（允许Google用户添加密码登录）"""
    try:
        # 查找用户
        result = await db.execute(
            select(SiteUserModel).where(SiteUserModel.id == user_id)
        )
        user = result.scalar_one_or_none()
        
        if not user:
            return ApiResponse(
                success=False,
                error="User not found"
            )
        
        # 检查密码长度
        if len(password_data.password) < 8:
            return ApiResponse(
                success=False,
                error="Password must be at least 8 characters long"
            )
        
        # 设置密码
        password_hash = bcrypt.hashpw(password_data.password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        user.password_hash = password_hash
        user.updated_at = datetime.utcnow()
        
        await db.commit()
        await db.refresh(user)
        
        return ApiResponse(
            success=True,
            data=user.to_dict(),
            message="Password set successfully"
        )
    except Exception as e:
        logger.error(f"Error setting password: {e}")
        await db.rollback()
        return ApiResponse(
            success=False,
            error=str(e)
        )

@router.post("/verify-email", response_model=ApiResponse)
async def verify_email(
    verify_data: VerifyEmailRequest,
    db: AsyncSession = Depends(get_db)
):
    """验证邮箱"""
    try:
        # 查找具有此验证令牌的用户
        result = await db.execute(
            select(SiteUserModel).where(
                SiteUserModel.email_verification_token == verify_data.token,
                SiteUserModel.email_verification_expires > datetime.utcnow()
            )
        )
        user = result.scalar_one_or_none()
        
        if not user:
            return ApiResponse(
                success=False,
                error="Invalid or expired verification token"
            )
        
        # 标记邮箱已验证
        user.is_verified = True
        user.email_verification_token = None
        user.email_verification_expires = None
        
        await db.commit()
        await db.refresh(user)
        
        # 发送欢迎邮件
        await email_service.send_welcome_email(
            to_email=user.email,
            user_name=user.name
        )
        
        return ApiResponse(
            success=True,
            data=user.to_dict(),
            message="Email verified successfully"
        )
    except Exception as e:
        logger.error(f"Error verifying email: {e}")
        return ApiResponse(
            success=False,
            error="Failed to verify email"
        )

@router.put("/{user_id}/subscriptions/{stripe_subscription_id}", response_model=ApiResponse)
async def update_user_subscription(
    user_id: str,
    stripe_subscription_id: str,
    subscription_data: SubscriptionUpdate,
    db: AsyncSession = Depends(get_db)
):
    """更新用户的 Stripe 订阅记录"""
    try:
        # 验证用户存在
        user_result = await db.execute(
            select(SiteUserModel).where(SiteUserModel.id == user_id)
        )
        user = user_result.scalar_one_or_none()
        
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        # 查找订阅记录
        subscription_result = await db.execute(
            select(UserStripeSubscription).where(
                UserStripeSubscription.user_id == user_id,
                UserStripeSubscription.stripe_subscription_id == stripe_subscription_id
            )
        )
        subscription = subscription_result.scalar_one_or_none()
        
        if not subscription:
            raise HTTPException(status_code=404, detail="Subscription not found")
        
        # 更新字段
        if subscription_data.status is not None:
            subscription.status = subscription_data.status
            
        if subscription_data.current_period_end is not None:
            subscription.current_period_end = subscription_data.current_period_end
            
        if subscription_data.cancel_at_period_end is not None:
            subscription.cancel_at_period_end = subscription_data.cancel_at_period_end
            
        if subscription_data.amount is not None:
            subscription.amount = subscription_data.amount
            
        if subscription_data.plan_name is not None:
            subscription.plan_name = subscription_data.plan_name
        
        # 如果状态变为取消，设置取消时间
        if subscription_data.status == 'canceled' and not subscription.canceled_at:
            subscription.canceled_at = datetime.utcnow()
        
        subscription.updated_at = datetime.utcnow()
        
        await db.commit()
        await db.refresh(subscription)
        
        return ApiResponse(
            success=True,
            data=subscription.to_dict(),
            message="Subscription updated successfully"
        )
    except Exception as e:
        logger.error(f"Error updating subscription: {e}")
        await db.rollback()
        return ApiResponse(
            success=False,
            error=str(e)
        )

@router.get("/{user_id}/company-info", response_model=ApiResponse)
async def get_company_info(user_id: str, db: AsyncSession = Depends(get_db)):
    """Get company information for a user"""
    try:
        result = await db.execute(
            select(SiteUserModel).where(SiteUserModel.id == user_id)
        )
        user = result.scalar_one_or_none()
        
        if not user:
            return ApiResponse(
                success=False,
                error="User not found"
            )
        
        company_info = {
            "company_name": user.company_name,
            "company_address": user.company_address,
            "company_city": user.company_city,
            "company_state": user.company_state,
            "company_postal_code": user.company_postal_code,
            "company_country": user.company_country
        }
        
        return ApiResponse(
            success=True,
            data=company_info
        )
    except Exception as e:
        logger.error(f"Error getting company info: {e}")
        return ApiResponse(
            success=False,
            error=str(e)
        )

@router.put("/{user_id}/company-info", response_model=ApiResponse)
async def update_company_info(
    user_id: str,
    company_info: CompanyInfoUpdate,
    db: AsyncSession = Depends(get_db)
):
    """Update company information for a user"""
    try:
        result = await db.execute(
            select(SiteUserModel).where(SiteUserModel.id == user_id)
        )
        user = result.scalar_one_or_none()
        
        if not user:
            return ApiResponse(
                success=False,
                error="User not found"
            )
        
        # Update company fields
        update_data = company_info.dict(exclude_unset=True)
        for field, value in update_data.items():
            setattr(user, field, value)
        
        user.updated_at = datetime.utcnow()
        await db.commit()
        
        # Return updated company info
        updated_info = {
            "company_name": user.company_name,
            "company_address": user.company_address,
            "company_city": user.company_city,
            "company_state": user.company_state,
            "company_postal_code": user.company_postal_code,
            "company_country": user.company_country
        }
        
        return ApiResponse(
            success=True,
            data=updated_info,
            message="Company information updated successfully"
        )
    except Exception as e:
        await db.rollback()
        logger.error(f"Error updating company info: {e}")
        return ApiResponse(
            success=False,
            error=str(e)
        )

# Duplicate class and endpoint removed - already defined earlier
