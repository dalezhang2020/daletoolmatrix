import httpx
from typing import Optional, Dict, Any, List
from datetime import datetime
import logging
import warnings
from ..models.customer import Customer, CustomerFilters
from ..models.order import Order, OrderFilters, OrderStatus


logger = logging.getLogger(__name__)

class ShoplineAPIService:
    def __init__(self, shopline_domain: str, access_token: str):
        """
        初始化 Shopline API 服务
        
        Args:
            shopline_domain: Shopline 域名，例如 "zg-sandbox"
            access_token: JWT 访问令牌
        """
        self.shopline_domain = shopline_domain
        self.access_token = access_token
        # 使用 Shopline API v20250601 版本
        self.base_url = f"https://{shopline_domain}.myshopline.com/admin/openapi/v20250601"
        self.headers = {
            "Authorization": f"Bearer {access_token}",  # 使用 Bearer 格式
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json"
        }
    
    async def _make_request(self, method: str, endpoint: str, max_retries: int = 2, **kwargs) -> Dict[Any, Any]:
        """发起HTTP请求到Shopline API，支持重试机制"""
        url = f"{self.base_url}{endpoint}"
        
        # 记录详细的请求信息
        logger.info(f"Shopline API request details:")
        logger.info(f"  Method: {method}")
        logger.info(f"  URL: {url}")
        logger.info(f"  Headers: {self.headers}")
        logger.info(f"  Kwargs: {kwargs}")
        
        for attempt in range(max_retries + 1):
            # 禁用 SSL 验证以避免证书问题（仅用于开发/测试环境）
            async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
                try:
                    logger.info(f"Shopline API request (attempt {attempt + 1}): {method} {url}")
                    response = await client.request(
                        method=method,
                        url=url,
                        headers=self.headers,
                        **kwargs
                    )
                    
                    # 记录响应状态
                    logger.info(f"Shopline API response: {response.status_code}")
                    
                    response.raise_for_status()
                    return response.json()
                    
                except httpx.HTTPStatusError as e:
                    error_msg = f"HTTP {e.response.status_code} for {url}"
                    logger.error(f"Shopline API HTTP error (attempt {attempt + 1}): {error_msg}")
                    
                    # 对于 5xx 错误，如果还有重试机会就继续
                    if e.response.status_code >= 500 and attempt < max_retries:
                        logger.info(f"Retrying due to server error (attempt {attempt + 1}/{max_retries})")
                        continue
                    
                    # 对于 4xx 错误或最后一次重试失败，直接抛出异常
                    raise Exception(f"Shopline API error: {error_msg}")
                    
                except httpx.RequestError as e:
                    error_msg = f"Request failed: {str(e)}"
                    logger.error(f"Shopline API request error (attempt {attempt + 1}): {error_msg}")
                    
                    # 网络错误也可以重试
                    if attempt < max_retries:
                        logger.info(f"Retrying due to request error (attempt {attempt + 1}/{max_retries})")
                        continue
                    
                    raise Exception(f"Shopline API error: {error_msg}")
        
        # 这里不应该到达，但为了安全起见
        raise Exception("Shopline API error: All retry attempts failed")
    
    # 客户相关方法
    async def get_customers(self, filters: Optional[CustomerFilters] = None, page: int = 1, limit: int = 50) -> Dict[str, Any]:
        """获取客户列表"""
        params = {
            "page": page,
            "limit": limit
        }
        
        if filters:
            if filters.email:
                params["email"] = filters.email
            if filters.phone:
                params["phone"] = filters.phone
            if filters.first_name:
                params["first_name"] = filters.first_name
            if filters.last_name:
                params["last_name"] = filters.last_name
            if filters.created_after:
                params["created_at_min"] = filters.created_after.isoformat()
            if filters.created_before:
                params["created_at_max"] = filters.created_before.isoformat()
        
        return await self._make_request("GET", "/customers.json", params=params)
    
    async def get_customer(self, customer_id: str) -> Dict[str, Any]:
        """获取单个客户"""
        return await self._make_request("GET", f"/customers/{customer_id}.json")
    
    async def search_customers_by_email(self, email: str) -> Dict[str, Any]:
        """通过邮箱搜索客户.

        Uses the v2 search endpoint which works for all customers including
        those whose orders are archived. The legacy /customers.json?email=
        endpoint returns 500 on this storefront.
        """
        try:
            return await self._make_request(
                "GET",
                "/customers/v2/search.json",
                params={"query_param": email},
            )
        except Exception as e:
            logger.error(f"Failed to search customers by email: {e}")
            return {"customers": []}
    
    async def search_customers_by_phone(self, phone: str) -> Dict[str, Any]:
        """通过电话搜索客户 (v2 search endpoint)."""
        try:
            return await self._make_request(
                "GET",
                "/customers/v2/search.json",
                params={"query_param": phone},
            )
        except Exception as e:
            logger.error(f"Failed to search customers by phone: {e}")
            return {"customers": []}
    
    async def search_customers_by_name(self, first_name: str = None, last_name: str = None) -> Dict[str, Any]:
        """通过姓名搜索客户"""
        try:
            params = {}
            if first_name:
                params['first_name'] = first_name
            if last_name:
                params['last_name'] = last_name
            
            if params:
                return await self._make_request("GET", "/customers.json", params=params)
            else:
                raise ValueError("至少需要提供 first_name 或 last_name")
        except Exception as e:
            logger.error(f"Failed to search customers by name: {e}")
            return {'customers': []}
    
    async def search_customers_fuzzy(self, search_term: str) -> Dict[str, Any]:
        """模糊搜索客户（支持姓名、邮箱、电话、订单号等）"""
        return await self._make_request("GET", "/customers/v2/search.json", params={"query_param": search_term})
    
    async def search_customers_by_query(self, query: str) -> Dict[str, Any]:
        """精确查询客户
        Args:
            query: 精确查询条件，格式如 "customer_first_name:John;customer_last_name:Doe"
        """
        return await self._make_request("GET", "/customers/v2/search.json", params={"query": query})
    
    # 订单相关方法
    async def get_orders(self, filters: Optional[OrderFilters] = None, page_info: Optional[str] = None, limit: int = 50) -> Dict[str, Any]:
        """获取订单列表，使用 Shopline API 标准分页"""
        params = {
            "limit": min(limit, 100)  # API 最大限制 100
        }
        
        # 检查是否使用邮箱过滤
        has_email_filter = filters and filters.email
        
        # 当使用邮箱过滤时，不使用 page_info 分页，而使用排序
        if has_email_filter:
            # 使用排序而不是分页，避免 API 500 错误
            params["sort_condition"] = "order_at:desc"
        else:
            # 只有在没有邮箱过滤时才使用 page_info 分页
            if page_info:
                params["page_info"] = page_info
        
        if filters:
            if filters.status:
                params["status"] = filters.status.value
            if filters.financial_status:
                params["financial_status"] = filters.financial_status.value
            if filters.fulfillment_status:
                params["fulfillment_status"] = filters.fulfillment_status.value
            if filters.email:
                params["email"] = filters.email
            # 注意：根据文档，订单 API 不支持 phone 参数搜索
            if filters.customer_id:
                params["buyer_id"] = filters.customer_id  # 使用正确的参数名
            if filters.created_after:
                params["created_at_min"] = filters.created_after.isoformat()
            if filters.created_before:
                params["created_at_max"] = filters.created_before.isoformat()
        
        return await self._make_request("GET", "/orders.json", params=params)
    
    async def get_order(self, order_id: str) -> Dict[str, Any]:
        """获取单个订单"""
        return await self._make_request("GET", f"/orders/{order_id}.json")
    
    async def get_orders_by_name(self, order_name: str) -> Dict[str, Any]:
        """通过订单名称获取订单（如示例中的 name=1097790）"""
        return await self._make_request("GET", "/orders.json", params={"name": order_name})
    
    async def get_orders_by_email(self, email: str, page_info: Optional[str] = None, limit: int = 50) -> Dict[str, Any]:
        """通过客户邮箱获取订单"""
        params = {
            "email": email,
            "limit": min(limit, 100),
            "sort_condition": "order_at:desc"  # 按订单时间倒序排列
        }
        
        if page_info:
            params["page_info"] = page_info
            
        return await self._make_request("GET", "/orders.json", params=params)
    
    async def get_orders_by_customer(self, customer_id: str, page_info: Optional[str] = None, limit: int = 50) -> Dict[str, Any]:
        """Return all orders for a customer including archived ones.

        Uses /customers/{id}/orders.json with status=any so that orders
        Shopline has moved to the archived ("hidden") cold tier are still
        included. The response uses ``list`` instead of ``orders``; we
        normalize it back to ``orders`` for downstream compatibility.
        """
        params = {
            "status": "any",
            "limit": min(limit, 100),
        }
        if page_info:
            params["page_info"] = page_info

        result = await self._make_request(
            "GET",
            f"/customers/{customer_id}/orders.json",
            params=params,
        )
        # Normalize the response shape: {"list": [...]} -> {"orders": [...]}
        if "orders" not in result:
            result["orders"] = result.pop("list", []) or []
        return result
    
    async def update_order_status(self, order_id: str, status: OrderStatus, notes: Optional[str] = None) -> Dict[str, Any]:
        """更新订单状态"""
        data = {"status": status.value}
        if notes:
            data["notes"] = notes
        return await self._make_request("PUT", f"/orders/{order_id}.json", json=data)
    
    async def cancel_order(self, order_id: str, reason: Optional[str] = None) -> Dict[str, Any]:
        """取消订单"""
        data = {"status": "cancelled"}
        if reason:
            data["cancel_reason"] = reason
        return await self._make_request("PUT", f"/orders/{order_id}/cancel.json", json=data)
    
    async def create_refund(self, order_id: str, amount: float, reason: str) -> Dict[str, Any]:
        """创建退款"""
        data = {
            "amount": amount,
            "reason": reason
        }
        return await self._make_request("POST", f"/orders/{order_id}/refunds.json", json=data)
    
    async def get_order_timeline(self, order_id: str) -> Dict[str, Any]:
        """获取订单时间线"""
        return await self._make_request("GET", f"/orders/{order_id}/events.json")
    
    # 物流相关方法
    async def get_shipping_info(self, order_id: str) -> Dict[str, Any]:
        """获取物流信息"""
        return await self._make_request("GET", f"/orders/{order_id}/fulfillments.json")
    
    async def track_package(self, tracking_number: str) -> Dict[str, Any]:
        """跟踪包裹"""
        return await self._make_request("GET", f"/fulfillments/track/{tracking_number}.json")
    
    # 店铺信息
    async def get_shop_info(self) -> Dict[str, Any]:
        """获取店铺信息"""
        return await self._make_request("GET", "/shop.json") 