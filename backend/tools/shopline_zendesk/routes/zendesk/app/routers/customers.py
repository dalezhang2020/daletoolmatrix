from fastapi import APIRouter, Request, Query, HTTPException
from typing import Optional, List
from ..models.customer import Customer, CustomerFilters, CustomerResponse, CustomersResponse
from ..models.base import ApiResponse
from ..services.shopline_api import ShoplineAPIService
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

@router.options("/search")
async def search_customers_options():
    """处理客户搜索的 CORS 预检请求"""
    return {"message": "OK"}

def get_shopline_service(request: Request) -> ShoplineAPIService:
    """获取Shopline API服务实例"""
    # 添加详细的调试日志
    logger.info(f"Request state attributes: {dir(request.state)}")
    logger.info(f"Request headers: X-Zendesk-Subdomain={request.headers.get('X-Zendesk-Subdomain')}")
    
    store_domain = getattr(request.state, 'shopline_domain', None)
    access_token = getattr(request.state, 'shopline_access_token', None)
    
    # 添加调试日志
    zendesk_subdomain = getattr(request.state, 'zendesk_subdomain', 'unknown')
    logger.info(f"Getting Shopline service for {zendesk_subdomain}: domain={store_domain}, has_token={bool(access_token)}")
    
    if not store_domain or not access_token:
        logger.error(f"Missing Shopline config for {zendesk_subdomain}: domain={store_domain}, token_exists={bool(access_token)}")
        raise HTTPException(
            status_code=400,
            detail="Shopline configuration not found"
        )
    
    return ShoplineAPIService(store_domain, access_token)

@router.get("/search", response_model=CustomersResponse)
async def search_customers(
    request: Request,
    email: Optional[str] = Query(None),
    phone: Optional[str] = Query(None),
    first_name: Optional[str] = Query(None),
    last_name: Optional[str] = Query(None),
    order_id: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100)
):
    """搜索客户，支持多种搜索方式"""
    try:
        shopline_service = get_shopline_service(request)
        
        # Search by email — use v2 customer search so archived-only customers
        # are still found. Orders are fetched separately by the frontend via
        # GET /api/customers/{customer_id}/orders.
        if email:
            logger.info(f"Searching by email: {email}")
            try:
                customers_result = await shopline_service.search_customers_by_email(email)
                return CustomersResponse(
                    success=True,
                    data=customers_result.get("customers", []),
                )
            except Exception as e:
                logger.error(f"Error searching by email: {e}")
                return CustomersResponse(success=True, data=[])
        
        # 如果提供了订单ID，通过订单API查找客户
        if order_id:
            logger.info(f"Searching customer by order ID/name: {order_id}")
            try:
                # 使用 orders.json?name=xxx API 查找订单
                orders_result = await shopline_service.get_orders_by_name(order_id)
                orders = orders_result.get('orders', [])
                
                if orders:
                    # 从订单中提取客户信息
                    customers_map = {}
                    for order in orders:
                        customer_info = order.get('customer', {})
                        customer_id = customer_info.get('id')
                        
                        if customer_id:
                            if customer_id not in customers_map:
                                # 创建客户记录
                                customers_map[customer_id] = {
                                    'id': customer_id,
                                    'email': order.get('email') or customer_info.get('email'),
                                    'first_name': customer_info.get('first_name'),
                                    'last_name': customer_info.get('last_name'),
                                    'phone': customer_info.get('phone') or order.get('phone'),
                                    'orders_count': 1,
                                    'total_spent': float(order.get('current_total_price', 0)),
                                    'created_at': customer_info.get('created_at'),
                                    'updated_at': customer_info.get('updated_at'),
                                    'matched_order': {
                                        'id': order.get('id'),
                                        'name': order.get('name'),
                                        'status': order.get('status'),
                                        'total': order.get('current_total_price')
                                    }
                                }
                            else:
                                # 更新订单统计
                                customers_map[customer_id]['orders_count'] += 1
                                customers_map[customer_id]['total_spent'] += float(order.get('current_total_price', 0))
                    
                    # 转换为列表
                    formatted_customers = list(customers_map.values())
                    
                    return CustomersResponse(
                        success=True,
                        data=formatted_customers
                    )
                else:
                    # 没有找到订单，返回空结果
                    return CustomersResponse(
                        success=True,
                        data=[]
                    )
            except Exception as e:
                logger.error(f"Error searching customer by order ID: {e}")
                # Return empty result on error
                return CustomersResponse(
                    success=True,
                    data=[]
                )
        
        # 如果提供了姓名，使用精确查询或模糊搜索
        if first_name or last_name:
            logger.info(f"Searching by name: first_name={first_name}, last_name={last_name}")
            try:
                # 如果同时提供了姓和名，使用精确查询
                if first_name and last_name:
                    # 使用精确查询格式
                    query = f"customer_first_name:{first_name};customer_last_name:{last_name}"
                    customers_result = await shopline_service.search_customers_by_query(query)
                else:
                    # 只有姓或名，使用模糊搜索
                    search_term = first_name or last_name
                    customers_result = await shopline_service.search_customers_fuzzy(search_term)
                
                customers_data = customers_result.get('customers', [])
                
                # 处理返回的客户数据
                formatted_customers = []
                for customer in customers_data:
                    formatted_customer = {
                        'id': customer.get('id'),
                        'email': customer.get('email'),
                        'first_name': customer.get('first_name'),
                        'last_name': customer.get('last_name'),
                        'phone': customer.get('phone'),
                        'orders_count': customer.get('orders_count', 0),
                        'total_spent': customer.get('total_spent', 0),
                        'created_at': customer.get('created_at'),
                        'updated_at': customer.get('updated_at')
                    }
                    formatted_customers.append(formatted_customer)
                
                return CustomersResponse(
                    success=True,
                    data=formatted_customers
                )
            except Exception as e:
                logger.error(f"Error searching by name: {e}")
                return CustomersResponse(
                    success=True,
                    data=[]
                )
        
        # 如果提供了电话，使用模糊搜索
        if phone:
            logger.info(f"Searching customers by phone: {phone}")
            try:
                # 使用模糊搜索API，它可以在多个字段中搜索包括电话号码
                customers_result = await shopline_service.search_customers_fuzzy(phone)
                customers_data = customers_result.get('customers', [])
                
                # 处理返回的客户数据
                formatted_customers = []
                for customer in customers_data:
                    formatted_customer = {
                        'id': customer.get('id'),
                        'email': customer.get('email'),
                        'first_name': customer.get('first_name'),
                        'last_name': customer.get('last_name'),
                        'phone': customer.get('phone'),
                        'orders_count': customer.get('orders_count', 0),
                        'total_spent': customer.get('total_spent', 0),
                        'created_at': customer.get('created_at'),
                        'updated_at': customer.get('updated_at')
                    }
                    formatted_customers.append(formatted_customer)
                
                return CustomersResponse(
                    success=True,
                    data=formatted_customers
                )
            except Exception as e:
                logger.error(f"Error searching customers by phone: {e}")
                return CustomersResponse(
                    success=True,
                    data=[]
                )
        
        # 如果没有提供任何搜索条件，返回错误
        return CustomersResponse(
            success=False,
            error="Please provide at least one search criteria (email, phone, name, or order ID)"
        )
    except Exception as e:
        logger.error(f"Error searching customers: {e}")
        return CustomersResponse(
            success=False,
            error=str(e)
        )

@router.get("/by-email", response_model=CustomerResponse)
async def get_customer_by_email(
    request: Request,
    email: str = Query(..., description="Customer email address")
):
    """通过邮箱获取客户"""
    try:
        shopline_service = get_shopline_service(request)
        result = await shopline_service.search_customers_by_email(email)
        
        customers = result.get('customers', [])
        customer = customers[0] if customers else None
        
        return CustomerResponse(
            success=True,
            data=customer
        )
    except Exception as e:
        logger.error(f"Error getting customer by email: {e}")
        return CustomerResponse(
            success=False,
            error=str(e)
        )

@router.get("/by-phone", response_model=CustomerResponse)
async def get_customer_by_phone(
    request: Request,
    phone: str = Query(..., description="Customer phone number")
):
    """通过电话获取客户"""
    try:
        shopline_service = get_shopline_service(request)
        result = await shopline_service.search_customers_by_phone(phone)
        
        customers = result.get('customers', [])
        customer = customers[0] if customers else None
        
        return CustomerResponse(
            success=True,
            data=customer
        )
    except Exception as e:
        logger.error(f"Error getting customer by phone: {e}")
        return CustomerResponse(
            success=False,
            error=str(e)
        )

@router.get("/{customer_id}/orders")
async def get_customer_orders(
    request: Request,
    customer_id: str,
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100)
):
    """获取客户的订单列表"""
    try:
        shopline_service = get_shopline_service(request)
        
        # 使用 buyer_id 参数获取客户订单
        orders_result = await shopline_service.get_orders_by_customer(customer_id, limit=limit)
        orders = orders_result.get('orders', [])
        
        # 格式化订单数据
        formatted_orders = []
        for order in orders:
            formatted_order = {
                'id': order.get('id'),
                'name': order.get('name'),
                'status': order.get('status'),
                'financial_status': order.get('financial_status'),
                'fulfillment_status': order.get('fulfillment_status'),
                'current_total_price': order.get('current_total_price'),
                'currency': order.get('currency'),
                'created_at': order.get('created_at'),
                'updated_at': order.get('updated_at'),
                'line_items_count': len(order.get('line_items', [])),
                'customer': {
                    'id': order.get('customer', {}).get('id'),
                    'email': order.get('email'),
                    'first_name': order.get('customer', {}).get('first_name'),
                    'last_name': order.get('customer', {}).get('last_name')
                }
            }
            formatted_orders.append(formatted_order)
        
        return ApiResponse(
            success=True,
            data={
                'orders': formatted_orders,
                'total': len(formatted_orders),
                'customer_id': customer_id
            }
        )
    except Exception as e:
        logger.error(f"Error getting customer orders: {e}")
        return ApiResponse(
            success=False,
            error=str(e)
        )

@router.get("/{customer_id}", response_model=CustomerResponse)
async def get_customer(
    request: Request,
    customer_id: str
):
    """获取单个客户"""
    try:
        shopline_service = get_shopline_service(request)
        result = await shopline_service.get_customer(customer_id)
        
        return CustomerResponse(
            success=True,
            data=result.get('customer')
        )
    except Exception as e:
        logger.error(f"Error getting customer: {e}")
        return CustomerResponse(
            success=False,
            error=str(e)
        ) 