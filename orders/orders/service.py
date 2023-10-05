from nameko.events import EventDispatcher
from nameko.rpc import rpc
from nameko_sqlalchemy import DatabaseSession

from orders.exceptions import NotFound
from orders.models import DeclarativeBase, Order, OrderDetail
from orders.schemas import OrderSchema

from caching.cache_service import CacheService

class OrdersService:
    name = 'orders'

    def __init__(self):
        self.cache = CacheService()

    db = DatabaseSession(DeclarativeBase)
    event_dispatcher = EventDispatcher()

    @rpc
    def get_order(self, order_id):
        cached_order = self.cache.retrieve_cached_data(order_id)
        if cached_order is not None:
            return cached_order
        
        order = self.db.query(Order).get(order_id)

        if not order:
            raise NotFound('Order with id {} not found'.format(order_id))

        serialized_order = OrderSchema().dump(order).data
        self.cache.cache_data(order_id, serialized_order, expiration = 3600)
        return serialized_order

    @rpc
    def create_order(self, order_details):
        order_details_products = [
            OrderDetail(
                product_id = order_detail['product_id'],
                price = order_detail['price'],
                quantity = order_detail['quantity']
            )
            for order_detail in order_details
        ]

        order = Order(order_details=order_details_products)
        self.db.add(order)
        self.db.commit()

        order = OrderSchema().dump(order).data

        self.event_dispatcher('order_created', {
            'order': order,
        })

        return order

    @rpc
    def update_order(self, order):
        order_details = {
            order_details['id']: order_details
            for order_details in order['order_details']
        }

        order = self.db.query(Order).get(order['id'])

        for order_detail in order.order_details:
            if order_detail.id in order_details:
                detail = order_details[order_detail.id]
                order_detail.price = detail['price']
                order_detail.quantity = detail['quantity']

        self.db.commit()
        return OrderSchema().dump(order).data

    @rpc
    def delete_order(self, order_id):
        order = self.db.query(Order).get(order_id)
        self.db.delete(order)
        self.db.commit()

    @rpc
    def list_orders(self):
        order_list = self.db.query(Order).all()
        return OrderSchema(many=True).dump(order_list).data