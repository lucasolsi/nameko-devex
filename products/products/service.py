import logging

from nameko.events import event_handler
from nameko.rpc import rpc

from products import dependencies, schemas

from caching.cache_service import CacheService

logger = logging.getLogger(__name__)


class ProductsService:

    name = 'products'

    storage = dependencies.Storage()
    cache = CacheService()

    @rpc
    def get(self, product_id):
        cached_product = self.cache.retrieve_cached_data(product_id)
        if cached_product is not None:
            return cached_product
        product = self.storage.get(product_id)

        self.cache.cache_data(product_id, product, expiration=3600)
        return schemas.Product().dump(product).data

    @rpc
    def list(self):
        products = self.storage.list()
        return schemas.Product(many=True).dump(products).data

    @rpc
    def create(self, product):
        product = schemas.Product(strict=True).load(product).data
        self.storage.create(product)

    @rpc
    def delete(self, product_id):
        self.storage.delete(product_id)
        self.cache.remove_from_cache(product_id)

    @event_handler('orders', 'order_created')
    def handle_order_created(self, payload):
        for product in payload['order']['order_details']:
            product_id = product['product_id']
            self.cache.remove_from_cache(product_id)
            self.storage.decrement_stock(
                product['product_id'], product['quantity'])
