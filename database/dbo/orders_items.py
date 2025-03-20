import random
import decimal
from datetime import timedelta

from database.models import OrderItem

class OrderItemModel:
    """Class that combines order item model definition and data generation."""
    
    OrderItem = OrderItem
    
    # Product data for generating order items
    products = [
        {"name": "Smartphone", "id": "TECH001", "price": decimal.Decimal('699.99')},
        {"name": "Laptop", "id": "TECH002", "price": decimal.Decimal('1299.99')},
        {"name": "Headphones", "id": "TECH003", "price": decimal.Decimal('149.99')},
        {"name": "Smart Watch", "id": "TECH004", "price": decimal.Decimal('249.99')},
        {"name": "Tablet", "id": "TECH005", "price": decimal.Decimal('499.99')},
        {"name": "Bluetooth Speaker", "id": "TECH006", "price": decimal.Decimal('79.99')},
        {"name": "Wireless Mouse", "id": "TECH007", "price": decimal.Decimal('39.99')},
        {"name": "Keyboard", "id": "TECH008", "price": decimal.Decimal('59.99')},
        {"name": "Monitor", "id": "TECH009", "price": decimal.Decimal('299.99')},
        {"name": "External Hard Drive", "id": "TECH010", "price": decimal.Decimal('129.99')},
        {"name": "USB Flash Drive", "id": "TECH011", "price": decimal.Decimal('19.99')},
        {"name": "Power Bank", "id": "TECH012", "price": decimal.Decimal('49.99')},
        {"name": "Phone Case", "id": "ACC001", "price": decimal.Decimal('24.99')},
        {"name": "Screen Protector", "id": "ACC002", "price": decimal.Decimal('14.99')},
        {"name": "Charging Cable", "id": "ACC003", "price": decimal.Decimal('9.99')},
    ]
    
    @staticmethod
    def safe_length(value, max_length):
        """Ensure field values don't exceed column size limits."""
        if isinstance(value, str) and len(value) > max_length:
            return value[:max_length]
        return value
    
    @classmethod
    def generate_data(cls, session, orders, max_items_per_order=10):
        """Generate and insert order item records for the given orders."""
        try:
            print(f"Generating order items for {len(orders)} orders (max {max_items_per_order} per order)...")
            item_count = 0

            for order in orders:
                # Generate random items for this order
                num_items = random.randint(1, max_items_per_order)
                order_items = []
                total_amount = decimal.Decimal('0.00')
                
                # Generate items for this order
                for _ in range(num_items):
                    product = random.choice(cls.products)
                    quantity = random.randint(1, 3)
                    discount = decimal.Decimal(str(round(random.uniform(0, 0.15), 2)))  # 0-15% discount
                    unit_price = product["price"]
                    item_price = unit_price * (1 - discount)
                    item_total = item_price * quantity
                    total_amount += item_total

                    order_item = cls.OrderItem(
                        order_id=order.order_id,
                        product_name=cls.safe_length(product["name"], 100),
                        product_id=cls.safe_length(product["id"], 50),
                        quantity=quantity,
                        unit_price=unit_price,
                        discount=discount,
                        last_update=order.order_date + timedelta(minutes=random.randint(1, 5))
                    )
                    
                    order_items.append(order_item)
                
                # Update the order's total amount
                order.total_amount = total_amount.quantize(decimal.Decimal('0.01'))
                
                # Add items to session
                session.add_all(order_items)
                item_count += len(order_items)
                
                # Commit in batches to avoid large transactions
                if item_count % 100 == 0:
                    session.commit()
            
            # Final commit
            session.commit()
            print(f"Successfully generated {item_count} order items.")
            
        except Exception as e:
            session.rollback()
            print(f"Error generating order items: {e}")