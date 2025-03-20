import random
import decimal

from faker import Faker
from datetime import datetime, timedelta

from database.models import Order

fake = Faker()

class OrderModel:
    """Class that combines order model definition and data generation."""
    
    Order = Order
    
    payment_methods = ["Credit Card", "PayPal", "Apple Pay", "Google Pay", "Bank Transfer"]
    order_statuses = ["pending", "processing", "shipped", "delivered", "cancelled"]
    
    @staticmethod
    def safe_length(value, max_length):
        """Ensure field values don't exceed column size limits."""
        if isinstance(value, str) and len(value) > max_length:
            return value[:max_length]
        return value
    
    @classmethod
    def generate_data(cls, session, customers, max_orders_per_customer=5):
        """Generate and insert order records for the given customers."""
        try:
            print(f"Generating orders for {len(customers)} customers (max {max_orders_per_customer} per customer)...")
            all_orders = []
            order_count = 0

            for customer in customers:
                # Random number of orders for this customer
                num_orders = random.randint(0, max_orders_per_customer)
                
                for _ in range(num_orders):
                    # Generate random order date within the last year
                    order_date = fake.date_time_between(start_date="-1y", end_date="now")

                    # Random shipping address (sometimes different from billing address)
                    use_billing_address = random.random() > 0.3
                    if use_billing_address:
                        address = customer.address
                        city = customer.city
                        state = customer.state
                        zip_code = customer.zip_code
                    else:
                        address = fake.street_address()
                        city = fake.city()
                        state = fake.state_abbr()
                        zip_code = fake.zipcode()

                    # Select status based on order date
                    days_ago = (datetime.now() - order_date).days
                    if days_ago < 2:
                        status_weights = [0.7, 0.3, 0, 0, 0]  # Mostly pending or processing
                    elif days_ago < 5:
                        status_weights = [0.1, 0.4, 0.5, 0, 0]  # Mostly processing or shipped
                    elif days_ago < 14:
                        status_weights = [0, 0.1, 0.3, 0.6, 0]  # Mostly delivered
                    else:
                        status_weights = [0, 0, 0.1, 0.8, 0.1]  # Mostly delivered, some cancelled

                    status = random.choices(cls.order_statuses, weights=status_weights)[0]

                    # Create the order (total_amount will be calculated later)
                    order = cls.Order(
                        customer_id=customer.customer_id,
                        order_date=order_date,
                        status=status,
                        total_amount=decimal.Decimal('0.00'),  # Placeholder, will update after adding items
                        payment_method=cls.safe_length(random.choice(cls.payment_methods), 50),
                        shipping_address=cls.safe_length(address, 200),
                        shipping_city=cls.safe_length(city, 50),
                        shipping_state=cls.safe_length(state, 50),
                        shipping_zip=cls.safe_length(zip_code, 20),
                        last_update=order_date + timedelta(minutes=random.randint(1, 60))
                    )
                    
                    all_orders.append(order)
                    order_count += 1
                    
                    # Add order to session and flush to get order_id
                    session.add(order)
                
                # Commit in batches to avoid large transactions
                if order_count % 50 == 0:
                    session.commit()
            
            # Final commit to ensure all orders are saved
            session.commit()
            print(f"Successfully generated {order_count} orders.")
            return all_orders
            
        except Exception as e:
            session.rollback()
            print(f"Error generating orders: {e}")
            return []