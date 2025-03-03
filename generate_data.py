from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
from faker import Faker
import random
import argparse
import decimal

# Update this import to match your table creation script filename
# If your table creation script is named create_tables.py:
from create_tables import Customer, Order, OrderItem, create_tables

# Initialize Faker to generate realistic looking data
fake = Faker()


def generate_and_insert_data(num_customers=100, max_orders_per_customer=5, max_items_per_order=10):
    """
    Generate and insert fake data into the PostgreSQL database using SQLAlchemy.

    Args:
        num_customers (int): Number of customers to generate
        max_orders_per_customer (int): Maximum number of orders per customer
        max_items_per_order (int): Maximum number of items per order
    """
    # Create engine and get a session
    engine = create_tables()  # This ensures tables exist
    Session = sessionmaker(bind=engine)
    session = Session()

    # Helper function to ensure field values don't exceed column size limits
    def safe_length(value, max_length):
        if isinstance(value, str) and len(value) > max_length:
            return value[:max_length]
        return value

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

    order_statuses = ["pending", "processing", "shipped", "delivered", "cancelled"]
    payment_methods = ["Credit Card", "PayPal", "Apple Pay", "Google Pay", "Bank Transfer"]

    try:
        # Generate customers
        print(f"Generating {num_customers} customers...")
        customers = []

        for _ in range(num_customers):
            first_name = fake.first_name()
            last_name = fake.last_name()
            email = f"{first_name.lower()}.{last_name.lower()}@{fake.domain_name()}"

            customer = Customer(
                first_name=first_name,
                last_name=last_name,
                email=email,
                phone=safe_length(fake.phone_number(), 20),
                address=safe_length(fake.street_address(), 200),
                city=safe_length(fake.city(), 50),
                state=safe_length(fake.state_abbr(), 50),
                zip_code=safe_length(fake.zipcode(), 20),
                registration_date=fake.date_time_between(start_date="-2y", end_date="now")
            )

            customers.append(customer)

        # Add all customers to the session
        session.add_all(customers)
        session.commit()

        print(f"Successfully generated {num_customers} customers.")

        # Generate orders and order items
        print("Generating orders and order items...")
        order_count = 0
        item_count = 0

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

                # Select status based on order date (more recent orders more likely to be pending/processing)
                days_ago = (datetime.now() - order_date).days
                if days_ago < 2:
                    status_weights = [0.7, 0.3, 0, 0, 0]  # Mostly pending or processing
                elif days_ago < 5:
                    status_weights = [0.1, 0.4, 0.5, 0, 0]  # Mostly processing or shipped
                elif days_ago < 14:
                    status_weights = [0, 0.1, 0.3, 0.6, 0]  # Mostly delivered
                else:
                    status_weights = [0, 0, 0.1, 0.8, 0.1]  # Mostly delivered, some cancelled

                status = random.choices(order_statuses, weights=status_weights)[0]

                # Generate random items for this order
                num_items = random.randint(1, max_items_per_order)
                order_items = []
                total_amount = decimal.Decimal('0.00')

                # Create the order
                order = Order(
                    customer_id=customer.customer_id,
                    order_date=order_date,
                    status=status,
                    payment_method=safe_length(random.choice(payment_methods), 50),
                    shipping_address=safe_length(address, 200),
                    shipping_city=safe_length(city, 50),
                    shipping_state=safe_length(state, 50),
                    shipping_zip=safe_length(zip_code, 20),
                    last_update=order_date + timedelta(minutes=random.randint(1, 60))
                )

                # Generate items for this order
                for _ in range(num_items):
                    product = random.choice(products)
                    quantity = random.randint(1, 3)
                    discount = decimal.Decimal(str(round(random.uniform(0, 0.15), 2)))  # 0-15% discount
                    unit_price = product["price"]
                    item_price = unit_price * (1 - discount)
                    item_total = item_price * quantity
                    total_amount += item_total

                    order_item = OrderItem(
                        product_name=safe_length(product["name"], 100),
                        product_id=safe_length(product["id"], 50),
                        quantity=quantity,
                        unit_price=unit_price,
                        discount=discount,
                        last_update=order_date + timedelta(minutes=random.randint(1, 5))
                    )

                    order_items.append(order_item)

                # Set the total amount on the order
                order.total_amount = total_amount.quantize(decimal.Decimal('0.01'))

                # Add the order to the session
                session.add(order)
                session.flush()  # This assigns the order_id

                # Set the order_id for each item and add them to the session
                for item in order_items:
                    item.order_id = order.order_id
                    session.add(item)

                order_count += 1
                item_count += len(order_items)

                # Commit in batches to avoid large transactions
                if order_count % 50 == 0:
                    session.commit()

        # Final commit
        session.commit()
        print(f"Successfully generated {order_count} orders and {item_count} order items.")

    except Exception as e:
        session.rollback()
        print(f"Error: {e}")
    finally:
        session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate sample data for the e-commerce database.')
    parser.add_argument('--customers', type=int, default=100, help='Number of customers to generate')
    parser.add_argument('--max-orders', type=int, default=5, help='Maximum number of orders per customer')
    parser.add_argument('--max-items', type=int, default=10, help='Maximum number of items per order')

    args = parser.parse_args()

    generate_and_insert_data(
        num_customers=args.customers,
        max_orders_per_customer=args.max_orders,
        max_items_per_order=args.max_items
    )