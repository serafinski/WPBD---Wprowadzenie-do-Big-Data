import argparse
from database.db_manager import DatabaseManager
from database.dbo.customers import CustomerModel
from database.dbo.orders import OrderModel
from database.dbo.orders_items import OrderItemModel
from database.models import Customer

DB_URL = "postgresql+pg8000://postgres:password@localhost:5432/postgres"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate sample data for the e-commerce database.')
    parser.add_argument('--customers', type=int, default=100, help='Number of customers to generate')
    parser.add_argument('--max-orders', type=int, default=5, help='Maximum number of orders per customer')
    parser.add_argument('--max-items', type=int, default=10, help='Maximum number of items per order')
    parser.add_argument('--customers-only', action='store_true', help='Generate only customer data')
    parser.add_argument('--orders-only', action='store_true', help='Generate only orders and items for existing customers')

    args = parser.parse_args()
    
    # Create a database manager
    db_manager = DatabaseManager(DB_URL)
    db_manager.create_tables()
    session = db_manager.get_session()
    
    try:
        if args.customers_only:
            # Generate only customers
            CustomerModel.generate_data(session, args.customers)
        
        elif args.orders_only:
            # Generate only orders and items for existing customers
            customers = session.query(Customer).all()  # Use the imported Customer model
            if not customers:
                print("No customers found in the database. Please generate customers first.")
            else:
                print(f"Found {len(customers)} existing customers.")
                orders = OrderModel.generate_data(session, customers, args.max_orders)
                OrderItemModel.generate_data(session, orders, args.max_items)
        
        else:
            # Generate all data
            customers = CustomerModel.generate_data(session, args.customers)
            orders = OrderModel.generate_data(session, customers, args.max_orders)
            OrderItemModel.generate_data(session, orders, args.max_items)
            print("Data generation completed successfully!")
            
    except Exception as e:
        print(f"Error during data generation: {e}")
    finally:
        db_manager.close_session()