from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import declarative_base, sessionmaker

# Create a base class for our models
Base = declarative_base()

class DatabaseManager:
    """Class to manage database operations."""
    
    def __init__(self, db_url):
        self.db_url = db_url
        self.engine = None
        self.session = None
    
    def create_tables(self):
        """Create tables if they don't exist and return the engine."""
        self.engine = create_engine(self.db_url)

        # Make sure all models are imported and registered with Base
        self._import_all_models()
        
        # Check if tables exist
        inspector = inspect(self.engine)
        existing_tables = inspector.get_table_names()
        tables_to_create = ['customers', 'orders', 'order_items']

        # Identify which tables need to be created
        new_tables = [table for table in tables_to_create if table not in existing_tables]

        if new_tables:
            print(f"Creating tables: {', '.join(new_tables)}")
            Base.metadata.create_all(self.engine)
            print("Tables created successfully!")
        else:
            print("All tables already exist. Skipping table creation.")

        return self.engine
    
    def _import_all_models(self):
        """Import all models to ensure they're registered with SQLAlchemy."""
        # This import is necessary for SQLAlchemy to know about our models
        # ruff: noqa F401
        from database.models import Customer, Order, OrderItem
        # The import might appear unused, but it's essential for SQLAlchemy
    
    def get_session(self):
        """Create and return a new session."""
        if not self.engine:
            self.create_tables()
        
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        return self.session
    
    def close_session(self):
        """Close the current session if it exists."""
        if self.session:
            self.session.close()
            self.session = None