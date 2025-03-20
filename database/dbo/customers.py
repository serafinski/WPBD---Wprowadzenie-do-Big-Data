from faker import Faker
from database.models import Customer

fake = Faker()

class CustomerModel:
    """Class that combines customer model definition and data generation."""

    Customer = Customer
    
    @staticmethod
    def safe_length(value, max_length):
        """Ensure field values don't exceed column size limits."""
        if isinstance(value, str) and len(value) > max_length:
            return value[:max_length]
        return value
    
    @classmethod
    def generate_data(cls, session, num_customers=100):
        """Generate and insert customer records."""
        try:
            print(f"Generating {num_customers} customers...")
            customers = []

            for _ in range(num_customers):
                first_name = fake.first_name()
                last_name = fake.last_name()
                email = f"{first_name.lower()}.{last_name.lower()}@{fake.domain_name()}"

                customer = cls.Customer(
                    first_name=first_name,
                    last_name=last_name,
                    email=email,
                    phone=cls.safe_length(fake.phone_number(), 20),
                    address=cls.safe_length(fake.street_address(), 200),
                    city=cls.safe_length(fake.city(), 50),
                    state=cls.safe_length(fake.state_abbr(), 50),
                    zip_code=cls.safe_length(fake.zipcode(), 20),
                    registration_date=fake.date_time_between(start_date="-2y", end_date="now")
                )

                customers.append(customer)

            # Add all customers to the session
            session.add_all(customers)
            session.commit()

            print(f"Successfully generated {num_customers} customers.")
            return customers
            
        except Exception as e:
            session.rollback()
            print(f"Error generating customers: {e}")
            return []