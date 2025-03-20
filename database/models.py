from datetime import datetime
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, CheckConstraint, Numeric

from database.db_manager import Base

# Define all models here
class Customer(Base):
    __tablename__ = 'customers'

    customer_id = Column(Integer, primary_key=True)
    first_name = Column(String(50), nullable=False)
    last_name = Column(String(50), nullable=False)
    email = Column(String(100), nullable=False, unique=True)
    phone = Column(String(20))
    address = Column(String(200))
    city = Column(String(50))
    state = Column(String(50))
    zip_code = Column(String(20))
    registration_date = Column(DateTime, default=datetime.now)
    last_update = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # Relationship with Order
    orders = relationship("Order", back_populates="customer")

    def __repr__(self):
        return f"<Customer(customer_id={self.customer_id}, name='{self.first_name} {self.last_name}')>"


class Order(Base):
    __tablename__ = 'orders'

    order_id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, ForeignKey('customers.customer_id'))
    order_date = Column(DateTime, default=datetime.now)
    status = Column(String(20),
                    CheckConstraint("status IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled')"),
                    default='pending')
    total_amount = Column(Numeric(10, 2), nullable=False)
    payment_method = Column(String(50))
    shipping_address = Column(String(200))
    shipping_city = Column(String(50))
    shipping_state = Column(String(50))
    shipping_zip = Column(String(20))
    last_update = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # Relationships
    customer = relationship("Customer", back_populates="orders")
    items = relationship("OrderItem", back_populates="order")

    def __repr__(self):
        return f"<Order(order_id={self.order_id}, customer_id={self.customer_id}, total_amount={self.total_amount})>"


class OrderItem(Base):
    __tablename__ = 'order_items'

    item_id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey('orders.order_id'))
    product_name = Column(String(100), nullable=False)
    product_id = Column(String(50))
    quantity = Column(Integer, CheckConstraint("quantity > 0"), nullable=False)
    unit_price = Column(Numeric(10, 2), nullable=False)
    discount = Column(Numeric(5, 2), default=0.00)
    last_update = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # Relationship with Order
    order = relationship("Order", back_populates="items")

    def __repr__(self):
        return f"<OrderItem(item_id={self.item_id}, order_id={self.order_id}, product='{self.product_name}')>"