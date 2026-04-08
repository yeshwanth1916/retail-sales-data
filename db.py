from sqlalchemy import create_engine, Column, String, Integer, Float, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

DB_NAME = "sqlite:///retail.db"

engine = create_engine(DB_NAME, echo=False)
Base = declarative_base()
Session = sessionmaker(bind=engine)


def get_connection():
    return engine.connect()


class RawSales(Base):
    __tablename__ = "raw_sales"
    
    order_id = Column(String, primary_key=True)
    order_date = Column(String)
    store_id = Column(String)
    product_id = Column(String)
    category = Column(String)
    quantity = Column(Integer)
    unit_price = Column(Float)
    file_name = Column(String)
    
    class Config:
        from_attributes = True


class CleanSales(Base):
    __tablename__ = "clean_sales"
    
    order_id = Column(String, primary_key=True)
    order_date = Column(Date)
    store_id = Column(String)
    product_id = Column(String)
    category = Column(String)
    quantity = Column(Integer)
    unit_price = Column(Float)
    total_amount = Column(Float)
    order_month = Column(String)
    order_day = Column(String)
    file_name = Column(String)
    
    class Config:
        from_attributes = True


class AggSales(Base):
    __tablename__ = "agg_sales"
    
    store_id = Column(String, primary_key=True)
    category = Column(String, primary_key=True)
    order_month = Column(String, primary_key=True)
    total_sales = Column(Float)


def get_session():
    return Session()


def create_tables():
    Base.metadata.create_all(engine)
