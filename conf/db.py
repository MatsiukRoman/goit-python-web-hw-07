from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

URI = f"sqlite:///university.db"
engine = create_engine(URI, echo=False, pool_size=5, max_overflow=0)

DBSession = sessionmaker(bind=engine)
session = DBSession()
