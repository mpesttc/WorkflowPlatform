import sys
import os
from sqlalchemy import select, create_engine, text
from sqlalchemy.orm import sessionmaker

# Add the parent directory to Python path to enable absolute imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.MySQL.models.model_345 import Beaker


DB_HOST = 'localhost'
DB_PORT = 3306
DB_USER = 'root'
DB_PASSWORD = 'root'
DB_NAME = 'demo-db-345'

DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


engine = create_engine(DATABASE_URI)
Session_maker = sessionmaker(bind=engine)

def read_from_db():
    with Session_maker() as session:
        try:
            query = select(Beaker)
            result = session.execute(query)
            beakers = result.scalars().all()
            for beaker in beakers:
                print(beaker.id, beaker.vbatt)
        except Exception as e:
            print(f"Error: {e}")

def write_to_db():
    with Session_maker() as session:
        with session.begin():
            try:
                query = select(Beaker) #Insert
                result = session.execute(query)
                beakers = result.scalars().all()
                for beaker in beakers:
                    print(beaker.id, beaker.vbatt)
            except Exception as e:
                print(f"Error: {e}")

if __name__ == '__main__':
    read_from_db()