import pandas as pd
from sqlalchemy import create_engine, delete as sa_delete, select
from sqlalchemy.orm import Session
from ..common.env import DB_URL
from .models import Base

engine = create_engine(DB_URL)


def create_tables():
    Base.metadata.create_all(bind=engine)


def create(table_name, df):
    df.to_sql(table_name, con=engine, if_exists="append", index=False)
    print(f"Inserted {len(df)} rows into {table_name} table")


class ModelManager:
    def __init__(self, model):
        self.model = model

    def create(self, df):
        create(self.model.__tablename__, df)

    def read(self, **filters) -> pd.DataFrame:
        with Session(engine) as session:
            stmt = select(self.model).filter_by(**filters)
            rows = session.execute(stmt).scalars().all()
            return pd.DataFrame([row.__dict__ for row in rows]).drop(
                columns=["_sa_instance_state"], errors="ignore"
            )

    def delete(self, **filters):
        with Session(engine) as session:
            stmt = sa_delete(self.model).filter_by(**filters)
            result = session.execute(stmt)
            session.commit()
            print(
                f"Deleted {result.rowcount} rows from {self.model.__tablename__} table"
            )
