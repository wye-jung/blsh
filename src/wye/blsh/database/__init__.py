import pandas as pd
from sqlalchemy import create_engine, delete as _delete, select, text
from sqlalchemy.orm import Session
from wye.blsh.common.env import DB_URL

engine = create_engine(DB_URL)


def create(table_name, df, if_exists="append"):
    df.to_sql(table_name, con=engine, if_exists=if_exists, index=False)
    print(f"Inserted {len(df)} rows into {table_name} table")


def select_one(sql, **params):
    with Session(engine) as session:
        return session.execute(text(sql), params).mappings().one()


def select_first(sql, **params):
    with Session(engine) as session:
        return session.execute(text(sql), params).mappings().first()


def select_all(sql, **params):
    with Session(engine) as session:
        return session.execute(text(sql), params).mappings().all()


def execute_batch(sql, data_iter):
    from psycopg2.extras import execute_batch as sa_execute_batch

    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            sa_execute_batch(cur, sql, data_iter)
        conn.commit()
    finally:
        conn.close()


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

    def update(self, values: dict, **filters):
        with Session(engine) as session:
            session.query(self.model).filter_by(**filters).update(values)
            session.commit()

    def delete(self, **filters):
        with Session(engine) as session:
            stmt = _delete(self.model).filter_by(**filters)
            result = session.execute(stmt)
            session.commit()
            print(
                f"Deleted {result.rowcount} rows from {self.model.__tablename__} table"
            )
            return result.rowcount
