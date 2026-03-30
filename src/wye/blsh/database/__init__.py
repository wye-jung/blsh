import pandas as pd
from sqlalchemy import create_engine, delete as _delete, select, text
from sqlalchemy.orm import Session


def _make_engine():
    from wye.blsh.common.env import DB_URL
    return create_engine(DB_URL)


class _LazyEngine:
    """DB 연결을 첫 사용 시점에 생성 (import 시 연결 없음)."""

    def __init__(self):
        self._engine = None

    def _get(self):
        if self._engine is None:
            self._engine = _make_engine()
        return self._engine

    # SQLAlchemy Engine 위임 메서드
    def connect(self):
        return self._get().connect()

    def raw_connection(self):
        return self._get().raw_connection()

    def dispose(self):
        if self._engine is not None:
            self._engine.dispose()

    # Session(engine) 용 dialect 접근
    def __getattr__(self, name):
        return getattr(self._get(), name)


engine = _LazyEngine()

import atexit
atexit.register(lambda: engine.dispose())


def create(table_name, df, if_exists="append"):
    with engine.connect() as conn:
        df.to_sql(table_name, con=conn, if_exists=if_exists, index=False)
        conn.commit()
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
