import os,logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from sqlalchemy import text
from src.config.database import Base,engine
from src.models import entities
from src.controllers.auth import router as auth
from src.routes import nodes,request_sets,trials,audit
from src.middlewares.errors import ErrorMiddleware
from src.middlewares.request_id import RequestIdMiddleware
def _upgrade_schema():
    """Additive column guard for existing demo volumes (no migration engine)."""
    if engine.dialect.name != 'mysql': return
    with engine.connect() as connection:
        columns={row[0] for row in connection.execute(text("SELECT column_name FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='trials'"))}
        if 'repair_mode' not in columns:
            connection.execute(text("ALTER TABLE trials ADD COLUMN repair_mode VARCHAR(20) NOT NULL DEFAULT 'verified'"))
        if 'baseline_trial_id' not in columns:
            connection.execute(text("ALTER TABLE trials ADD COLUMN baseline_trial_id INT NULL"))
            connection.execute(text("ALTER TABLE trials ADD CONSTRAINT fk_trials_baseline FOREIGN KEY (baseline_trial_id) REFERENCES trials(id)"))
        connection.commit()
@asynccontextmanager
async def lifespan(app):
    Base.metadata.create_all(engine)
    try: _upgrade_schema()
    except Exception: logging.exception('schema upgrade skipped')
    if os.getenv('SEED_DEMO','true')=='true':
        try:
            from scripts.seed_demo import seed
            seed()
        except Exception: logging.exception('seed failed; manual retry is available')
    yield
app=FastAPI(title='站点读流量路由演练',lifespan=lifespan)
app.add_middleware(ErrorMiddleware);app.add_middleware(RequestIdMiddleware)
for r in (auth,nodes.router,request_sets.router,trials.router,audit.router): app.include_router(r)
@app.get('/health')
def health():
    with engine.connect() as c: c.execute(text('SELECT 1'))
    from src.services.trials import queue
    queue.ping()
    return {'status':'healthy'}
