from sqlalchemy import select
from redis import Redis
from fastapi import HTTPException
from src.models.entities import Node,RequestSet,Trial
from src.config.settings import REDIS_URL
from src.constants.enums import RUN_STATES
from src.utils.serialization import record
from .common import save
from .routing import simulate
queue=Redis.from_url(REDIS_URL,decode_responses=True)

def _carry_over(nodes, baseline):
    """Seed each replica with the repaired log ledger from a previous trial.

    Backfill between read trials is what the operator is rehearsing: the new
    trial must route on the progress verified so far, not re-derive from the
    node registry.  Only content the baseline actually holds is carried.
    """
    if not baseline or not baseline.get('result'):return nodes
    prior={x['name']:x for x in baseline['result'].get('final_nodes',[])}
    carried=[]
    for node in nodes:
        source=prior.get(node['name'])
        if not source or source.get('role')!='replica':carried.append(node);continue
        seeded=dict(node)
        seeded['content_segments']=source.get('content_segments') or []
        seeded['position']=max(node['position'],int(source.get('position') or 0))
        if source.get('repair'):seeded['repair']=source['repair']
        carried.append(seeded)
    return carried

def listing(db):return [record(x) for x in db.scalars(select(Trial).order_by(Trial.id.desc()))]
def detail(db,ident):
    obj=db.get(Trial,ident)
    if not obj:raise HTTPException(404,'试验不存在')
    data=record(obj)
    if obj.baseline_trial_id:
        base=db.get(Trial,obj.baseline_trial_id)
        data['baseline']=None if not base else {'id':base.id,'name':base.name,'status':base.status}
    return data
def create(db,data,user):
    req=db.get(RequestSet,data.request_set_id)
    if not req:raise HTTPException(404,'请求集不存在')
    baseline=None
    if data.baseline_trial_id:
        base=db.get(Trial,data.baseline_trial_id)
        if not base:raise HTTPException(404,'基线试验不存在')
        if base.status!='completed':raise HTTPException(422,'基线试验尚未完成，不能按它的进度接续')
        baseline={'id':base.id,'name':base.name,'result':base.result}
    nodes=[record(x) for x in db.scalars(select(Node).order_by(Node.id))]
    seeded=_carry_over(nodes,baseline)
    try:simulate(seeded,req.events,data.max_delay_ms,data.sticky_ms,data.repair_mode)
    except ValueError as e:raise HTTPException(422,str(e))
    snapshot={'nodes':nodes,'request_set':record(req),'repair_mode':data.repair_mode,
              'baseline_trial_id':data.baseline_trial_id,
              'seeded_nodes':seeded}
    obj=Trial(**data.model_dump(),input_snapshot=snapshot,status=RUN_STATES[0]);result=save(db,obj,user,'路由演练入队 '+data.name)
    try:queue.rpush('replicaroute:queue',str(obj.id))
    except Exception:
        obj.status='failed';obj.error='队列暂时不可用，请重新创建';db.commit();raise HTTPException(503,obj.error)
    return result
