from sqlalchemy import select,func
from src.config.database import Session,Base,engine
from src.models.entities import User,Node,RequestSet,Trial
from src.utils.security import password_hash

def seed():
    Base.metadata.create_all(engine)
    with Session() as db:
        for role in ('admin','operator','viewer'):
            email=role+'@qiyuan.test'
            if not db.scalar(select(User).where(User.email==email)):db.add(User(email=email,password=password_hash('Demo2026!'),role=role))
        for name,role,pos,delay,note in [('harbor-primary','primary',105,0,'栖原主站 · 写入航道'),('north-replica','replica',104,150,'北站副本 · 延迟偏高'),('south-replica','replica',105,20,'南站副本 · 常用读航道')]:
            if not db.scalar(select(Node).where(Node.name==name)):db.add(Node(name=name,role=role,status='healthy',lineage='qiyuan-log-01',position=pos,delay_ms=delay,note=note))
        mixed=[{'at_ms':0,'kind':'write','session':'lab-A'},{'at_ms':100,'kind':'read','session':'lab-A'},{'at_ms':600,'kind':'replicate','node':'south-replica','position':106,'delay_ms':20},{'at_ms':700,'kind':'read','session':'lab-A'},{'at_ms':800,'kind':'node_down','node':'south-replica'},{'at_ms':900,'kind':'read','session':'lab-A'},{'at_ms':1000,'kind':'node_down','node':'harbor-primary'},{'at_ms':1100,'kind':'read','session':'lab-A'}]
        divergence=[
            {'at_ms':0,'kind':'write','session':'lab-A'},
            {'at_ms':100,'kind':'write','session':'lab-A'},
            {'at_ms':150,'kind':'replicate','node':'north-replica','position':107,'delay_ms':90},
            {'at_ms':200,'kind':'diverge','node':'south-replica','position':104},
            {'at_ms':600,'kind':'read','session':'lab-A'},
            {'at_ms':700,'kind':'backfill','node':'south-replica','from_position':105,'to_position':106},
            {'at_ms':750,'kind':'read','session':'lab-A'},
            {'at_ms':800,'kind':'backfill','node':'south-replica','from_position':104,'to_position':104},
            {'at_ms':850,'kind':'backfill','node':'south-replica','from_position':104,'to_position':105},
            {'at_ms':870,'kind':'backfill','node':'south-replica','from_position':104,'to_position':106},
            {'at_ms':900,'kind':'read','session':'lab-A'},
            {'at_ms':950,'kind':'backfill','node':'south-replica','from_position':107,'to_position':107},
            {'at_ms':1000,'kind':'read','session':'lab-A'},
            {'at_ms':1050,'kind':'inspect','node':'south-replica'},
            {'at_ms':1100,'kind':'repair_done','node':'south-replica'},
            {'at_ms':1150,'kind':'read','session':'lab-A'}
        ]
        stage1=[{'at_ms':0,'kind':'diverge','node':'south-replica','position':104},
                {'at_ms':10,'kind':'backfill','node':'south-replica','from_position':105,'to_position':105},
                {'at_ms':20,'kind':'read','session':'lab-A'}]
        stage2=[{'at_ms':0,'kind':'read','session':'lab-A'},
                {'at_ms':10,'kind':'backfill','node':'south-replica','from_position':104,'to_position':104},
                {'at_ms':20,'kind':'read','session':'lab-A'},
                {'at_ms':30,'kind':'repair_done','node':'south-replica'}]
        cases=[('QY-1117-A','交接班写后读与双节点故障',mixed,'栖原站点值班交接 · 09:15 请求轨迹'),
               ('QY-1117-B','延迟超标回退主库',[{'at_ms':0,'kind':'node_down','node':'south-replica'},{'at_ms':10,'kind':'read','session':'visitor'}],'副本停机时的回退复核'),
               ('QY-1117-C','双副本不可读时拒绝',[{'at_ms':0,'kind':'write','session':'lab-B'},{'at_ms':1000,'kind':'node_down','node':'harbor-primary'},{'at_ms':1100,'kind':'read','session':'lab-B'}],'主库故障且副本未追平的拒绝案例'),
               ('QY-1117-D','同一位点内容分叉：乱序补传与恢复接读',divergence,'分叉发现→乱序/重复补传→恢复接读全流程'),
               ('QY-1117-E1','分叉第一批补传（E2 的基线）',stage1,'先跑本集；结束时保留未完成的修复台账'),
               ('QY-1117-E2','补传之间新开读试验：按当时已核对进度选路',stage2,'运行试验时选择基线的「接续已核对进度」= QY-1117-E1 的那次试验')]
        for code,title,events,note in cases:
            if not db.scalar(select(RequestSet).where(RequestSet.code==code)):db.add(RequestSet(code=code,title=title,events=events,note=note))
        db.commit();print({cls.__tablename__:db.scalar(select(func.count()).select_from(cls)) for cls in (User,Node,RequestSet,Trial)})
if __name__=='__main__':seed()
