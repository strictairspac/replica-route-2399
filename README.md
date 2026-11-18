# 站点读流量路由演练

栖原站点运维组（11人）在交接班前，用节点位点与会话时序复核读写路由，明确何时读副本、何时回主库、何时必须拒绝读取。

## 场景与模块

原先值班员在配置表和聊天记录中核对副本延迟，以“延迟很低”推断写后读安全。痛点：低延迟不等于已应用会话写位点、故障时回退策略不清、决策无逐请求证据。上游输入为人工确认的节点状态和请求轨迹，本站承接路由规则演练与证据留档，下游为站点值班与变更复核人员。

三个核心实体：复制节点 Node（主副角色/在线状态/日志谱系/位点/延迟），会话请求集 RequestSet（带时序的读写、复制应用、故障恢复、内容分叉与补传事件），路由试验 Trial（冻结输入、策略参数与结果）。4业务页+登录页，另有管理员审计。

**这是受控复制路由模型，不连接真实主从集群。** MySQL真实保存节点、请求、任务及证据；Python真实逐事件计算策略。日志位点采用命名谱系中的单调整数，并附内容版本令牌：位点相同不代表内容一致，副本内容分叉会被识别并分段修复。未实现完整GTID集合/真实复制协议。

## 快速启动

```sh
cp .env.example .env
docker compose up -d --build
docker compose exec -T backend python -m scripts.seed_demo
```

前端 http://localhost:18247 ，后端 http://localhost:19247/docs ，MySQL localhost:17747。

|角色|账号|密码|权限|
|---|---|---|---|
|admin|admin@qiyuan.test|Demo2026!|全部业务写入、运行与审计|
|operator|operator@qiyuan.test|Demo2026!|节点/请求编辑、运行、导出|
|viewer|viewer@qiyuan.test|Demo2026!|只读和导出，写API403|

## 一天的工作剧本

09:00 operator登录；09:05查看harbor-primary位点105、north-replica位点104/150ms、south-replica位点105/20ms；09:10打开 `QY-1117-A` 请求集；09:15确认八事件时序；09:20以100ms延迟门槛和500ms粘滞执行 `shift-20261117`；09:25检查：写入主库位点106、100ms读仍走主库、600ms南站追平106、700ms读走南站、800ms南站故障、900ms回主库、1000ms主库故障、1100ms拒绝读取；09:30导出route-1.json；09:40 viewer核验拒绝理由；10:00 admin看审计。

另有 `QY-1117-B` 高延迟副本回退主库、`QY-1117-C` 主库故障且副本未追平的拒绝案例。`QY-1117-D` 演练同一位点内容分叉：200ms 发现南站自位点104内容不一致并保留1–103已对上日志；600/750ms 读改走北站；700ms 的 105–106 批次先到暂存、800ms 的 104–104 补齐缺口后才推进已核对前缀；850/870ms 重复提交不追加；950ms 补 107；1000ms 待修副本按已核对进度接读；1050ms 核对一致；1100ms 恢复接读。`QY-1117-E1`/`QY-1117-E2` 演示补传之间新开读试验：E2 以 E1 结束时保存的修复台账为基线（试验表单的「接续已核对进度」），按当时已核对前缀选路，不必重放第一段补传；节点台账本身不被改写。所有试验使用冻结快照，不会把模型故障写回节点台账。

## 真实计算规则

输入上限12节点、100事件；时间0–86400000ms，事件按at_ms非递减，时间相同按输入顺序执行。必须有且仅一个主节点，同谱系副本初始位点不能超过主库。事件字段：

```json
[
  {"at_ms":0,"kind":"write","session":"lab-A"},
  {"at_ms":100,"kind":"read","session":"lab-A"},
  {"at_ms":600,"kind":"diverge","node":"south-replica","position":104},
  {"at_ms":650,"kind":"backfill","node":"south-replica","from_position":105,"to_position":106},
  {"at_ms":700,"kind":"read","session":"lab-A"},
  {"at_ms":750,"kind":"backfill","node":"south-replica","from_position":104,"to_position":104},
  {"at_ms":800,"kind":"inspect","node":"south-replica"},
  {"at_ms":850,"kind":"repair_done","node":"south-replica"}
]
```

- write：主库在线才成功；主库位点+1，记录该会话required_position和last_write_ms。失败写入不推进会话位点。
- read：若距最近成功写入 `< sticky_ms`，必须主库在线、同谱系且位点满足；否则拒绝。到达边界即结束粘滞。
- 粘滞期外：副本必须在线、同谱系、position≥会话required_position、delay_ms≤max_delay_ms，四条件同时成立。多个候选按延迟升序、位点降序、名称字典序选择。无副本达标则回主库；主库也失效则拒绝。
- replicate：只允许在线同谱系副本；位点不能回退、不能超过主库已提交位点。node_down/node_up修改模型状态。未知节点或非法复制在入队前422。
- 内容分叉：diverge 只允许副本，把位点 position（默认副本当前已应用位点）至已应用末端的版本改写为该副本的分叉令牌，并记录首个分叉位点与保留的已对上前缀；replicate 不得用于修复中的副本。
- 补传：backfill 需要 from_position/to_position，区间不得超过主库已提交位点。新试验可指定 baseline_trial_id 接续上一次结果里的修复台账（内容分段与批次记录），只允许接续已完成的试验；未指定时从节点台账的初始内容开始。批次按位点幂等写入，重复提交不再追加；先到的区间可以暂存，只有从1连续覆盖的“已核对前缀”会推进。补传期间其他副本照常接读，待修副本按 Trial 的 repair_mode 处理：verified 只按已核对前缀接读；pause 修复期间停读并回退主库；position 仅按位点接读（用于演示相同位点读错内容）。
- 恢复接读：repair_done 要求已核对前缀覆盖主库最新位点，否则记 blocked 并继续按已核对进度处理；满足后才把副本重新作为常规读副本。
- 读内容核对：每个被路由的读都记录读到位点的版本令牌和主库同一位点的版本；两者不一致即标为不安全，即便位点满足会话要求。
- 每个会话独立维护位点；从未成功写入的会话required=0，允许最终一致副本读取，不承诺全局强一致。低延迟绝不覆盖落后位点，相同位点也绝不覆盖内容分叉。

输出每事件目标/拒绝、会话所需位点、实际位点、各副本资格（含内容一致与待修状态）、读到的版本与主库版本、修复台账（分叉点/保留前缀/每批补传结果/重复与乱序计数/恢复依据）、最终节点、会话状态、汇总和SHA256。相同输入产生相同摘要；不依赖随机数或外部网络。拒绝不安全读是正常业务结果，不算任务失败。快照与结果保存在MySQL，Redis只传递任务ID。

## 工程说明

|服务|技术|职责|
|---|---|---|
|frontend|原生HTML/CSS/JS + Nginx|4业务页、编辑表单、共享组件、/api代理|
|backend|Python3.11/FastAPI/SQLAlchemy/PyJWT|校验、权限、审计、快照与导出|
|worker|Python3.11|会话时序与路由算法|
|db|MySQL8.0|真实持久化业务和JSON证据|
|redis|Redis7|持久队列|

各实体贯穿 `backend/src/{models,schemas,services,routes}` 和 `frontend/src/{api,stores,pages}`；其余强制目录含controllers/middlewares/config/constants/utils、components/common/hooks/types/router。三组件table/badge/frame共用；hooks/session/feedback和utils/forms/html共用；request_id/errors双中间件。

共享枚举角色admin/operator/viewer与任务queued/running/completed/failed位于 `backend/src/constants/enums.py` / `frontend/src/constants/enums.js`，security/trials、frame/badge消费。节点角色primary/replica和状态healthy/down由node schema及前端表单定义，事件类型由request_set schema及路由算法定义。

环境变量见.env.example：COMPOSE_PROJECT_NAME、三端口、DB_PASSWORD、JWT_SECRET、SEED_DEMO。默认密码仅本地演示、JWT8小时；seed自然键仅补缺失，不覆盖修改。schema由metadata.create_all初始建表；启动时会对旧卷做仅新增列的兼容升级（trials.repair_mode、trials.baseline_trial_id），仍无通用在线迁移引擎。5服务均健康检查，backend/worker依赖健康db/redis，frontend依赖backend；数据库、Redis、worker产物使用命名卷。

本地开发：Python3.11安装backend/requirements.txt、配置DATABASE_URL/REDIS_URL，在backend运行 `uvicorn src.main:app --reload` 和 `python -m src.worker`，前端经Nginx同源代理。路由模型自检：`cd backend && python -m pytest tests/test_routing_repair.py`（覆盖乱序、重复、停读/误读与恢复门槛）。停止清空：`docker compose down -v --remove-orphans`。

限制：单worker串行、无生产任务重试/崩溃续跑；不模拟网络随机抖动、自动选主或完整GTID集合。内容版本用确定性令牌表达，不存储真实行数据；修复只在本模型的日志区间上演练。Google Fonts离线回退，业务不依赖字体网络。

## License

MIT。源码外 `../../test-artifacts/gb-4108/` 含真实测试、截图与结果。
