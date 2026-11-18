export const ROLES={admin:'管理员',operator:'操作员',viewer:'观察员'};
export const RUN_STATES={queued:'排队中',running:'执行中',completed:'已完成',failed:'失败'};
export const REPAIR_MODES={verified:'按已核对进度接读',pause:'修复期间停读',position:'仅按位点（演示误读）'};
export const EVENT_KINDS={write:'写入',read:'读取',replicate:'副本复制',node_down:'节点下线',node_up:'节点恢复',diverge:'内容分叉',backfill:'补传批次',inspect:'核对进度',repair_done:'修复完成接读'};
export const DECISIONS={routed:'已路由',rejected:'拒绝',applied:'已应用',detected:'发现分叉',inspected:'已核对',buffered:'先到暂存',noop:'重复未追加',resumed:'恢复接读',blocked:'暂缓恢复'};
