import {table} from '../components/common/table.js';
import {badge} from '../components/common/badge.js';
import {esc,date} from '../utils/html.js';
import {EVENT_KINDS,REPAIR_MODES,DECISIONS} from '../constants/enums.js';

const kindLabel=kind=>EVENT_KINDS[kind]||kind;
const decisionLabel=d=>DECISIONS[d]||d;
const versionTag=v=>v===null||v===undefined?'缺口':(v==='main'?'主库版本':esc(v));

function segmentChart(node){
  const segments=(node.content_segments||[]).slice().sort((a,b)=>a[0]-b[0]);
  if(!segments.length)return '<div class="empty">该节点没有已应用日志。</div>';
  const total=Math.max(...segments.map(s=>s[1]),node.position||1);
  return `<div class="segments">${segments.map(([start,stop,version])=>{
    const width=Math.max(6,(stop-start+1)/total*100);
    const fork=version!=='main';
    return `<div class="segment ${fork?'fork':'ok'}" style="flex:0 0 ${width}%" title="${start}–${stop} · ${versionTag(version)}"><b>${start}–${stop}</b><span>${versionTag(version)}</span></div>`;
  }).join('')}</div><small>位点 ${node.position} · 已核对前缀 ${node.verified_position ?? '—'}${node.divergence?` · 分叉自 ${node.divergence.first_divergent_position}（保留前缀 ${node.divergence.matched_prefix_preserved}）`:''}</small>`;
}

function repairPanel(r){
  if(!r.repairs?.length)return '';
  return `<div class="section-top"><h2>分叉发现与补传修复</h2><span>REPAIR LEDGER / 乱序可重试</span></div>
  <div class="repair-grid">${r.repairs.map(repair=>{
    const batches=repair.batches||[];
    const gate=repair.resumed
      ? `已于 ${repair.resumed_at_ms}ms 恢复接读：已核对 ${repair.verified_position_final} 覆盖主库位点 ${repair.primary_position_final}`
      : repair.repaired
        ? `已核对 ${repair.verified_position_final} 覆盖主库 ${repair.primary_position_final}，尚未发出修复完成事件，仍按待修副本对待`
        : `尚未恢复常规接读：已核对 ${repair.verified_position_final} / 主库 ${repair.primary_position_final}${repair.pending_from?`，缺口自 ${repair.pending_from}`:''}；读请求只按已核对前缀分流`;
    return `<section class="repair-card">
      <div class="repair-head"><div><span class="eyebrow">DIVERGENT REPLICA</span><h3>${esc(repair.node)}</h3></div><span class="badge ${repair.resumed?'completed':repair.repaired?'queued':'failed'}">${repair.resumed?'已恢复接读':repair.repaired?'已追平待恢复':'修复中'}</span></div>
      <div class="key-proof"><strong>发现分叉 @ ${repair.discovered_at_ms}ms</strong><p>位点 ${repair.first_divergent_position} 起内容版本 <code>${esc(repair.fork_tag)}</code> 与主库不一致；保留已对上日志前缀 1–${repair.matched_prefix_preserved}，未整段回滚。当时主库位点 ${repair.primary_position_at_detection}。</p></div>
      <div class="key-proof"><strong>恢复接读依据</strong><p>${esc(gate)}</p></div>
      ${repair.open?`<div class="key-proof"><strong>当前仍在修复</strong><p>已核对前缀 ${repair.verified_position_final}${repair.pending_from?`，缺口自位点 ${repair.pending_from}`:'，等待下一批补传或修复完成事件'}；此期间只有已核对内容可接读。</p></div>`:''}
      <h4>补传批次（可乱序、可重试）</h4>
      ${batches.length?table(['时间','区间','结果','已核对前缀'],batches.map(b=>[`${b.at_ms}ms`, `${b.from}–${b.to}`, b.duplicate?'<span class="badge">重复，未追加</span>':b.out_of_order?'<span class="badge queued">先到暂存</span>':'<span class="badge completed">已写入</span>', `${b.verified_before} → ${b.verified_after}`])):'<div class="empty">尚未补传。</div>'}
      <div class="repair-foot">重复批次 ${repair.duplicates} · 乱序批次 ${repair.out_of_order} · 最终应用位点 ${repair.applied_position_final} · 已核对位点 ${repair.verified_position_final}</div>
    </section>`;
  }).join('')}</div>`;
}

function readEvidence(r){
  const reads=r.trace.filter(e=>e.kind==='read');
  const rows=reads.map(e=>[
    `${e.at_ms}ms`,
    esc(e.session),
    e.decision==='rejected'?'<span class="rejected">拒绝读取</span>':`${esc(e.node)}${e.repairing?' <span class="badge queued">待修接读</span>':''}`,
    `${e.required_position}`,
    e.observed_position??'—',
    e.read_content?(e.read_content.matches?'<span class="badge completed">一致</span>':'<span class="badge failed">不一致</span>'):'—',
    e.read_content?`${versionTag(e.read_content.version)} / ${versionTag(e.read_content.primary_version)}`:'—',
    esc(e.reason)
  ]);
  return `<div class="section-top"><h2>会话读到的内容</h2><span>内容版本与主库对应位点比对</span></div>
  ${table(['时间','会话','读取节点','会话位点','实际位点','内容一致','读到 / 主库版本','决策理由'],rows)}
  <div class="evidence-notes"><section><h3>为什么位点相同仍要核对内容</h3><p>两份日志可以位点相同但内容分叉。读路由先看会话要求位点，再核对实际读到的版本令牌与主库同一位点是否一致；不一致即判不安全。</p></section><section><h3>何时重新接读</h3><p>待修副本按已核对前缀接待读请求；只有补传使已核对前缀覆盖主库最新位点并发出修复完成事件后，才重新作为常规读副本。</p></section></div>`;
}

function checkSummary(e){
  if(!e.replica_checks?.length)return '—';
  const labeled={healthy:'在线',same_lineage:'同谱系',position_sufficient:'位点足',delay_within_limit:'延迟达标',content_matches:'内容一致'};
  return e.replica_checks.map(c=>{
    const fails=Object.entries(labeled).filter(([key])=>!c[key]).map(([,label])=>label);
    const mode=c.repairing?`待修·${c.read_mode}`:'常规';
    return `<div class="replica-check"><b>${esc(c.name)}</b><small>${mode} · 有效位点 ${c.effective_position??'停读'}</small><small>${fails.length?`不达标：${fails.join('、')}`:'四项条件满足'}</small></div>`;
  }).join('');
}

export function evidencePage(trial){
  if(!trial)return '<div class="empty">还没有一致性证据。先登记节点与请求集，再从「路由执行」运行一次演练。</div>';
  const r=trial.result;
  const mode=r?.policy?.repair_read_mode||trial.repair_mode;
  return `<div class="evidence-top"><div><span class="eyebrow">ROUTE EVIDENCE / #${trial.id}</span><h2>${esc(trial.name)} ${badge(trial.status)}</h2><p>${date(trial.created_at)} · 节点与请求快照已冻结</p></div><button id="export" data-id="${trial.id}">导出 JSON ↓</button></div>
  ${trial.error?`<div class="error">${esc(trial.error)}。请检查配置并重新执行。</div>`:''}
  ${r?`<div class="verdict"><div class="verdict-icon">${r.summary.all_routed_reads_safe?'✓':'!'}</div><div><span class="eyebrow">READ-YOUR-WRITES CHECK</span><h2>${r.summary.all_routed_reads_safe?'已路由请求均满足会话位点且内容与主库一致':'存在不安全读取或误读'}</h2><p>成功路由 ${r.summary.routed} / ${r.summary.requests} · 安全拒绝 ${r.summary.rejected} · 副本读取 ${r.summary.replica_reads} · 待修副本接读 ${r.summary.repairing_replica_reads||0} · 内容不一致 ${r.summary.misreads||0}</p></div><div class="equation">${R(r.summary)}</div></div>
  <div class="policy-strip">延迟门槛 ${r.policy.max_delay_ms}ms <span>／</span> 写后读粘滞 ${r.policy.sticky_ms}ms <span>／</span> 修复期策略 ${esc(REPAIR_MODES[mode]||mode)} <span>／</span> 谱系 ${esc(r.lineage)}</div>
  ${trial.baseline?`<div class="notice">本次试验接续 <a href="#evidence/${trial.baseline.id}">#${trial.baseline.id} ${esc(trial.baseline.name)}</a> 结束时的已核对进度开读；补传批次可继续累加，节点台账本身未被改写。</div>`:''}
  ${repairPanel(r)}
  <div class="section-top"><h2>节点日志内容</h2><span>位点 / 版本令牌 / 分叉</span></div>
  <div class="log-map">${r.final_nodes.map(n=>`<div class="log-node ${n.role}"><span class="eyebrow">${n.role==='primary'?'PRIMARY':'REPLICA'} / ${n.status}</span><strong>${esc(n.name)}</strong>${segmentChart(n)}</div>`).join('')}</div>
  ${readEvidence(r)}
  <div class="section-top"><h2>逐事件路由证据</h2><span>受控模型 / 不连接真实复制集群</span></div>
  ${table(['时间 / 事件','会话 / 节点','决策','位点需求 / 实际','副本四条件','决策理由'],r.trace.map(e=>[
    `${e.at_ms}ms<small>${kindLabel(e.kind)}</small>`,
    `${esc(e.session)}${e.node?`<small>${esc(e.node)}</small>`:''}`,
    e.decision==='rejected'?'<span class="rejected">拒绝请求</span>':`<span class="badge ${e.decision==='resumed'?'completed':e.kind==='diverge'?'failed':''}">${esc(decisionLabel(e.decision))}</span>`,
    `${e.required_position} / ${e.observed_position??'—'}`,
    e.kind==='read'?checkSummary(e):'—',
    esc(e.reason)
  ]))}
  <div class="evidence-notes"><section><h3>安全判定口径</h3><p>副本必须在线、同日志谱系、已核对内容覆盖会话最近写位点、延迟≤门槛，并核对读到的版本与主库同一位点一致。低延迟和相同位点都不能覆盖内容分叉。</p><p>${esc(r.mode)}</p></section><section><h3>确定性摘要</h3><p>命名日志谱系的单调整数位点 + 内容版本令牌；补传按位点幂等写入，未实现完整GTID集合或真实复制协议。</p><code>${esc(r.checksum)}</code></section></div>`
  :'<div class="notice">后台重放中，每1.5秒刷新。可保留此页，或稍后从运行记录返回。</div>'}`;
}

function R(summary){
  return `<span>${summary.misreads?'!':'✓'}</span> ${summary.requests-summary.rejected} / ${summary.requests}`;
}
