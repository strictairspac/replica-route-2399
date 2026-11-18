import {table} from '../components/common/table.js';import {esc} from '../utils/html.js';import {session} from '../hooks/session.js';
const sample=[
  {at_ms:0,kind:'write',session:'lab-A'},
  {at_ms:100,kind:'write',session:'lab-A'},
  {at_ms:200,kind:'write',session:'lab-A'},
  {at_ms:300,kind:'diverge',node:'south-replica',position:106},
  {at_ms:400,kind:'read',session:'lab-A'},
  {at_ms:500,kind:'backfill',node:'south-replica',from_position:108,to_position:108},
  {at_ms:600,kind:'backfill',node:'south-replica',from_position:106,to_position:107},
  {at_ms:700,kind:'read',session:'lab-A'},
  {at_ms:800,kind:'inspect',node:'south-replica'},
  {at_ms:900,kind:'repair_done',node:'south-replica'},
  {at_ms:1000,kind:'read',session:'lab-A'}
];
export function requestSetsPage(items){return `<div class="split"><section><div class="section-top"><h2>会话请求轨迹</h2><span>${items.length} SETS / REPLAY INPUT</span></div>${table(['请求集 / 说明','事件数','操作'],items.map(r=>[`<strong>${esc(r.code)}</strong><small>${esc(r.title)}</small><small>${esc(r.note)}</small>`,`${r.events.length} 个`,`<button class="small edit-request" data-id="${r.id}" ${session.user.role==='viewer'?'disabled':''}>编辑</button>`]))}<div class="protocol"><h3>事件字段速查</h3><p>read / write：at_ms、kind、session。<br>node_down / node_up / inspect / repair_done：另需node。<br>replicate：另需node、position、delay_ms。<br>diverge：另需node，position可选分叉起始位点。<br>backfill：另需node、from_position、to_position。</p><p>事件按at_ms非递减排列，同一时刻按输入先后。最多100条。补传批次可乱序提交、可重复提交（按位点幂等，不重复追加）；恢复接读前，已核对前缀必须覆盖主库最新位点。</p></div></section><section class="form-panel"><span class="eyebrow">SESSION REQUEST SET</span><h2 id="request-heading">编排会话请求集</h2><form id="request-form"><input type="hidden" name="id"><label>请求集编号<input name="code" placeholder="shift-20261117" required></label><label>工作场景<input name="title" value="副本内容分叉修复演练" required></label><label>时序事件JSON<textarea name="events" rows="14" required>${esc(JSON.stringify(sample,null,2))}</textarea></label><div class="event-tools"><button type="button" class="small" id="insert-diverge" ${session.user.role==='viewer'?'disabled':''}>＋ 插入副本分叉</button><button type="button" class="small" id="insert-backfill" ${session.user.role==='viewer'?'disabled':''}>＋ 插入补传批次</button><button type="button" class="small" id="insert-read" ${session.user.role==='viewer'?'disabled':''}>＋ 插入一次读</button></div><label>备注<input name="note" value="09:15 分叉补齐与恢复接读"></label><button class="primary" ${session.user.role==='viewer'?'disabled':''}>保存会话请求集 +</button></form></section></div>`;}
