"""Deterministic routing replay with content-aware log segments.

Two replicas can report the same integer log position and still hold different
content.  Positions therefore never prove consistency on their own: every read
is checked against the primary's version at the session's required position.
A divergent replica is repaired with staged backfill batches that may arrive
out of order and may be retried without appending duplicate entries.
"""
import copy
import hashlib
import json

CANONICAL = 'main'
EVENT_KINDS = ('write', 'read', 'replicate', 'node_down', 'node_up', 'diverge', 'backfill', 'inspect', 'repair_done')
REPAIR_MODES = ('verified', 'pause', 'position')


class Log:
    """Applied log content as sorted, non-overlapping segments [start, end, version].

    Versions are compact deterministic content tokens.  The primary's token is
    always CANONICAL; a divergent replica carries a fork token.  Missing ranges
    are gaps, which lets a backfill batch that arrives early be staged without
    pretending the prefix is verified.
    """

    def __init__(self, end, version=CANONICAL):
        self.segments = [[1, end, version]] if end > 0 else []

    def version_at(self, position):
        for start, end, version in self.segments:
            if start <= position <= end:
                return version
        return None

    def applied_end(self):
        end = 0
        for start, stop, _version in sorted(self.segments):
            if start <= end + 1:
                end = max(end, stop)
            else:
                break
        return end

    def max_end(self):
        return max((stop for _start, stop, _version in self.segments), default=0)

    def set_range(self, start, stop, version):
        """Overwrite [start, stop]; idempotent because entries are keyed by position."""
        pieces = []
        for seg_start, seg_stop, seg_version in self.segments:
            if seg_stop < start or seg_start > stop:
                pieces.append([seg_start, seg_stop, seg_version])
            else:
                if seg_start < start:
                    pieces.append([seg_start, start - 1, seg_version])
                if seg_stop > stop:
                    pieces.append([stop + 1, seg_stop, seg_version])
        pieces.append([start, stop, version])
        pieces.sort()
        merged = []
        for seg_start, seg_stop, seg_version in pieces:
            if merged and merged[-1][2] == seg_version and merged[-1][1] + 1 == seg_start:
                merged[-1][1] = seg_stop
            else:
                merged.append([seg_start, seg_stop, seg_version])
        self.segments = merged

    def covered_by(self, start, stop, version=CANONICAL):
        covered = 0
        for seg_start, seg_stop, seg_version in self.segments:
            if seg_version == version:
                low, high = max(seg_start, start), min(seg_stop, stop)
                if low <= high:
                    covered += high - low + 1
        return covered == stop - start + 1


def _verified_end(log, primary_end):
    """Length of the contiguous, version-matching prefix of the primary log."""
    if primary_end <= 0:
        return 0
    verified = 0
    for start, stop, version in sorted(log.segments):
        if version != CANONICAL or start > verified + 1:
            break
        verified = stop
    return min(verified, primary_end)


def _first_divergence(log, end):
    """First position whose content differs from the primary, or None."""
    position = 1
    for start, stop, version in sorted(log.segments):
        if start > position:
            return position if position <= end else None
        if version != CANONICAL and stop >= position:
            return max(position, start)
        position = max(position, stop + 1)
        if position > end:
            return None
    return position if position <= end else None


def _fork_tag(node_name, first_position):
    return f'fork-{node_name}@{first_position}'


def _checked_position(session_required, observed):
    return session_required if session_required > 0 else observed


def simulate(node_inputs, events, max_delay_ms, sticky_ms, repair_mode='verified'):
    if not 1 <= len(node_inputs) <= 12:
        raise ValueError('需要1–12个节点')
    if not 1 <= len(events) <= 100:
        raise ValueError('需要1–100个时序事件')
    if max_delay_ms < 0 or sticky_ms < 0:
        raise ValueError('延迟门槛和粘滞时间不得为负')
    if repair_mode not in REPAIR_MODES:
        raise ValueError('修复期读策略必须是 verified / pause / position')
    if len({x['name'] for x in node_inputs}) != len(node_inputs):
        raise ValueError('节点名必须唯一')
    primaries = [x for x in node_inputs if x['role'] == 'primary']
    if len(primaries) != 1:
        raise ValueError('必须且只能配置一个主节点')

    nodes = {x['name']: copy.deepcopy(x) for x in node_inputs}
    primary = nodes[primaries[0]['name']]
    lineage = primary['lineage']
    primary['_log'] = Log(primary['position'], CANONICAL)
    seeded_repairs = []
    for node in nodes.values():
        if node is primary:
            if node.get('repair') or any(str(v) != CANONICAL for _s, _e, v in (node.get('content_segments') or [])):
                raise ValueError('主节点不能声明内容分叉或修复台账')
            node.pop('repair', None)
            node.pop('content_segments', None)
            continue
        segments = node.pop('content_segments', None)
        node['_log'] = Log(node['position'], CANONICAL)
        if segments:
            node['_log'].segments = [[int(start), int(stop), str(version)] for start, stop, version in segments]
            node['position'] = node['_log'].applied_end()
        repair = node.pop('repair', None)
        if repair:
            if node['role'] != 'replica':
                raise ValueError('修复台账仅允许副本节点')
            seeded_repairs.append((node, repair))
        if node['role'] == 'replica' and node['lineage'] == lineage and node['position'] > primary['position']:
            raise ValueError('同日志谱系副本位点不得高于主库初始位点')

    sessions = {}
    repairs = {}
    for node, repair in seeded_repairs:
        log = node['_log']
        first = int(repair.get('first_divergent_position') or 1)
        if not 1 <= first <= log.applied_end():
            raise ValueError(f'节点 {node["name"]} 的分叉起始位点不在已应用位点范围内')
        repairs[node['name']] = {
            'open': repair.get('open', True), 'discovered_at_ms': repair.get('discovered_at_ms', 0),
            'first_divergent_position': first,
            'matched_prefix_preserved': first - 1,
            'fork_tag': repair.get('fork_tag') or _fork_tag(node['name'], first),
            'primary_position_at_detection': repair.get('primary_position_at_detection', primary['position']),
            'batches': list(repair.get('batches', [])),
            'duplicates': int(repair.get('duplicates', 0)),
            'out_of_order': int(repair.get('out_of_order', 0)),
            'resumed_at_ms': repair.get('resumed_at_ms'),
        }
    trace = []
    last_time = -1

    for index, event in enumerate(events):
        at = event['at_ms']
        kind = event['kind']
        session = event.get('session', 'default')
        if at < last_time:
            raise ValueError('事件必须按时间非递减排列')
        if kind not in EVENT_KINDS:
            raise ValueError('未知事件类型')
        last_time = at
        required = sessions.get(session, {}).get('required', 0)
        row = {
            'index': index + 1, 'at_ms': at, 'kind': kind, 'session': session,
            'node': None, 'decision': 'applied', 'reason': '', 'required_position': required,
            'observed_position': None, 'safe': True, 'repair_mode': repair_mode,
        }

        if kind in ('node_down', 'node_up', 'replicate', 'diverge', 'backfill', 'inspect', 'repair_done'):
            target = nodes.get(event.get('node'))
            if not target:
                raise ValueError('事件引用未知节点：' + str(event.get('node')))
            row['node'] = target['name']
            log = target['_log']

            if kind == 'replicate':
                if target['role'] != 'replica':
                    raise ValueError('replicate仅允许副本节点')
                if target['status'] != 'healthy':
                    raise ValueError('停机副本不能应用日志，请先node_up')
                if target['lineage'] != lineage:
                    raise ValueError('日志谱系不匹配，不得复制当前主库日志')
                if repairs.get(target['name'], {}).get('open'):
                    raise ValueError('修复中的副本请使用backfill补传，不能直接replicate')
                position = event.get('position')
                delay = event.get('delay_ms')
                if position is None or delay is None or delay < 0 or not target['position'] <= position <= primary['position']:
                    raise ValueError('复制位点必须单调且不超过已提交主库位点')
                if position > log.applied_end():
                    log.set_range(log.applied_end() + 1, position, CANONICAL)
                target['position'] = log.applied_end()
                target['delay_ms'] = delay
                row['observed_position'] = position
                row['reason'] = '副本应用已提交日志'

            elif kind in ('node_down', 'node_up'):
                target['status'] = 'down' if kind == 'node_down' else 'healthy'
                row['reason'] = '节点下线' if kind == 'node_down' else '节点恢复'

            elif kind == 'diverge':
                if target['role'] != 'replica':
                    raise ValueError('diverge仅允许副本节点')
                if repairs.get(target['name'], {}).get('open'):
                    raise ValueError('该副本已有未修复的分叉')
                applied = log.applied_end()
                if applied <= 0:
                    raise ValueError('副本尚无已应用日志，无法安排分叉')
                first = event.get('position')
                if first is None:
                    first = applied
                if not 1 <= first <= applied:
                    raise ValueError('分叉起始位点必须在副本已应用位点范围内')
                if _first_divergence(log, applied) is not None:
                    raise ValueError('副本内容已存在分叉，请先使用backfill修复')
                tag = _fork_tag(target['name'], first)
                log.set_range(first, applied, tag)
                target['position'] = log.applied_end()
                repairs[target['name']] = {
                    'open': True, 'discovered_at_ms': at,
                    'first_divergent_position': first,
                    'matched_prefix_preserved': first - 1,
                    'fork_tag': tag, 'primary_position_at_detection': primary['position'],
                    'batches': [], 'duplicates': 0, 'out_of_order': 0,
                    'resumed_at_ms': None,
                }
                row.update(decision='detected', observed_position=first,
                           divergence={'first_divergent_position': first,
                                       'matched_prefix_preserved': first - 1,
                                       'fork_tag': tag,
                                       'applied_position': applied},
                           verified_position=first - 1,
                           reason=f'发现内容分叉：位点 {first} 起版本与主库不一致，保留已核对前缀 {first - 1}')

            elif kind == 'inspect':
                applied = log.applied_end()
                divergence = _first_divergence(log, applied)
                verified = _verified_end(log, primary['_log'].applied_end())
                pending_from = (applied + 1) if log.max_end() > applied else None
                row.update(decision='inspected', observed_position=verified,
                           verification={'first_divergent_position': divergence,
                                         'matched_prefix': divergence - 1 if divergence else applied,
                                         'verified_position': verified,
                                         'applied_position': applied,
                                         'pending_from': pending_from},
                           verified_position=verified,
                           reason=('内容与主库一致，已核对前缀 ' + str(verified)) if divergence is None
                           else f'位点 {divergence} 起内容分叉，已核对前缀 {divergence - 1}')

            elif kind == 'backfill':
                if target['role'] != 'replica':
                    raise ValueError('backfill仅允许副本节点')
                if target['status'] != 'healthy':
                    raise ValueError('停机副本不能补传，请先node_up')
                if target['lineage'] != lineage:
                    raise ValueError('日志谱系不匹配，不得补传当前主库日志')
                start = event.get('from_position')
                stop = event.get('to_position')
                if start is None or stop is None or not 1 <= start <= stop <= primary['position']:
                    raise ValueError('补传区间必须满足 1 ≤ 起始 ≤ 结束 ≤ 主库已提交位点')
                verified_before = _verified_end(log, primary['_log'].applied_end())
                applied_before = log.applied_end()
                duplicate = log.covered_by(start, stop, CANONICAL)
                if not duplicate:
                    log.set_range(start, stop, CANONICAL)
                target['position'] = log.applied_end()
                verified_after = _verified_end(log, primary['_log'].applied_end())
                applied_after = log.applied_end()
                out_of_order = start > verified_before + 1 and verified_after == verified_before
                info = repairs.get(target['name'])
                if info:
                    info['batches'].append({'at_ms': at, 'from': start, 'to': stop,
                                            'duplicate': duplicate, 'out_of_order': out_of_order,
                                            'verified_before': verified_before,
                                            'verified_after': verified_after})
                    info['duplicates'] += int(duplicate)
                    info['out_of_order'] += int(out_of_order)
                row.update(decision='buffered' if out_of_order else ('noop' if duplicate else 'applied'),
                           observed_position=start,
                           batch={'from': start, 'to': stop, 'duplicate': duplicate,
                                  'out_of_order': out_of_order},
                           verified_before=verified_before, verified_after=verified_after,
                           applied_before=applied_before, applied_after=applied_after,
                           verified_position=verified_after,
                           reason=('重复补传：区间已与主库一致，未追加日志条目' if duplicate
                                   else (f'批次 {start}-{stop} 先到并暂存，已核对前缀仍为 {verified_after}，等待缺口补齐'
                                         if out_of_order else
                                         f'批次 {start}-{stop} 已写入主库版本，已核对前缀 {verified_before} → {verified_after}')))

            elif kind == 'repair_done':
                if target['role'] != 'replica':
                    raise ValueError('repair_done仅允许副本节点')
                info = repairs.get(target['name'])
                verified = _verified_end(log, primary['_log'].applied_end())
                if not info or not info['open']:
                    if verified >= primary['position']:
                        row.update(decision='resumed', observed_position=verified,
                                   verified_position=verified,
                                   resume={'verified_position': verified,
                                           'primary_position': primary['position'],
                                           'allowed': True, 'idempotent': True},
                                   reason='副本已与主库一致且没有进行中的修复，恢复接读为空操作')
                    else:
                        raise ValueError('该副本没有进行中的修复，请先用diverge登记分叉再用backfill补传')
                elif target['status'] != 'healthy':
                    raise ValueError('待修副本仍停机，请先node_up再恢复接读')
                elif verified < primary['position']:
                    row.update(decision='blocked', observed_position=verified,
                               verified_position=verified,
                               resume={'verified_position': verified,
                                       'primary_position': primary['position'],
                                       'allowed': False},
                               reason=(f'拒绝恢复接读：已核对前缀 {verified} 未覆盖主库位点 {primary["position"]}，'
                                       f'待修副本继续按已核对进度处理'))
                else:
                    info['open'] = False
                    info['resumed_at_ms'] = at
                    row.update(decision='resumed', observed_position=verified,
                               verified_position=verified,
                               resume={'verified_position': verified,
                                       'primary_position': primary['position'],
                                       'allowed': True},
                               reason=f'修复完成：已核对前缀覆盖主库位点 {primary["position"]}，恢复接读')

        elif kind == 'write':
            if primary['status'] != 'healthy':
                row.update(decision='rejected', reason='主库不可用，拒绝写入')
            else:
                primary['_log'].set_range(primary['_log'].applied_end() + 1, primary['_log'].applied_end() + 1, CANONICAL)
                primary['position'] = primary['_log'].applied_end()
                sessions[session] = {'required': primary['position'], 'lineage': lineage, 'last_write_ms': at}
                row.update(node=primary['name'], decision='routed',
                           reason='写入主库并记录会话位点',
                           required_position=primary['position'],
                           observed_position=primary['position'],
                           read_content={'position': primary['position'], 'version': CANONICAL,
                                         'primary_version': CANONICAL, 'matches': True})

        elif kind == 'read':
            state = sessions.get(session, {'required': 0, 'lineage': lineage, 'last_write_ms': None})
            required = state['required']
            row['required_position'] = required
            sticky = state['last_write_ms'] is not None and at - state['last_write_ms'] < sticky_ms
            primary_end = primary['_log'].applied_end()
            checks = []
            candidates = []
            blocked = []
            for node in nodes.values():
                if node['role'] != 'replica':
                    continue
                log = node['_log']
                info = repairs.get(node['name'])
                repairing = bool(info and info['open'])
                verified = _verified_end(log, primary_end)
                applied = log.applied_end()
                if repairing and repair_mode == 'pause':
                    effective = None
                elif repairing and repair_mode == 'position':
                    effective = applied
                else:
                    effective = verified
                healthy = node['status'] == 'healthy'
                same_lineage = node['lineage'] == state['lineage']
                position_sufficient = effective is not None and effective >= required
                delay_within = node['delay_ms'] <= max_delay_ms
                check_position = _checked_position(required, effective if effective is not None else applied)
                content_matches = False
                if effective is not None:
                    content_matches = log.version_at(check_position) == primary['_log'].version_at(check_position)
                checks.append({'name': node['name'], 'healthy': healthy,
                               'same_lineage': same_lineage,
                               'position_sufficient': position_sufficient,
                               'delay_within_limit': delay_within,
                               'repairing': repairing,
                               'read_mode': repair_mode if repairing else 'normal',
                               'effective_position': effective,
                               'verified_position': verified,
                               'applied_position': applied,
                               'content_matches': content_matches})
                if healthy and same_lineage and position_sufficient and delay_within and effective is not None:
                    candidates.append((node, effective))
                elif repairing:
                    if not healthy:
                        why = '待修副本停机'
                    elif not delay_within:
                        why = '待修副本延迟超标'
                    elif repair_mode == 'pause':
                        why = '待修副本按停读策略不接读'
                    elif not position_sufficient:
                        why = f'待修副本已核对前缀 {verified} 未覆盖会话位点 {required}'
                    else:
                        why = '待修副本不满足条件'
                    blocked.append(f'{node["name"]}: {why}')
            candidates.sort(key=lambda item: (item[0]['delay_ms'], -item[1], item[0]['name']))
            selected = None
            effective = None
            if sticky:
                if primary['status'] == 'healthy' and primary['lineage'] == state['lineage'] and primary['position'] >= required:
                    selected, effective = primary, primary['position']
                    row['reason'] = '写后读粘滞：保持主库'
                else:
                    row['reason'] = '粘滞期主库不可用，拒绝不确定读取'
            elif candidates:
                selected, effective = candidates[0]
                selected_repair = repairs.get(selected['name'])
                if selected_repair and selected_repair['open']:
                    row['reason'] = '待修副本按已核对进度接读' if repair_mode == 'verified' else '仅按位点接读（未核对内容）'
                else:
                    row['reason'] = '副本位点满足会话且延迟达标'
            elif primary['status'] == 'healthy' and primary['lineage'] == state['lineage'] and primary['position'] >= required:
                selected, effective = primary, primary['position']
                row['reason'] = '无安全达标副本，回退主库'
            else:
                row['reason'] = '主库不可用且无安全达标副本，拒绝读取'
            if blocked and selected is not primary:
                row['reason'] += '；' + '；'.join(blocked)
            if selected:
                observed = effective
                check_position = _checked_position(required, observed)
                token = selected['_log'].version_at(check_position)
                primary_token = primary['_log'].version_at(check_position)
                content_match = check_position == 0 or token == primary_token
                row.update(node=selected['name'], decision='routed', observed_position=observed,
                           read_content={'position': check_position, 'version': token,
                                         'primary_version': primary_token, 'matches': content_match},
                           content_matches=content_match,
                           repairing=bool(repairs.get(selected['name'], {}).get('open')),
                           safe=observed >= required and selected['lineage'] == state['lineage'] and content_match)
                if not content_match:
                    row['reason'] += f'；读到的版本 {token} 与主库 {primary_token} 不一致'
            else:
                row['decision'] = 'rejected'
                row['safe'] = False
                row['read_content'] = None
            row['replica_checks'] = checks

        trace.append(row)

    requests = [r for r in trace if r['kind'] in ('read', 'write')]
    routed = [r for r in requests if r['decision'] == 'routed']
    rejected = [r for r in requests if r['decision'] == 'rejected']
    routed_reads = [r for r in routed if r['kind'] == 'read']
    safe = all(r['safe'] for r in routed)
    misreads = [r for r in routed_reads if not r['safe']]
    primary_end = primary['_log'].applied_end()

    final_nodes = []
    log_states = {}
    for name, node in nodes.items():
        log = node.pop('_log')
        applied = log.applied_end()
        verified = _verified_end(log, primary_end)
        divergence = _first_divergence(log, applied)
        log_states[name] = log
        node['position'] = applied
        node['verified_position'] = verified
        node['content_segments'] = log.segments
        node['divergence'] = None if divergence is None else {
            'first_divergent_position': divergence,
            'matched_prefix_preserved': divergence - 1,
        }
        info = repairs.get(name)
        node['repair'] = None if not info else {
            'first_divergent_position': info['first_divergent_position'],
            'fork_tag': info['fork_tag'],
            'open': info['open'],
            'discovered_at_ms': info['discovered_at_ms'],
            'primary_position_at_detection': info['primary_position_at_detection'],
            'verified_position': verified,
            'batches': info['batches'],
            'duplicates': info['duplicates'],
            'out_of_order': info['out_of_order'],
            'resumed_at_ms': info['resumed_at_ms'],
        }
        if info and not info['open'] and verified >= primary_end:
            node['repair'] = None
        final_nodes.append(node)

    def pending_from_of(log, verified):
        return (verified + 1) if log.max_end() > verified else None

    repair_rows = []
    for name, info in repairs.items():
        node = next(x for x in final_nodes if x['name'] == name)
        verified = node['verified_position']
        repair_rows.append({
            'node': name,
            'discovered_at_ms': info['discovered_at_ms'],
            'first_divergent_position': info['first_divergent_position'],
            'matched_prefix_preserved': info['matched_prefix_preserved'],
            'fork_tag': info['fork_tag'],
            'primary_position_at_detection': info['primary_position_at_detection'],
            'primary_position_final': primary_end,
            'applied_position_final': node['position'],
            'verified_position_final': verified,
            'pending_from': pending_from_of(log_states[name], verified),
            'batches': info['batches'],
            'duplicates': info['duplicates'],
            'out_of_order': info['out_of_order'],
            'repaired': verified >= primary_end,
            'resumed': info['resumed_at_ms'] is not None,
            'open': info['open'],
            'resumed_at_ms': info['resumed_at_ms'],
        })

    result = {
        'mode': '受控复制路由模型 / 非真实复制集群',
        'lineage': lineage,
        'trace': trace,
        'sessions': sessions,
        'final_nodes': final_nodes,
        'repairs': repair_rows,
        'seeded': [{'node': node['name'],
                    'first_divergent_position': repairs[node['name']]['first_divergent_position'],
                    'from_baseline': True} for node, repair in seeded_repairs],
        'summary': {
            'requests': len(requests), 'routed': len(routed), 'rejected': len(rejected),
            'replica_reads': sum(1 for r in routed_reads if r['node'] != primary['name']),
            'primary_reads': sum(1 for r in routed_reads if r['node'] == primary['name']),
            'all_routed_reads_safe': safe,
            'misreads': len(misreads),
            'read_content_mismatches': len(misreads),
            'repairing_replica_reads': sum(1 for r in routed_reads if r.get('repairing')),
        },
        'policy': {
            'max_delay_ms': max_delay_ms, 'sticky_ms': sticky_ms,
            'repair_read_mode': repair_mode,
            'repair_read_mode_label': {'verified': '按已核对进度接读', 'pause': '修复期间停读', 'position': '仅按位点（危险演示）'}[repair_mode],
            'position_model': '同一命名日志谱系的单调整数位点 + 内容版本令牌；位点相同不代表内容一致',
            'sticky_boundary': 'at_ms - last_write_ms < sticky_ms',
            'replica_tie_break': 'delay升序 / 已核对位点降序 / name字典序',
            'repair_gate': '副本读取需已核对前缀覆盖会话位点；恢复接读需已核对前缀覆盖主库最新位点',
        },
    }
    result['checksum'] = hashlib.sha256(json.dumps(result, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    return result
