"""Scenario checks for the divergence-repair routing model."""
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.services.routing import simulate

NODES = [
    {'name': 'harbor-primary', 'role': 'primary', 'status': 'healthy', 'lineage': 'qiyuan-log-01', 'position': 105, 'delay_ms': 0, 'note': ''},
    {'name': 'north-replica', 'role': 'replica', 'status': 'healthy', 'lineage': 'qiyuan-log-01', 'position': 105, 'delay_ms': 150, 'note': ''},
    {'name': 'south-replica', 'role': 'replica', 'status': 'healthy', 'lineage': 'qiyuan-log-01', 'position': 105, 'delay_ms': 20, 'note': ''},
]


def run(events, mode='verified', max_delay=100, sticky=500, nodes=None):
    return simulate(nodes or NODES, events, max_delay, sticky, mode)


def trace_of(result, kind=None):
    return [e for e in result['trace'] if kind is None or e['kind'] == kind]


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def test_divergence_detected_and_prefix_preserved():
    events = [
        {'at_ms': 0, 'kind': 'write', 'session': 's'},
        {'at_ms': 100, 'kind': 'write', 'session': 's'},
        {'at_ms': 200, 'kind': 'diverge', 'node': 'south-replica', 'position': 104},
        {'at_ms': 300, 'kind': 'inspect', 'node': 'south-replica'},
    ]
    result = run(events)
    detected = trace_of(result, 'diverge')[0]
    check(detected['decision'] == 'detected', 'diverge should be detected')
    check(detected['divergence']['first_divergent_position'] == 104, 'first divergent position')
    check(detected['divergence']['matched_prefix_preserved'] == 103, 'matched prefix preserved')
    inspected = trace_of(result, 'inspect')[0]
    check(inspected['verification']['first_divergent_position'] == 104, 'inspect reports divergence')
    check(inspected['verification']['matched_prefix'] == 103, 'inspect prefix')


def test_out_of_order_batches_do_not_advance_verified_prefix():
    events = [
        {'at_ms': 0, 'kind': 'diverge', 'node': 'south-replica', 'position': 100},
        {'at_ms': 10, 'kind': 'backfill', 'node': 'south-replica', 'from_position': 103, 'to_position': 105},
        {'at_ms': 20, 'kind': 'backfill', 'node': 'south-replica', 'from_position': 100, 'to_position': 102},
    ]
    result = run(events)
    batches = trace_of(result, 'backfill')
    check(batches[0]['decision'] == 'buffered', 'late segment buffered')
    check(batches[0]['verified_before'] == 99 and batches[0]['verified_after'] == 99, 'verified prefix unchanged by early batch')
    check(batches[1]['verified_after'] == 105, 'gap filled advances verified prefix')
    repair = result['repairs'][0]
    check(repair['out_of_order'] == 1, 'out of order counted')


def test_duplicate_batch_is_not_appended_twice():
    events = [
        {'at_ms': 0, 'kind': 'diverge', 'node': 'south-replica', 'position': 100},
        {'at_ms': 10, 'kind': 'backfill', 'node': 'south-replica', 'from_position': 100, 'to_position': 105},
        {'at_ms': 20, 'kind': 'backfill', 'node': 'south-replica', 'from_position': 100, 'to_position': 105},
    ]
    result = run(events)
    batches = trace_of(result, 'backfill')
    check(batches[1]['decision'] == 'noop', 'duplicate batch is a no-op')
    check(batches[1]['batch']['duplicate'] is True, 'duplicate flagged')
    check(result['repairs'][0]['duplicates'] == 1, 'duplicate counted')
    node = next(n for n in result['final_nodes'] if n['name'] == 'south-replica')
    check(node['position'] == 105, 'position stays 105, no duplicate append')
    check(sum(stop - start + 1 for start, stop, _v in node['content_segments']) == 105, 'each position stored once')
    check(max(stop for _s, stop, _v in node['content_segments']) == 105, 'no duplicate tail at 106')


def test_reads_during_repair_follow_verified_progress():
    events = [
        {'at_ms': 0, 'kind': 'write', 'session': 'lab'},
        {'at_ms': 10, 'kind': 'write', 'session': 'lab'},
        {'at_ms': 20, 'kind': 'write', 'session': 'lab'},
        {'at_ms': 30, 'kind': 'diverge', 'node': 'south-replica', 'position': 104},
        {'at_ms': 1000, 'kind': 'read', 'session': 'lab'},      # required 108, verified 103 -> not eligible
        {'at_ms': 1010, 'kind': 'backfill', 'node': 'south-replica', 'from_position': 104, 'to_position': 106},
        {'at_ms': 1020, 'kind': 'read', 'session': 'lab'},      # required 108 still > 106 -> main/safe
        {'at_ms': 1030, 'kind': 'backfill', 'node': 'south-replica', 'from_position': 107, 'to_position': 108},
        {'at_ms': 1040, 'kind': 'read', 'session': 'lab'},      # required 108 <= 108 -> replica eligible
    ]
    result = run(events, sticky=0)
    reads = trace_of(result, 'read')
    check(reads[0]['decision'] == 'routed', 'first read routed to primary')
    check(reads[0]['node'] != 'south-replica', 'divergent replica skipped before verified progress')
    check(reads[1]['node'] != 'south-replica', 'still skipped at verified 106')
    check(reads[2]['node'] == 'south-replica', 'eligible once verified reaches 108')
    check(reads[2]['read_content']['matches'] is True, 'read content matches primary')


def test_position_mode_demonstrates_misread():
    events = [
        {'at_ms': 0, 'kind': 'write', 'session': 'lab'},
        {'at_ms': 10, 'kind': 'write', 'session': 'lab'},
        {'at_ms': 20, 'kind': 'write', 'session': 'lab'},
        {'at_ms': 25, 'kind': 'replicate', 'node': 'south-replica', 'position': 108, 'delay_ms': 20},
        {'at_ms': 30, 'kind': 'diverge', 'node': 'south-replica', 'position': 106},
        {'at_ms': 40, 'kind': 'read', 'session': 'lab'},
    ]
    result = run(events, mode='position', sticky=0, max_delay=1000)
    read = trace_of(result, 'read')[0]
    check(read['node'] == 'south-replica', 'position-only routing picks divergent replica')
    check(read['read_content']['matches'] is False, 'content mismatch is recorded')
    check(read['safe'] is False, 'unsafe read flagged')
    check(result['summary']['misreads'] == 1, 'misread counted')


def test_pause_mode_blocks_repairing_replica():
    events = [
        {'at_ms': 0, 'kind': 'write', 'session': 'lab'},
        {'at_ms': 30, 'kind': 'diverge', 'node': 'south-replica', 'position': 104},
        {'at_ms': 40, 'kind': 'read', 'session': 'lab'},
    ]
    result = run(events, mode='pause', sticky=0, max_delay=1000)
    read = trace_of(result, 'read')[0]
    check(read['node'] != 'south-replica', 'pause mode excludes repairing replica')
    repair_check = next(c for c in read['replica_checks'] if c['name'] == 'south-replica')
    check(repair_check['read_mode'] == 'pause' and repair_check['effective_position'] is None, 'pause mode check')


def test_resume_requires_verified_prefix_covering_primary():
    base = [
        {'at_ms': 0, 'kind': 'write', 'session': 'lab'},
        {'at_ms': 10, 'kind': 'diverge', 'node': 'south-replica', 'position': 104},
        {'at_ms': 20, 'kind': 'backfill', 'node': 'south-replica', 'from_position': 104, 'to_position': 106},
        {'at_ms': 25, 'kind': 'write', 'session': 'other'},
    ]
    blocked = run(base + [{'at_ms': 30, 'kind': 'repair_done', 'node': 'south-replica'}])
    row = trace_of(blocked, 'repair_done')[0]
    check(row['decision'] == 'blocked', 'resume blocked before catching up')
    check(row['resume']['allowed'] is False, 'blocked resume carries evidence')
    check(row['resume']['primary_position'] == 107, 'blocked resume records primary position')
    check(blocked['repairs'][0]['resumed'] is False, 'repair not marked resumed')
    check(blocked['summary']['all_routed_reads_safe'], 'blocked resume is not a failure of the run')
    good = run(base[:-1] + [{'at_ms': 30, 'kind': 'repair_done', 'node': 'south-replica'}], max_delay=1000)
    resumed = trace_of(good, 'repair_done')[0]
    check(resumed['decision'] == 'resumed', 'resume allowed after verified covers primary')


def test_catch_up_while_writes_continue_then_resume():
    events = [
        {'at_ms': 0, 'kind': 'write', 'session': 'lab'},
        {'at_ms': 10, 'kind': 'diverge', 'node': 'south-replica', 'position': 104},
        {'at_ms': 20, 'kind': 'backfill', 'node': 'south-replica', 'from_position': 104, 'to_position': 105},
        {'at_ms': 30, 'kind': 'write', 'session': 'lab'},
        {'at_ms': 40, 'kind': 'read', 'session': 'lab'},  # primary at 107, verified 105 -> read falls back
        {'at_ms': 50, 'kind': 'backfill', 'node': 'south-replica', 'from_position': 106, 'to_position': 107},
        {'at_ms': 60, 'kind': 'repair_done', 'node': 'south-replica'},
        {'at_ms': 70, 'kind': 'read', 'session': 'lab'},
    ]
    result = run(events, sticky=0, max_delay=1000)
    reads = trace_of(result, 'read')
    check(reads[0]['node'] == 'harbor-primary', 'falls back to primary while catching up')
    check(reads[1]['node'] == 'south-replica', 'resumes reading after repair_done')
    repair = result['repairs'][0]
    check(repair['resumed'] is True and repair['repaired'] is True, 'repair marked complete')


def test_same_position_different_content_is_caught():
    """A replica at the same position as the primary still fails a content check."""
    events = [
        {'at_ms': 0, 'kind': 'diverge', 'node': 'north-replica', 'position': 104},
        {'at_ms': 10, 'kind': 'read', 'session': 'visitor'},
    ]
    result = simulate(NODES, events, 100, 0, 'verified')
    read = trace_of(result, 'read')[0]
    check(read['node'] != 'north-replica', 'divergent same-position replica not selected')
    check(read['read_content']['matches'] is True, 'selected replica content matches primary')


def test_determinism():
    events = [
        {'at_ms': 0, 'kind': 'write', 'session': 's'},
        {'at_ms': 10, 'kind': 'diverge', 'node': 'south-replica', 'position': 104},
        {'at_ms': 20, 'kind': 'backfill', 'node': 'south-replica', 'from_position': 104, 'to_position': 106},
        {'at_ms': 30, 'kind': 'inspect', 'node': 'south-replica'},
        {'at_ms': 40, 'kind': 'repair_done', 'node': 'south-replica'},
    ]
    first = run(events)
    second = run(events)
    check(first['checksum'] == second['checksum'], 'same input, same checksum')


def test_validation_guards():
    cases = [
        ([{'at_ms': 0, 'kind': 'diverge', 'node': 'harbor-primary'}], 'diverge仅允许副本'),
        ([{'at_ms': 0, 'kind': 'diverge', 'node': 'south-replica', 'position': 106}], '分叉起始位点必须在副本已应用位点范围内'),
        ([{'at_ms': 0, 'kind': 'write'}, {'at_ms': 10, 'kind': 'diverge', 'node': 'south-replica'}, {'at_ms': 20, 'kind': 'replicate', 'node': 'south-replica', 'position': 106, 'delay_ms': 5}], '请使用backfill'),
        ([{'at_ms': 0, 'kind': 'backfill', 'node': 'south-replica', 'from_position': 100, 'to_position': 200}], '主库已提交位点'),
    ]
    for events, fragment in cases:
        try:
            run(events)
        except ValueError as error:
            check(fragment in str(error), f'expected {fragment!r}, got {error}')
        else:
            raise AssertionError(f'expected rejection: {fragment}')


def test_baseline_ledger_continues_across_trials():
    """A later read trial can start from the ledger the prior trial ended with."""
    first_nodes = [
        {'name': 'p', 'role': 'primary', 'status': 'healthy', 'lineage': 'L', 'position': 6, 'delay_ms': 0, 'note': ''},
        {'name': 'r', 'role': 'replica', 'status': 'healthy', 'lineage': 'L', 'position': 6, 'delay_ms': 5, 'note': ''},
        {'name': 'r2', 'role': 'replica', 'status': 'healthy', 'lineage': 'L', 'position': 6, 'delay_ms': 9, 'note': ''},
    ]
    first = run([
        {'at_ms': 0, 'kind': 'diverge', 'node': 'r', 'position': 4},
        {'at_ms': 10, 'kind': 'backfill', 'node': 'r', 'from_position': 5, 'to_position': 6},
        {'at_ms': 20, 'kind': 'read', 'session': 'v'},
    ], max_delay=100, nodes=first_nodes)
    ledger = json.loads(json.dumps(first['final_nodes']))
    second = run([
        {'at_ms': 0, 'kind': 'read', 'session': 'v'},
        {'at_ms': 10, 'kind': 'backfill', 'node': 'r', 'from_position': 4, 'to_position': 4},
        {'at_ms': 20, 'kind': 'repair_done', 'node': 'r'},
        {'at_ms': 30, 'kind': 'read', 'session': 'v'},
    ], max_delay=100, nodes=ledger)
    check(second['seeded'] and second['seeded'][0]['node'] == 'r', 'seeded ledger reported')
    first_read, last_read = trace_of(second, 'read')
    check(first_read['node'] == 'r' and first_read['repairing'] is True, 'seeded repair keeps serving verified prefix')
    check(last_read['node'] == 'r' and not last_read['repairing'], 'resume clears repairing on the same ledger')
    check(second['repairs'][0]['resumed'] is True, 'resume recorded across trials')
    third = run([{'at_ms': 0, 'kind': 'read', 'session': 'v'}], max_delay=100,
                nodes=json.loads(json.dumps(second['final_nodes'])))
    check(third['repairs'] == [], 'finished repair is not re-seeded as open')
    check(trace_of(third, 'read')[0]['node'] == 'r', 'repaired replica serves normally')


if __name__ == '__main__':
    tests = [value for name, value in sorted(globals().items()) if name.startswith('test_')]
    for test in tests:
        test()
        print('ok -', test.__name__)
    print(f'{len(tests)} checks passed')
