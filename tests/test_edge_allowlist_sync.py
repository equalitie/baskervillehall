import pytest

from baskervillehall.edge_allowlist_sync import (SyncError, check_edges, desired_ranges, parse_cidrs,
                                                 parse_edge_list)


def test_parse_edge_list():
    assert parse_edge_list({'last_updated': 'x', 'list': ['1.2.3.4', ' 5.6.7.8 ', '1.2.3.4']}) == \
        {'1.2.3.4/32', '5.6.7.8/32'}


def test_parse_edge_list_rejects_garbage():
    with pytest.raises(SyncError):
        parse_edge_list({'list': ['1.2.3.4', 'not-an-ip']})
    with pytest.raises(SyncError):
        parse_edge_list({'error': 'unauthorized'})
    with pytest.raises(SyncError):
        parse_edge_list(['1.2.3.4'])


def test_parse_cidrs():
    assert parse_cidrs(' 212.105.153.41/32, 10.0.0.7/24 ,') == {'212.105.153.41/32', '10.0.0.0/24'}
    assert parse_cidrs('') == set()


def edges(n, start=1):
    return {f'10.0.0.{i}/32' for i in range(start, start + n)}


def test_check_edges_first_run_and_normal_change():
    check_edges(edges(29), set(), 10, 0.7)                 # nothing allowed yet
    check_edges(edges(29, 8), edges(29), 10, 0.7)          # 7 of 29 replaced


def test_check_edges_refuses_short_or_very_different_list():
    with pytest.raises(SyncError):
        check_edges(edges(3), edges(29), 10, 0.7)
    with pytest.raises(SyncError):
        check_edges(edges(29, 100), edges(29), 10, 0.7)    # nothing in common


def test_desired_ranges_sorted_with_extra():
    assert desired_ranges({'10.0.0.2/32', '10.0.0.10/32'}, {'1.1.1.1/32'}) == \
        ['1.1.1.1/32', '10.0.0.2/32', '10.0.0.10/32']
