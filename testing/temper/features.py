"""Each subject's feature, as temper/subjects declares it."""

FEATURES = {
    'path.kafka_kafka': 'connector.kafka',
    'txn.http': 'transactions',
    'view.aggregate': 'sql.aggregate',
    'view.asof': 'sql.asof',
    'view.grouping': 'sql.grouping',
    'view.hop': 'sql.hop',
    'view.join_full': 'sql.join',
    'view.join_inner': 'sql.join',
    'view.join_left': 'sql.join',
    'view.setop': 'sql.setop',
    'view.subquery': 'sql.subquery',
    'view.tumble': 'sql.tumble',
    'view.window': 'sql.window',
    'view.recursive': 'sql.recursion',
}
