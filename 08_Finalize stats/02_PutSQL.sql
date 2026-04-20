INSERT INTO DI_PARAMETER (DI_ID, PARAMETER_NAME, PARAMETER_VALUE)
VALUES
  (${prod_db_id}, 'fetch.start',                 '${fetch.start}'),
  (${prod_db_id}, 'fetch.end',                   '${fetch.end}'),
  (${prod_db_id}, 'fetch.duration',              '${fetch.duration}'),

  (${prod_db_id}, 'checksum.start',              '${checksum.start}'),
  (${prod_db_id}, 'checksum.end',                '${checksum.end}'),
  (${prod_db_id}, 'checksum.duration',           '${checksum.durationMs}'),

  (${prod_db_id}, 'rawcooked.start',             '${rawcooked.start}'),
  (${prod_db_id}, 'rawcooked.end',               '${rawcooked.end}'),
  (${prod_db_id}, 'rawcooked.duration',          '${rawcooked.durationMs}'),
  (${prod_db_id}, 'rawcooked.input.bytes',       '${rawcooked.total.input.bytes}'),
  (${prod_db_id}, 'rawcooked.output.bytes',      '${rawcooked.total.output.bytes}'),
  (${prod_db_id}, 'rawcooked.compression.ratio', '${rawcooked.total.compression_ratio}'),

  (${prod_db_id}, 'eark.start',                  '${eark.start}'),
  (${prod_db_id}, 'eark.end',                    '${eark.end}'),
  (${prod_db_id}, 'eark.duration',               '${eark.duration}'),

  (${prod_db_id}, 'pipeline.start',              '${total.pipeline.start}'),
  (${prod_db_id}, 'pipeline.end',                '${total.pipeline.end}'),
  (${prod_db_id}, 'pipeline.duration',           '${total.pipeline.duration}'),

  (${prod_db_id}, 'package.size.start',          '${package.size.start}'),
  (${prod_db_id}, 'package.size.end',            '${package.size.end}')
ON DUPLICATE KEY UPDATE
  PARAMETER_VALUE = VALUES(PARAMETER_VALUE);
