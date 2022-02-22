sed -i "s/#shared_preload_libraries = ''/shared_preload_libraries = 'pg_stat_statements'\npg_stat_statements.max=10000\npg_stat_statements.track=all/g" /var/lib/postgresql/data/postgresql.conf

echo "Enabled pg_stat_statements"