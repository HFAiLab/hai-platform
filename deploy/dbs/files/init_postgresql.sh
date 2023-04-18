set -e

: ${PGPASSWORD:="root"}
: ${PGUSER:="root"}

while [[ $(PGPASSWORD=${PGPASSWORD} psql -U ${PGUSER} mars_db -c "select count(*) from pg_stat_activity" 2>&1 | grep row -c)x != "1"x ]] ;
do
  echo "数据库还没有启动"
  sleep 1
done

fused_sql=/tmp/fuse_sql.sql
echo "合并所有sql到一个文件中：${fused_sql}"

echo "BEGIN;" > ${fused_sql}
db_schema_files=$(find db_schemas/ -name '*.sql' | sort)
if [ -n "${NO_CI_DB_FILE}" ]; then
  db_ci_files=""
else
  db_ci_files=$(find ci/ci_db_data/ -name '*.sql' | sort)  # 插入 ci 数据
fi
for sql_file in ${db_schema_files} ${db_ci_files} ${INIT_SQL}; do
  { echo "-- ${sql_file}"
    cat "${sql_file}"
    echo "-- ${sql_file}"
    echo ""
  } >> /tmp/fuse_sql.sql
done

echo "COMMIT;" >> /tmp/fuse_sql.sql

echo "开始初始化，注意，若存在 task_ng 和 user 这两个表，将不会进一步初始化"
set +e
while
  [[ $(PGPASSWORD=${PGPASSWORD} psql -U ${PGUSER} mars_db -c "select id from mars_db.public.task_ng limit 1" 1>/dev/null 2>&1 || echo 1)x == "1"x ]] ||
  [[ $(PGPASSWORD=${PGPASSWORD} psql -U ${PGUSER} mars_db -c "select user_name from mars_db.public.user limit 1" 1>/dev/null 2>&1 || echo 1)x == "1"x ]] ;
do
  PGPASSWORD=${PGPASSWORD} psql -U ${PGUSER} mars_db -f /tmp/fuse_sql.sql > /dev/null
  if [[ "$?"x != '0'x ]] ; then
    echo "执行出问题了，ROLLBACK"
    PGPASSWORD=${PGPASSWORD} psql -U ${PGUSER} mars_db -c "ROLLBACK;"
  else
    echo "执行成功"
    break
  fi
  echo "还没有执行成功，等 5s 重新执行"
  sleep 5
done

echo "数据库初始化完成"
set -e
