#!/bin/bash
# 这个脚本会在瞬间把 5 个请求同时挂到后台执行，用来测试并发冲突

URL="http://localhost:8080/api/v1/gift/send"
USER_ID="1002"

echo "开始发射狂暴并发流量..."

for i in {3..8}
do
  # 优化点：外层用单引号包裹整个 JSON，变量 $i 用 '"$i"' 的标准 Bash 方式安全拼接
  # 去掉了末尾可能导致换行断开的反斜杠，直接单行写完
  curl -s -X POST "$URL" \
    -H "X-User-ID: $USER_ID" \
    -H "Content-Type: application/json" \
    -d '{"room_id":3,"anchor_id":888,"gift_id":1,"combo_seq":'"$i"'}' &
    
done

wait # 等待所有后台的 5 个 curl 进程全部执行完毕
echo -e "\n5个并发请求已在同一瞬间全部发射完毕！"