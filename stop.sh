#!/bin/bash

# 停止Streamlit应用
if [ -f logs/streamlit.pid ]; then
    echo "停止Streamlit应用..."
    kill -9 $(cat logs/streamlit.pid) 2>/dev/null || echo "Streamlit应用已经停止"
    rm logs/streamlit.pid
else
    echo "Streamlit应用未运行"
fi

# 停止Celery Worker
if [ -f logs/worker.pid ]; then
    echo "停止Celery Worker..."
    kill -9 $(cat logs/worker.pid) 2>/dev/null || echo "Celery Worker已经停止"
    rm logs/worker.pid
else
    echo "Celery Worker未运行"
fi

# 提示用户是否停止Redis
read -p "是否停止Redis服务？(y/n): " stop_redis
if [ "$stop_redis" = "y" ] || [ "$stop_redis" = "Y" ]; then
    echo "停止Redis服务..."
    redis-cli shutdown || echo "Redis服务已经停止或无法正常停止"
else
    echo "保持Redis服务运行..."
fi

echo "所有服务已停止！" 