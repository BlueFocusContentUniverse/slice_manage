
# 创建日志目录
mkdir -p logs

# 启动Celery Worker
echo "启动Celery Worker..."
celery -A celery_app worker --loglevel=info --concurrency=7 > logs/celery_worker.log 2>&1 &
WORKER_PID=$!
echo "Celery Worker 已启动，PID: $WORKER_PID"

# 启动Streamlit应用
echo "启动Streamlit应用..."
streamlit run streamlit_ui.py > logs/streamlit.log 2>&1 &
STREAMLIT_PID=$!
echo "Streamlit应用已启动，PID: $STREAMLIT_PID"

# 保存PID到文件，方便停止服务
echo $WORKER_PID > logs/worker.pid
echo $STREAMLIT_PID > logs/streamlit.pid

echo "所有服务已启动完成！"
echo "要停止服务，请运行 ./stop.sh" 