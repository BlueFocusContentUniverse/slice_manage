from celery import Celery
import os
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# 获取当前工作目录
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Redis配置
REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD', "Bfg@usr")
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6381))
REDIS_DB_BROKER = int(os.environ.get('REDIS_DB_BROKER', 4))
REDIS_DB_BACKEND = int(os.environ.get('REDIS_DB_BACKEND', 5))
REDIS_DB_LOCKS = int(os.environ.get('REDIS_DB_LOCKS', 6))

# 构建Redis URL并记录（密码遮蔽）
broker_url = f'redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB_BROKER}'
backend_url = f'redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB_BACKEND}'
logger.info(f"Redis Broker URL: redis://:**@{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB_BROKER}")
logger.info(f"Redis Backend URL: redis://:**@{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB_BACKEND}")
logger.info(f"Redis Locks DB: {REDIS_DB_LOCKS}")

# 测试Redis连接
try:
    import redis
    # 测试Broker连接
    r_broker = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB_BROKER, password=REDIS_PASSWORD)
    r_broker.ping()
    logger.info(f"Redis Broker连接测试成功")
    
    # 测试Backend连接
    r_backend = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB_BACKEND, password=REDIS_PASSWORD)
    r_backend.ping()
    logger.info(f"Redis Backend连接测试成功")
    
    # 测试Locks连接
    r_locks = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB_LOCKS, password=REDIS_PASSWORD)
    r_locks.ping()
    logger.info(f"Redis Locks连接测试成功")
except Exception as e:
    logger.error(f"Redis连接测试失败: {e}")

# 配置Celery
app = Celery(
    'video_processor',
    broker=broker_url,
    backend=backend_url,
    include=['tasks']  # 包含任务模块
)

# 配置
app.conf.update(
    worker_concurrency=20,  # 设置并发数为7
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Shanghai',
    enable_utc=False,
    task_track_started=True,  # 记录任务的开始时间
    task_acks_late=True,      # 任务完成后再确认
    worker_prefetch_multiplier=1,  # 控制消费速率，每个worker一次只处理一个任务
    task_ignore_result=False, # 保存任务结果，用于前端查询进度
    # 临时文件目录
    TEMP_DIR=os.path.join(BASE_DIR, 'temp'),
    broker_connection_retry=True,  # 自动重试连接Broker
    broker_connection_retry_on_startup=True,  # 启动时自动重试连接
    broker_connection_max_retries=10,  # 最大重试次数
    broker_pool_limit=10,  # 连接池大小
    
    # Redis安全相关配置
    broker_transport_options={
        'visibility_timeout': 3600,  # 1小时
        'socket_timeout': 30,        # 30秒超时
        'socket_connect_timeout': 30,
        'socket_keepalive': True,    # 保持连接
        'max_connections': 20,       # 最大连接数
        'retry_on_timeout': True,    # 超时时重试
        'retry_policy': {
            'max_retries': 5          # 最大重试次数
        },
    },
    redis_backend_use_ssl=False,     # 是否使用SSL (如果需要)
    redis_max_connections=20,         # Redis后端最大连接数
)

# 确保临时目录存在
os.makedirs(app.conf.TEMP_DIR, exist_ok=True)

# 在主进程启动时显式测试Redis连接
try:
    # 直接使用标准redis库测试连接
    import redis
    logger.info("验证Redis连接...")
    test_client = redis.Redis(
        host=REDIS_HOST, 
        port=REDIS_PORT, 
        password=REDIS_PASSWORD, 
        db=REDIS_DB_BROKER
    )
    test_key = "celery:connection:test"
    test_value = "1"
    test_client.set(test_key, test_value)
    result = test_client.get(test_key)
    test_client.delete(test_key)
    logger.info(f"Redis连接验证成功: 能够设置和获取测试键，值={result}")
    test_client.close()
except Exception as e:
    logger.error(f"Redis连接测试失败: {e}")

if __name__ == '__main__':
    app.start() 