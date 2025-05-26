import sys
# 假设你的项目根目录在 PYTHONPATH 中，或者脚本在根目录运行
# 如果不在，可能需要像下面这样调整 sys.path
# sys.path.append('/path/to/your/ProPainter') # 指向 ProPainter 目录

from celery_app import app
from tasks import process_video_task # 确保 tasks.py 可以被找到
import time

print(f"Celery app instance from celery_app: {app}")
print(f"Broker URL from app.conf: {app.conf.broker_url}")
print(f"Result backend from app.conf: {app.conf.result_backend}")

if __name__ == '__main__':
    print("Attempting to send a task...")
    try:
        # 使用虚拟参数，确保任务能被接受但可能不会完整执行
        result = process_video_task.apply_async(
            args=['/tmp/fake_video.mp4', 'fake_kb_id'],
            kwargs={'original_video_name': 'fake_video.mp4'}
        )
        print(f"Task sent! Task ID: {result.id}")
        print(f"Task state (after sending): {result.state}")

        # 等待几秒钟看看任务状态是否变化 (可选)
        # time.sleep(5)
        # print(f"Task state (after 5s): {result.state}")
        # if result.failed():
        #     print(f"Task failed: {result.traceback}")
        # elif result.successful():
        #     print(f"Task succeeded, result: {result.get()}")

    except Exception as e:
        print(f"Error sending task: {e}")
        import traceback
        traceback.print_exc()