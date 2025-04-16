import os
from storage.minio_handler import MinIOHandler
import yaml
from typing import Tuple
from config.config import Config
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
MAX_RETRIES = 3  # 最大重试次数

def build_object_path(video_path: str) -> str:
    """
    根据视频文件路径构建对象存储中的对象路径。

    Args:
        video_path: 视频文件路径

    Returns:
        对象存储中的对象路径
    """
    filename = os.path.basename(video_path)
    pattern = r'^(.+)_segment_(\d+)\.mp4$'
    match = re.match(pattern, filename)
    if match:
        video_id, segment_num = match.groups()
        object_path = f"{video_id}/slice_{segment_num}.mp4"
        return object_path
    else:
        print(f"Warning: Skipping file {video_path} due to invalid filename format")
        return None


def handle_upload_failure(video_path: str, object_path: str, result: str, error_file: str) -> None:
    """
    处理上传失败的情况，包括重试和记录错误文件名。

    Args:
        video_path: 视频文件路径
        result: 上传失败的结果
        error_file: 记录错误文件名的文件路径
    """
    retries = 0
    while retries < MAX_RETRIES:
        print(f"重试上传: {video_path} (重试次数: {retries + 1}/{MAX_RETRIES})")
        result = minio_handler.upload_file(video_path, object_path)
        if result:
            print(f"上传成功: {object_path}")
            return
        retries += 1

    # 达到最大重试次数后记录错误文件名
    with open(error_file, "a") as f:
        f.write(f"{video_path}\n")
    print(f"上传失败: {video_path} ({result})")

def upload_file(minio_handler: MinIOHandler,video_path: str, object_path: str) -> bool:
    """
    上传单个文件到 MinIO

    Args:
        video_path: 视频文件路径
        object_path: 对象存储中的对象路径

    Returns:
        上传是否成功
    """
    try:
        minio_handler.client.fput_object(
            bucket_name=minio_handler.config.minio_config['bucket'],
            object_name=f"{minio_handler.prefix}/{object_path}",
            file_path=video_path,
            content_type='video/mp4'
        )
        print(f"上传{video_path}成功")
        return True
    except Exception as e:
        print(f"上传失败: {video_path} ({e})")
        return False

def upload_videos(minio_handler: MinIOHandler, source_dir: str, error_file: str) -> None:
    """
    上传指定目录下的所有视频文件到 MinIO
    
    Args:
        minio_handler: MinIOHandler 实例
        source_dir: 源目录路径
        error_file: 记录错误文件名的文件路径
    """
    # 获取所有视频文件路径
    video_files = []
    for root, dirs, files in os.walk(source_dir):
        for file in files:
            if file.endswith('.mp4'):
                video_path = os.path.join(root, file)
                video_files.append(video_path)
    print("video_list", video_files)
    
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = []
        for video_path in video_files:
            object_path = build_object_path(video_path)
            if object_path:
                futures.append(executor.submit(upload_file,minio_handler,video_path, object_path))

        for future in as_completed(futures):
            result = future.result()
            if not result:
                video_path, object_path = future.result_args
                handle_upload_failure(video_path, object_path, "上传失败", error_file)

if __name__ == "__main__":
    # 实例化 MinIOHandler
    config = Config()

    minio_handler = MinIOHandler(config)
    
    # 上传视频文件
    source_dir = "/home/jinpeng/slice_for_video/video_output/slice"
    error_file = "upload_errors.txt"
    upload_videos(minio_handler, source_dir,error_file)