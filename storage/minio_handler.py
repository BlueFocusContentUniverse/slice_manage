# handlers/minio_handler.py
from minio import Minio
from minio.error import S3Error
import os
import logging
from typing import Optional, Tuple, BinaryIO
from datetime import timedelta

class MinIOHandler:
    """处理与 MinIO 的交互"""
    
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.prefix = self.config.minio_config.get('prefix', '').strip('/')

        
        # 初始化 MinIO 客户端
        self.client = Minio(
            endpoint=config.minio_config['endpoint'],
            access_key=config.minio_config['access_key'],
            secret_key=config.minio_config['secret_key'],
            secure= True
        )
        
        # 确保 bucket 存在
        self._ensure_bucket()

    def _ensure_bucket(self) -> None:
        """确保存储桶存在"""
        try:
            if not self.client.bucket_exists(self.config.minio_config['bucket']):
                self.client.make_bucket(self.config.minio_config['bucket'])
                print(f"创建存储桶: {self.config.minio_config['bucket']}")
        except S3Error as e:
            self.logger.error(f"存储桶操作失败: {str(e)}")
            raise

    def _get_full_path(self, object_path: str) -> str:
        """
        获取完整的对象路径
        
        Args:
            object_path: 相对路径
            
        Returns:
            完整的对象路径
        """
        # 清理路径中的多余斜杠和开头的斜杠
        clean_path = object_path.strip('/')
        if self.prefix:
            return f"{self.prefix}/{clean_path}"
        return clean_path
    
    def upload_file(self, file_path: str, object_path: Optional[str] = None) -> Tuple[bool, str]:
        """
        上传文件到 MinIO
        
        Args:
            file_path: 本地文件路径
            object_path: MinIO 中的对象路径（可选，相对于 prefix）
            
        Returns:
            (success, message)
        """
        try:
            if not os.path.exists(file_path):
                return False, "文件不存在"
                
            # 如果没有指定对象路径，使用文件名
            if object_path is None:
                object_path = os.path.basename(file_path)
                
            # 获取完整路径
            full_path = self._get_full_path(object_path)
            
            # 上传文件
            result = self.client.fput_object(
                bucket_name=self.config.minio_config['bucket'],
                object_name=full_path,
                file_path=file_path,
                content_type='video/mp4'
            )
            
            return result
            
        except S3Error as e:
            error_msg = f"上传失败: {str(e)}"
            self.logger.error(error_msg)
            return None

    def download_file(self, object_path: str, file_path: str) -> Tuple[bool, str]:
        """
        从 MinIO 下载文件
        
        Args:
            object_name: MinIO 中的对象名称
            file_path: 本地保存路径
            
        Returns:
            (success, message)
        """
        try:
            full_path = self._get_full_path(object_path)

            self.client.fget_object(
                bucket_name=self.config.minio_config['bucket'],
                object_name=full_path,
                file_path=file_path
            )
            return True, file_path
            
        except S3Error as e:
            error_msg = f"下载失败: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg

    def get_public_url(self, object_path: str) -> Tuple[bool, str]:
        """
        获取对象的公网访问 URL
        """
        try:
            full_path = self._get_full_path(object_path)
            # 直接拼接公网访问地址
            url = f"https://{self.config.minio_config['endpoint']}/{self.config.minio_config['bucket']}/{full_path}"
            return True, url
        except Exception as e:
            error_msg = f"获取公网访问 URL 失败: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg


    def delete_file(self, object_path: str) -> Tuple[bool, str]:
        """
        删除 MinIO 中的文件
        
        Args:
            object_path: 要删除的对象路径（相对于 prefix）
            
        Returns:
            (success, message)
        """
        try:
            full_path = self._get_full_path(object_path)
            
            self.client.remove_object(
                bucket_name=self.config.minio_config['bucket'],
                object_name=full_path
            )
            return True, f"成功删除 {full_path}"
            
        except S3Error as e:
            error_msg = f"删除失败: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg

    def list_files(self, prefix: str = "") -> Tuple[bool, list]:
        """
        列出 MinIO 中的文件
        
        Args:
            prefix: 额外的路径前缀（可选，相对于基础 prefix）
            
        Returns:
            (success, file_list/error_message)
        """
        try:
            full_prefix = self._get_full_path(prefix)
            
            objects = self.client.list_objects(
                bucket_name=self.config.minio_config['bucket'],
                prefix=full_prefix
            )
            
            # 移除基础前缀，返回相对路径
            file_list = [obj.object_name for obj in objects]
            return True, file_list
            
        except S3Error as e:
            error_msg = f"列举文件失败: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg
    
    def upload_video_and_get_url(
        self, 
        video_path: str, 
        object_path: Optional[str] = None, 
        expires: timedelta = timedelta(hours=1)
    ) -> Tuple[bool, dict]:
        """
        上传视频并获取临时访问链接
        
        Args:
            video_path: 本地视频文件路径
            object_path: MinIO 中的存储路径（可选）
            expires: 链接有效期（默认 1小时）
            
        Returns:
            Tuple[bool, dict]: (成功状态, 结果字典)
            结果字典包含：
            - success 时: {"url": "访问链接", "object_path": "存储路径"}
            - failure 时: {"error": "错误信息"}
        """
        try:
            # 1. 上传视频
            upload_result = self.upload_file(video_path, object_path)
            if not upload_result:
                return False, {"error": "视频上传失败"}
                
            # 获取实际的存储路径
            actual_object_path = object_path if object_path else os.path.basename(video_path)
            
            # 2. 获取访问链接
            success, url_result = self.get_public_url(actual_object_path)
            if not success:
                return False, {"error": f"获取访问链接失败: {url_result}"}
                
            return True, {
                "url": url_result,
                "object_path": actual_object_path
            }
            
        except Exception as e:
            error_msg = f"处理失败: {str(e)}"
            self.logger.error(error_msg)
            return False, {"error": error_msg}