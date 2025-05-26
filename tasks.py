import os
import uuid
import shutil
import logging
import time
import asyncio
from typing import Dict, List, Optional
from datetime import datetime
from pathlib import Path
import tempfile
import fcntl  # 用于文件锁
import errno  # 错误处理

from celery import Task, shared_task

# 导入应用配置和组件
from celery_app import app
from config.config import Config
from processors.video_preprocessor import VideoPreprocessor
from processors.video_slicer import VideoSlicer
from processors.video_analyzer import VideoAnalyzer
from storage.minio_handler import MinIOHandler
from storage.knowledge_base import KnowledgeBaseHandler

# 创建日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# 文件锁目录
LOCK_DIR = os.path.join(tempfile.gettempdir(), "propainter_locks")
os.makedirs(LOCK_DIR, exist_ok=True)

class VideoProcessorTask(Task):
    """视频处理任务基类，用于提供共享资源和配置"""
    
    _config = None
    _preprocessor = None
    _slicer = None
    _minio_handler = None
    
    @property
    def config(self):
        if self._config is None:
            self._config = Config()
        return self._config
    
    @property
    def preprocessor(self):
        if self._preprocessor is None:
            self._preprocessor = VideoPreprocessor(self.config)
        return self._preprocessor
    
    @property
    def slicer(self):
        if self._slicer is None:
            self._slicer = VideoSlicer(self.config)
        return self._slicer
    
    @property
    def minio_handler(self):
        if self._minio_handler is None:
            self._minio_handler = MinIOHandler(self.config)
            self._minio_handler._ensure_bucket()
        return self._minio_handler

    def get_knowledge_base_handler(self):
        """每次创建新的实例，因为不适合缓存"""
        kb_handler = KnowledgeBaseHandler(self.config)
        # 同步方式登录，设置auth_cookie
        kb_handler._login_sync()
        return kb_handler
    
    def extract_file_path(self, slice_store_info):
        """从存储信息中提取文件路径"""
        # 获取 URL
        from urllib.parse import urlparse, urlunparse
        url = slice_store_info['url']
        # 解析 URL
        parsed_url = urlparse(url)
        # 重新构建 URL，不包括查询参数
        base_url = urlunparse((parsed_url.scheme, parsed_url.netloc, parsed_url.path, '', '', ''))
        return base_url

    def acquire_file_lock(self, lock_name: str) -> tuple:
        """
        获取基于文件的锁
        
        Args:
            lock_name: 锁名称
            
        Returns:
            tuple: (成功获取锁?, 锁文件对象)
        """
        lock_file_path = os.path.join(LOCK_DIR, f"{lock_name}.lock")
        try:
            # 打开或创建锁文件
            f = open(lock_file_path, 'w')
            # 尝试获取独占锁 (非阻塞模式)
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            # 将进程ID写入锁文件
            f.write(str(os.getpid()))
            f.flush()
            logger.info(f"成功获取文件锁: {lock_name}")
            return True, f
        except IOError as e:
            # 获取锁失败
            if e.errno == errno.EACCES or e.errno == errno.EAGAIN:
                logger.warning(f"无法获取文件锁 {lock_name}，资源已被锁定")
                if 'f' in locals():
                    f.close()
                return False, None
            # 其他IO错误
            logger.error(f"获取文件锁失败 {lock_name}: {e}")
            if 'f' in locals():
                f.close()
            return True, None  # 在发生错误时假设获取锁成功，继续处理
        except Exception as e:
            # 其他异常
            logger.error(f"获取文件锁时发生异常 {lock_name}: {e}")
            if 'f' in locals():
                f.close()
            return True, None  # 在发生异常时假设获取锁成功，继续处理

    def release_file_lock(self, lock_file):
        """
        释放文件锁
        
        Args:
            lock_file: 锁文件对象
        """
        if lock_file is not None:
            try:
                # 释放锁并关闭文件
                fcntl.flock(lock_file, fcntl.LOCK_UN)
                lock_file.close()
                logger.info("释放了文件锁")
            except Exception as e:
                logger.error(f"释放文件锁失败: {e}")

@shared_task(bind=True, base=VideoProcessorTask)
def process_video_task(
    self, 
    video_file_path: str, 
    knowledge_base_id: str, 
    custom_dimensions: Optional[str] = None,
    original_video_name: str = None,
    processing_uuid: str = None,
    user_config: Optional[Dict] = None
) -> Dict:
    """
    处理单个视频的Celery任务
    
    Args:
        video_file_path: 上传视频的临时存储路径
        knowledge_base_id: 知识库ID
        custom_dimensions: 自定义的解析维度
        original_video_name: 原始视频文件名
        processing_uuid: 处理过程的唯一标识符
        user_config: 用户会话中的配置，优先于全局配置
        
    Returns:
        Dict: 包含处理结果的字典
    """
    try:
        video_name = original_video_name or os.path.basename(video_file_path)
        processing_uuid = processing_uuid or str(uuid.uuid4())
        
        logger.info(f"开始处理视频任务: ID={self.request.id}, 视频={video_name}")
        
        # 生成锁名称，使用视频名称生成唯一锁标识
        lock_name = f"video_process_{video_name.replace(' ', '_').replace('.', '_')}"
        lock_acquired, lock_file = self.acquire_file_lock(lock_name)
        
        if not lock_acquired:
            logger.warning(f"视频 {video_name} 已有其他任务在处理，跳过")
            return {
                'status': 'skipped',
                'message': '该视频正在被其他任务处理',
                'video_name': video_name,
                'processing_uuid': processing_uuid
            }
        
        try:
            # 更新处理状态
            self.update_state(
                state='PROGRESS',
                meta={
                    'current': 1,
                    'total': 100,
                    'step': '初始化处理环境'
                }
            )
            
            # 确保知识库ID已设置
            logger.info(f"设置知识库ID: {knowledge_base_id}")
            logger.info(f"设置分析点: {custom_dimensions}")
            
            # 应用用户配置（如果存在）
            if user_config:
                logger.info("使用用户会话中的配置")
                if 'knowledge_base' in user_config and 'datasetId' in user_config['knowledge_base']:
                    self.config.knowledge_base_config['datasetId'] = user_config['knowledge_base']['datasetId']
                if 'gemini_service' in user_config and 'prompt' in user_config['gemini_service']:
                    self.config.analyse_config['gemini_prompt'] = user_config['gemini_service']['prompt']
            else:
                # 兼容原有代码，使用参数配置
                logger.info("未提供用户配置，使用参数配置")
                self.config.knowledge_base_config['datasetId'] = knowledge_base_id
            
            # 分析维度始终使用参数
            self.config.analyse_config['analyze_point'] = custom_dimensions
            
            # 获取知识库处理器
            kb_handler = self.get_knowledge_base_handler()
            
            # 初始化分析器
            analyzer = VideoAnalyzer(self.config)
            
            # 使用UUID重命名视频文件
            temp_dir = app.conf.TEMP_DIR
            file_extension = os.path.splitext(video_name)[1]
            new_video_name = f"{processing_uuid}{file_extension}"
            new_video_path = os.path.join(temp_dir, new_video_name)
            
            # 确保临时目录存在
            os.makedirs(temp_dir, exist_ok=True)
            
            # 复制视频文件到临时目录
            shutil.copy2(video_file_path, new_video_path)
            
            # 更新处理状态
            self.update_state(
                state='PROGRESS',
                meta={
                    'current': 10,
                    'total': 100,
                    'step': '创建知识库数据集'
                }
            )
            
            # 创建数据集 - 使用同步方式调用异步方法
            logger.info(f"创建数据集: {processing_uuid}")
            dataset_id = asyncio.run(kb_handler.create_dataset(processing_uuid))
            
            # 更新处理状态
            self.update_state(
                state='PROGRESS',
                meta={
                    'current': 20,
                    'total': 100,
                    'step': '开始视频切片'
                }
            )
            
            # 切片处理
            logger.info(f"开始视频切片: {processing_uuid}, 素材路径: {new_video_path}")
            slices = self.slicer.slice_video(
                video_path=new_video_path,
                threshold=self.config.slice_config['slice_threshold']
            )
            
            # 更新处理状态
            self.update_state(
                state='PROGRESS',
                meta={
                    'current': 30,
                    'total': 100,
                    'step': '处理视频切片'
                }
            )
            
            # 处理每个切片
            slice_paths = self.slicer.get_slice_paths()
            total_slices = len(slice_paths)
            logger.info(f"开始处理 {total_slices} 个切片")
            
            prev_analysis_result = ""
            
            for idx, slice_path in enumerate(slice_paths, 1):
                # 更新处理状态
                progress = 30 + (idx / total_slices * 60)  # 切片处理从30%到90%
                self.update_state(
                    state='PROGRESS',
                    meta={
                        'current': idx,
                        'total': total_slices,
                        'step': f'处理第 {idx}/{total_slices} 个切片',
                        'progress': progress
                    }
                )
                
                # 上传到 MinIO
                logger.info(f"处理切片 {idx}/{total_slices}: {slice_path}")
                success, slice_store_info = self.minio_handler.upload_video_and_get_url(
                    video_path=slice_path,
                    object_path=f"{processing_uuid}/slice_{idx}.mp4"
                )
                
                if not success:
                    logger.error(f"上传切片 {idx} 失败")
                    continue
                
                # 提取文件路径
                file_path = self.extract_file_path(slice_store_info)
                logger.info(f"切片 {idx} 的URL: {file_path}")
                
                # 分析切片
                analysis_result = analyzer.analyze_video_slice(
                    slice_path,
                    title=video_name,
                    prev_analysis_result=prev_analysis_result,
                    custom_analysis_dimensions=custom_dimensions
                )
                
                if not analysis_result.success:
                    logger.error(f"分析切片 {idx} 失败: {analysis_result.message}")
                    continue
                
                # 更新前一个分析结果，用于下一次分析
                prev_analysis_result = analysis_result.analysis_info['analysis_result']
                
                # 存储到知识库 - 使用同步方式调用异步方法
                asyncio.run(kb_handler.create_data(
                    collection_id=dataset_id,
                    question=f"{analysis_result.analysis_info['analysis_result']}\n\n{file_path}",
                    answer=os.path.abspath(slice_path)
                ))
            
            # 更新处理状态
            self.update_state(
                state='PROGRESS',
                meta={
                    'current': 95,
                    'total': 100,
                    'step': '清理临时文件'
                }
            )
            
            # 清理临时文件
            if os.path.exists(new_video_path):
                os.remove(new_video_path)
            
            # 处理完成
            result = {
                'status': 'success',
                'video_name': video_name,
                'processing_uuid': processing_uuid,
                'dataset_id': dataset_id,
                'total_slices': total_slices,
                'processing_time': time.time()
            }
            
            logger.info(f"视频 {video_name} 处理完成")
            return result
            
        except Exception as e:
            logger.error(f"处理视频 {video_name} 失败: {str(e)}", exc_info=True)
            return {
                'status': 'failed',
                'error': str(e),
                'video_name': video_name,
                'processing_uuid': processing_uuid
            }
        finally:
            # 释放文件锁
            if lock_file:
                self.release_file_lock(lock_file)
                logger.info(f"释放视频 {video_name} 的处理锁")
    except Exception as e:
        # 捕获任何Redis或其他初始化阶段的错误，避免任务无法启动
        logger.error(f"任务初始化阶段失败: {str(e)}", exc_info=True)
        return {
            'status': 'failed',
            'error': f"任务初始化失败: {str(e)}",
            'video_name': original_video_name or os.path.basename(video_file_path) if video_file_path else "未知视频"
        } 