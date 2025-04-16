import os
import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Optional
from minio import Minio
import subprocess
from pathlib import Path
import aiohttp
from tqdm import tqdm
from urllib.parse import urlparse, urlunparse
from asyncio import gather
import shutil
import traceback
import uuid
from pathlib import Path
import schedule
import time
import glob

# 导入自定义组件
from processors.video_preprocessor import VideoPreprocessor
from processors.video_slicer import VideoSlicer
from processors.video_analyzer import VideoAnalyzer
from storage.minio_handler import MinIOHandler
from storage.knowledge_base import KnowledgeBaseHandler
from config.config import Config
from services.tagging_service import TaggingService



class VideoProcessor:
    def __init__(self, config):
        """
        初始化视频处理器
        params:
            config: 配置对象，包含所有必要的配置信息
        """
        self.config = config
        # 初始化各个组件
        self.preprocessor = VideoPreprocessor(config)
        self.slicer = VideoSlicer(config)
        self.minio_handler = MinIOHandler(config)
        self.kb_handler = KnowledgeBaseHandler(config)
        self.analyzer = VideoAnalyzer(config)
        self.tagging_service = None  # 将在initialize中初始化
        # 初始化日志
        self.logger = logging.getLogger(__name__)
        # 进度跟踪
        self.processing_status = {}

    def extract_file_path(self,slice_store_info):
    # 获取 URL
        url = slice_store_info['url']
    # 解析 URL
        parsed_url = urlparse(url)
        # 重新构建 URL，不包括查询参数
        base_url = urlunparse((parsed_url.scheme, parsed_url.netloc, parsed_url.path, '', '', ''))
        return base_url


    async def initialize(self):
        """
        初始化处理器，包括登录知识库等操作
        """
        try:
            # 确保 MinIO 连接
            self.minio_handler._ensure_bucket()
            # 登录知识库
            self.kb_handler = await self.kb_handler.__aenter__()
            # 初始化标签服务
            self.tagging_service = TaggingService(self.config, self.kb_handler)
            await self.tagging_service.initialize()
            # 确保输出目录存在
            os.makedirs(self.config.video_config['output_dir'], exist_ok=True)
            os.makedirs(self.config.video_config['frames_dir'], exist_ok=True)
            os.makedirs(self.config.video_config['mask_dir'], exist_ok=True)
        except Exception as e:
            self.logger.error(f"初始化失败: {str(e)}")
            raise

    async def process_single_video(self, video_path: str) -> Dict:
        """
        处理单个视频
        params:
            video_path: 视频文件路径
        returns:
            处理结果信息
        """
        video_name = os.path.basename(video_path)
        video_uuid = str(uuid.uuid4())
        self.processing_status[video_uuid] = {
            'status': 'processing',
            'start_time': datetime.now(),
            'progress': 0
        }

        try:
            try:
                file_extension = Path(video_path).suffix
                
                # 创建新的文件名
                new_video_name = f"{video_uuid}{file_extension}"
                new_video_path = os.path.join(os.path.dirname(video_path), new_video_name)
                
                # 检查文件是否存在
                if os.path.exists(new_video_path):
                    raise FileExistsError(f"目标文件已存在: {new_video_path}")
                    
                # 检查源文件是否存在
                if not os.path.exists(video_path):
                    raise FileNotFoundError(f"源文件不存在: {video_path}")
                    
                # 检查写入权限
                if not os.access(os.path.dirname(new_video_path), os.W_OK):
                    raise PermissionError(f"没有写入权限: {new_video_path}")
                
                # 重命名文件
                os.rename(video_path, new_video_path)
            except FileExistsError as e:
                self.logger.error(f"重命名失败 - 文件已存在: {str(e)}")
                # 生成一个新的UUID重试
                new_video_name = f"{str(uuid.uuid4())}{file_extension}"
                new_video_path = os.path.join(os.path.dirname(video_path), new_video_name)
                os.rename(video_path, new_video_path)
            except FileNotFoundError as e:
                self.logger.error(f"重命名失败 - 文件不存在: {str(e)}")
                raise
            except PermissionError as e:
                self.logger.error(f"重命名失败 - 权限不足: {str(e)}")
                raise
            except OSError as e:
                self.logger.error(f"重命名失败 - 系统错误: {str(e)}")
                raise
            # file_extension = Path(video_path).suffix
            
            # # 创建新的文件名
            # new_video_name = f"{video_uuid}{file_extension}"
            # new_video_path = os.path.join(os.path.dirname(video_path), new_video_name)
            
            # # 重命名文件
            # os.rename(video_path, new_video_path)
            # 1. 预处理视频
            """print(f"开始预处理视频: {video_name}")
            final_video = self.preprocessor.process_video(
                video_path=video_path,
                #output_path=os.path.join(self.config.video_config['output_dir'], f"{video_name}_processed.mp4")
            )
            self.processing_status[video_name]['progress'] = 25"""

            # 2. 创建数据集
            print(f"创建数据集: {video_uuid}")
            dataset_id = await self.kb_handler.create_dataset(video_uuid)
            self.processing_status[video_uuid]['progress'] = 35

            # 3. 切片处理

            print(f"开始视频切片: {video_uuid},素材路径：{new_video_path}")
            """file_name = "inpaint_out.mp4"
            final_video_name = os.path.join(video_path,file_name)"""
           
            slices = self.slicer.slice_video(
                video_path=new_video_path,
                #output_dir=os.path.join(self.config.video_config['frames_dir'], video_name),
                threshold=self.config.slice_config['slice_threshold']
            )
            self.processing_status[video_uuid]['progress'] = 50

            # 4. 处理每个切片
            total_slices = len(slices.slice_info)
            print("开始处理",{total_slices},"个切片")
            slice_paths = self.slicer.get_slice_paths()
            for idx, slice_path in enumerate(slice_paths, 1):
                # 上传到 MinIO
                print(f"处理切片 {idx}/{total_slices}: {slice_path}")
                base_video_name = os.path.splitext(video_uuid)[0]
                success,slice_store_info = self.minio_handler.upload_video_and_get_url(
                    video_path=slice_path,
                    object_path=f"{base_video_name}/slice_{idx}.mp4"
                )
                print(slice_store_info)
                file_path = self.extract_file_path(slice_store_info)
                print(file_path)

                

                # 分析切片
                pre_result = ""
                analysis_result = self.analyzer.analyze_video_slice(slice_path,title = video_name,prev_analysis_result=pre_result)
                print(analysis_result.analysis_info)

                pre_result = analysis_result.analysis_info['analysis_result']

                # 存储到知识库
                await self.kb_handler.create_data(collection_id=dataset_id,question = f"{analysis_result.analysis_info['analysis_result']}\n\n{file_path}",answer= os.path.abspath(slice_path))

                # 更新进度
                progress = 50 + (idx / total_slices * 50)
                self.processing_status[video_uuid]['progress'] = progress

            # 处理完成后，为集合添加标签
            # try:
            #     # 获取视频所在目录名
            #     video_dir = os.path.basename(os.path.dirname(video_path))
            #     # 从标签映射中获取对应的标签ID
            #     if video_dir in self.tagging_service.tag_mappings:
            #         tag_info = self.tagging_service.tag_mappings[video_dir]
            #         await self.kb_handler.add_tags_to_collections(
            #             collection_ids=[dataset_id],
            #             dataset_id=self.config.knowledge_base_config['datasetId'],
            #             tag_id=tag_info['tag_id']
            #         )
            #         self.logger.info(f"成功为集合 {dataset_id} 添加标签: {tag_info['tag_name']}")
            # except Exception as e:
            #     self.logger.error(f"添加标签失败: {str(e)}")

            # 处理完成
            self.processing_status[video_uuid].update({
                'status': 'completed',
                'end_time': datetime.now(),
                'progress': 100
            })
            finished_dir = self.config.slice_config['finish_dir']
            os.makedirs(finished_dir, exist_ok=True)
            shutil.move(new_video_path, os.path.join(finished_dir, os.path.basename(video_path)))


            return {
                'status': 'success',
                'video_name': video_name,
                'dataset_id': dataset_id,
                'total_slices': total_slices,
                'processing_time': (datetime.now() - self.processing_status[video_uuid]['start_time']).total_seconds()
            }

        except Exception as e:
            self.logger.error(f"处理视频失败 {video_name}: {str(e)}")
            self.processing_status[video_uuid].update({
                'status': 'failed',
                'error': str(e),
                'end_time': datetime.now()
            })
            raise
        finally:
            # 清理临时文件
            await self.cleanup_temp_files(video_uuid)

    async def process_all_videos(self, input_dir: str) -> List[Dict]:
        """
        处理目录下的所有视频
        params:
            input_dir: 输入目录路径
        returns:
            所有视频的处理结果
        """
        # 获取所有视频文件
        video_files = [f for f in os.listdir(input_dir) 
                      if f.endswith(('.mp4', '.avi', '.mov'))]
        results = []

        for video_file in video_files:
            video_path = os.path.join(input_dir, video_file)
            try:
                result = await self.process_single_video(video_path)
                results.append(result)
            except Exception as e:
                self.logger.error(f"处理视频失败 {video_file}: {str(e)}")
                results.append({
                    'status': 'failed',
                    'video_name': video_file,
                    'error': str(e)
                })

        return results
    
    async def process_all_videos_batch(self, input_dir: str, batch_size: int = 10, max_concurrent: int = 10, max_retries: int = 3) -> List[Dict]:
        try:
            # 首先检查并更新标签映射
            await self.tagging_service.update_folder_mappings(str(Path(input_dir).parent))
            self.logger.info("标签映射更新完成")

            video_files = [f for f in os.listdir(input_dir) 
                        if f.endswith(('.mp4', '.avi', '.mov'))]
            
            async def process_batch(batch: List[str]) -> List[Dict]:
                tasks = [
                    self.process_single_video(os.path.join(input_dir, video))
                    for video in batch
                ]
                results = await gather(*tasks, return_exceptions=True)
                return results
            
            results: List[Dict] = []
            for i in range(0, len(video_files), batch_size):
                batch = video_files[i:i + batch_size]
                for retry in range(max_retries):
                    batch_results = await process_batch(batch)
                    
                    for video, res in zip(batch, batch_results):
                        if isinstance(res, Exception):
                            self.logger.error(f"处理视频 {video} 失败: {str(res)}")
                            self.logger.error(traceback.format_exc())
                            if retry == max_retries - 1:
                                results.append({
                                    'status': 'failed',
                                    'video_name': video,
                                    'error': str(res)
                                })
                        else:
                            results.append(res)
                            break
                
                # 更新进度
                processed_count = sum(1 for res in results if not isinstance(res, Exception))
                total_count = len(video_files)
                progress = processed_count / total_count * 100
                self.processing_status['batch_progress'] = progress
            
            return results
        except Exception as e:
            self.logger.error(f"批处理视频失败: {str(e)}")
            raise
    
    def load_config_from_dir(self, dir_name: str) -> None:
        """
        根据目录名称加载相应的配置文件
        """
        # 根据目录名称加载相应的配置文件
        # 这里只是一个示例,你需要实现具体的加载逻辑
        if dir_name.startswith('理想L7'):
            self.analyse_config['analyze_point'] = f'{dir_name}功能推广'
            self.video_config['frames_dir'] = f'/path/to/frames/{dir_name}'
            self.video_config['mask_dir'] = f'/path/to/masks/{dir_name}'
    
    async def process_all_subdirs(self, input_dir: str, batch_size: int = 10, max_concurrent: int = 10, max_retries: int = 3) -> List[Dict]:
        """
        处理指定目录下的所有子目录
        """
        # 获取所有子目录
        subdirs = [d for d in os.listdir(input_dir) if os.path.isdir(os.path.join(input_dir, d)) and d.startswith('理想L7')]
        results: List[Dict] = []
        for subdir in subdirs:
            subdir_path = os.path.join(input_dir, subdir)
            
            # 加载子目录对应的配置文件
            self.load_config_from_dir(subdir)

            # 处理该子目录下的所有视频
            subdir_results = await self.process_all_videos_batch(subdir_path, batch_size, max_concurrent, max_retries)
            results.extend(subdir_results)

        return results

    async def cleanup_temp_files(self, video_name: str):
        """
        清理处理过程中产生的临时文件
        params:
            video_name: 视频名称
        """
        try:
            # 清理帧目录
            frames_path = os.path.join(self.config.video_config['frames_dir'], video_name)
            if os.path.exists(frames_path):
                for file in os.listdir(frames_path):
                    os.remove(os.path.join(frames_path, file))
                os.rmdir(frames_path)

            # 清理掩码目录
            mask_path = os.path.join(self.config.video_config['mask_dir'], video_name)
            if os.path.exists(mask_path):
                for file in os.listdir(mask_path):
                    os.remove(os.path.join(mask_path, file))
                os.rmdir(mask_path)

        except Exception as e:
            self.logger.warning(f"清理临时文件失败: {str(e)}")

    def get_processing_status(self, video_name: Optional[str] = None) -> Dict:
        """
        获取处理状态
        params:
            video_name: 可选，具体视频名称
        returns:
            处理状态信息
        """
        if video_name:
            return self.processing_status.get(video_name, {})
        return self.processing_status

    def update_config_for_directory(self, dir_path: str):
        """
        更新目录配置，使用第一层目录作为标签
        params:
            dir_path: 包含视频文件的目录路径
        """
        # 获取基础目录（配置中input_dir的父目录）
        base_dir = "/home/jinpeng/slice_for_video/video_input"
        
        # 计算相对路径
        try:
            rel_path = os.path.relpath(dir_path, base_dir)
        except ValueError:
            # 如果dir_path和base_dir在不同的驱动器上（Windows系统），使用绝对路径
            self.logger.warning(f"无法计算相对路径，使用目录基本名称: {dir_path}")
            dir_name = os.path.basename(dir_path)
            self.config.video_config['input_dir'] = dir_path
            self.config.analyse_config['analyze_point'] = f'{dir_name}'
            return
        
        # 分割路径为各个部分
        path_parts = Path(rel_path).parts
        
        # 获取第一层目录名称（如果存在）
        if path_parts:
            first_level_dir = path_parts[0]
        else:
            # 如果没有相对路径部分（即dir_path就是base_dir），使用目录基本名称
            first_level_dir = os.path.basename(dir_path)
        
        # 更新输入输出路径（保持指向实际包含视频的目录）
        self.config.video_config['input_dir'] = dir_path
        
        # 更新分析提示词，使用第一层目录名称
        self.config.analyse_config['analyze_point'] = f'{first_level_dir}'
        
        self.logger.info(f"已更新配置，目录路径: {dir_path}, 分析标签: {first_level_dir}")


    def detect_input_directories(self, max_depth=5) -> List[str]:
        """
        递归检测上级目录中的子目录，支持多层嵌套
        params:
            max_depth: 最大递归深度，防止无限递归
        returns:
            包含视频的子目录列表
        """
        base_dir = str(Path(self.config.video_config['input_dir']).parent)
        subdirs = []
        
        # 使用os.walk递归遍历目录树
        for root, dirs, files in os.walk(base_dir, followlinks=False):
            # 检查当前递归深度
            relative_path = os.path.relpath(root, base_dir)
            current_depth = len(relative_path.split(os.sep)) if relative_path != '.' else 0
            
            # 如果超过最大深度，跳过此目录的子目录
            if current_depth >= max_depth:
                dirs.clear()  # 清空dirs列表，防止os.walk继续递归
                continue
                
            # 检查当前目录是否包含视频文件
            video_files = [f for f in files if f.endswith(('.mp4', '.avi', '.mov'))]
            if video_files:
                subdirs.append(root)
        
        return subdirs


    async def process_all_directories(self):
        """
        处理所有检测到的目录
        """
        directories = self.detect_input_directories()
        for dir_path in directories:
            try:
                self.logger.info(f"开始处理目录: {dir_path}")
                self.update_config_for_directory(dir_path)
                await self.process_all_videos_batch(dir_path)
            except Exception as e:
                self.logger.error(f"处理目录 {dir_path} 时出错: {str(e)}")
                continue

async def main():
    # 初始化配置
    config = Config()
    
    # 创建处理器实例
    processor = VideoProcessor(config)
    print("初始化处理器...")
    # 初始化处理器
    await processor.initialize()
    
    # 记录已处理的目录和文件
    processed_files = set()
    
    print("开始持续监控视频文件...")
    while True:
        try:
            # 获取所有包含视频的目录
            directories = processor.detect_input_directories()
            
            for dir_path in directories:
                try:
                    # 获取目录中的所有视频文件
                    video_files = glob.glob(os.path.join(dir_path, "*.mp4")) + \
                                glob.glob(os.path.join(dir_path, "*.avi")) + \
                                glob.glob(os.path.join(dir_path, "*.mov"))
                    
                    # 找出未处理的视频文件
                    new_files = set(video_files) - processed_files
                    
                    if new_files:
                        print(f"发现新视频文件在目录: {dir_path}")
                        processor.update_config_for_directory(dir_path)
                        await processor.process_all_videos_batch(dir_path)
                        # 更新已处理文件集合
                        processed_files.update(new_files)
                
                except Exception as e:
                    print(f"处理目录 {dir_path} 时出错: {str(e)}")
                    continue
            
            # 休眠一段时间再继续检查
            await asyncio.sleep(30)  # 每30秒检查一次
            
        except Exception as e:
            print(f"监控过程发生错误: {str(e)}")
            await asyncio.sleep(60)  # 发生错误时等待较长时间再重试
            continue

if __name__ == "__main__":
    asyncio.run(main())