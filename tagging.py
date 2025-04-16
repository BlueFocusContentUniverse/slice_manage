import os
import logging
from typing import Dict, List, Optional
import aiohttp
import asyncio
from pathlib import Path
import json
from storage.knowledge_base import KnowledgeBaseHandler
from config.config import Config
from processors.video_analyzer import VideoAnalyzer
class TaggingService:
    def __init__(self, config, kb_handler: KnowledgeBaseHandler):
        self.config = config
        self.kb_handler = kb_handler
        self.logger = logging.getLogger(__name__)
        self.dataset_id = self.config.knowledge_base_config['datasetId']
        self.base_url = self.config.knowledge_base_config['base_url']
        self.session = None
        self.mapping_file = "tag_mappings.json"
        self.tag_mappings = self.load_mappings()

    def load_mappings(self) -> Dict[str, Dict[str, str]]:
        """从文件加载标签映射"""
        try:
            if os.path.exists(self.mapping_file):
                with open(self.mapping_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            self.logger.error(f"加载标签映射文件失败: {str(e)}")
            return {}

    def save_mappings(self):
        """保存标签映射到文件"""
        try:
            with open(self.mapping_file, 'w', encoding='utf-8') as f:
                json.dump(self.tag_mappings, f, ensure_ascii=False, indent=2)
            self.logger.info("标签映射已保存到文件")
        except Exception as e:
            self.logger.error(f"保存标签映射文件失败: {str(e)}")

    async def initialize(self):
        """初始化服务，创建HTTP会话"""
        if not self.session:
            self.session = aiohttp.ClientSession()
            # 使用与KnowledgeBaseHandler相同的认证
            await self.kb_handler._login()
            self.session.headers.update(self.kb_handler.session.headers)

    async def close(self):
        """关闭服务"""
        if self.session:
            await self.session.close()

    async def create_tag(self, tag_name: str) -> Optional[str]:
        """创建新的标签
        
        Args:
            tag_name: 标签名称
            
        Returns:
            tag_id: 标签ID，如果创建失败返回None
        """
        try:
            payload = {
                "datasetId": self.dataset_id,
                "tag": tag_name
            }
            
            async with self.session.post(
                f"{self.base_url}/api/proApi/core/dataset/tag/create",
                json=payload
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    tag_id = data.get('data')
                    if tag_id:
                        self.logger.info(f"成功创建标签: {tag_name}, ID: {tag_id}")
                        return tag_id
                    else:
                        self.logger.error(f"创建标签响应中未找到tag_id: {data}")
                        return None
                else:
                    self.logger.error(f"创建标签失败: HTTP {response.status}")
                    return None
                    
        except Exception as e:
            self.logger.error(f"创建标签时发生错误: {str(e)}")
            return None

    async def process_folder_name(self, folder_name: str) -> str:
        """使用LLM处理文件夹名称，获取标准化的标签名称"""
        prompt = f"这是一个汽车品牌的文件夹名称：{folder_name}，请分析并直接输出对应的汽车品牌和型号名称，不要输出任何多余的信息，例如输入'理想L7白色'，输出'理想L7'，如输入‘竞品车型————理想L7’，输出'理想L7'："
        try:
            result = await self.kb_handler._call_llm(prompt)
            return result.strip()
        except Exception as e:
            self.logger.error(f"LLM处理文件夹名称时发生错误: {str(e)}")
            raise

    async def update_folder_mappings(self, base_path: str):
        """更新文件夹映射
        
        Args:
            base_path: 基础路径，包含所有子文件夹
        """
        try:
            # 获取所有子文件夹
            subdirs = [d for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d))]
            
            for subdir in subdirs:
                if subdir not in self.tag_mappings:
                    # 使用LLM处理文件夹名称
                    tag_name = await self.process_folder_name(subdir)
                    
                    # 检查是否已存在相同的标签名称
                    existing_tag = next(
                        (mapping for mapping in self.tag_mappings.values() if mapping["tag_name"] == tag_name),
                        None
                    )
                    
                    if existing_tag:
                        # 使用现有的标签ID
                        self.tag_mappings[subdir] = {
                            "tag_name": tag_name,
                            "tag_id": existing_tag["tag_id"]
                        }
                    else:
                        # 创建新标签
                        tag_id = await self.create_tag(tag_name)
                        if tag_id:
                            self.tag_mappings[subdir] = {
                                "tag_name": tag_name,
                                "tag_id": tag_id
                            }
                        else:
                            self.logger.error(f"无法为文件夹 {subdir} 创建标签")
                            continue
                    
                    # 每次更新映射后保存到文件
                    self.save_mappings()
                    self.logger.info(f"更新映射 - 文件夹: {subdir}, 标签: {tag_name}")
                    
        except Exception as e:
            self.logger.error(f"更新文件夹映射时发生错误: {str(e)}")
            raise

    async def process_collections(self):
        """处理所有集合并添加标签"""
        try:
            # 获取所有集合
            collections = await self.kb_handler.get_all_collections(self.dataset_id)
            
            # 按批次处理集合
            batch_size = 10
            for i in range(0, len(collections), batch_size):
                batch = collections[i:i + batch_size]
                
                # 处理每个批次的集合
                for collection in batch:
                    collection_id = collection['_id']
                    
                    # 获取该集合的问题分析结果
                    tag_name = await self.kb_handler.process_filtered_collections_questions([collection])
                    
                    # 查找对应的标签ID
                    tag_mapping = next(
                        (mapping for mapping in self.tag_mappings.values() if mapping["tag_name"] == tag_name),
                        None
                    )
                    
                    if tag_mapping:
                        # 添加标签到集合
                        await self.kb_handler.add_tags_to_collections(
                            collection_ids=[collection_id],
                            dataset_id=self.dataset_id,
                            tag_id=tag_mapping["tag_id"]
                        )
                        self.logger.info(f"成功为集合 {collection_id} 添加标签 {tag_name}")
                    else:
                        self.logger.warning(f"未找到集合 {collection_id} 对应的标签映射: {tag_name}")
                
                # 在批次之间添加短暂延迟，避免请求过于频繁
                await asyncio.sleep(1)
                
        except Exception as e:
            self.logger.error(f"处理集合时发生错误: {str(e)}")
            raise

    async def run(self, base_path: str):
        """运行完整的标签处理流程
        
        Args:
            base_path: 基础路径，包含所有子文件夹
        """
        try:
            await self.initialize()
            
            # 1. 更新文件夹映射
            await self.update_folder_mappings(base_path)
            self.logger.info("文件夹映射更新完成")
            
            # 2. 处理集合和标签
            await self.process_collections()
            self.logger.info("集合处理完成")
            
        except Exception as e:
            self.logger.error(f"处理过程中发生错误: {str(e)}")
            raise
        finally:
            # 确保最后一次保存映射
            self.save_mappings()
            await self.close()
