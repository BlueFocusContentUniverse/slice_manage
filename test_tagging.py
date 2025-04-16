import os
import asyncio
import logging
import pytest
from pathlib import Path
from config.config import Config
from storage.knowledge_base import KnowledgeBaseHandler
from processors.video_analyzer import VideoAnalyzer
from tagging import TaggingService

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def test_process_folder_name():
    """测试文件夹名称处理功能"""
    config = Config()
    kb_handler = KnowledgeBaseHandler(config)
    analyzer = VideoAnalyzer(config)
    async with kb_handler:
        service = TaggingService(config, kb_handler)
        await service.initialize()
        
        test_cases = [
            "理想L7白色",
            "竞品车型————理想L7",
            "阿维塔11",
            "理想L8 MAX"
        ]
        
        logger.info("开始测试文件夹名称处理...")
        for folder_name in test_cases:
            try:
                result = await service.process_folder_name(folder_name)
                logger.info(f"输入: {folder_name} -> 输出: {result}")
            except Exception as e:
                logger.error(f"处理文件夹名称失败 {folder_name}: {str(e)}")
        
        await service.close()

async def test_create_and_load_mappings():
    """测试标签映射的创建和加载功能"""
    config = Config()
    kb_handler = KnowledgeBaseHandler(config)
    async with kb_handler:
        service = TaggingService(config, kb_handler)
        await service.initialize()
        
        # 测试目录路径
        test_path = "/home/jinpeng/slice_for_video/video_input"
        
        logger.info("开始测试标签映射功能...")
        try:
            # 更新映射
            await service.update_folder_mappings(test_path)
            
            # 检查映射文件是否创建
            if os.path.exists(service.mapping_file):
                logger.info(f"映射文件已创建: {service.mapping_file}")
                logger.info("当前映射内容:")
                for folder, mapping in service.tag_mappings.items():
                    logger.info(f"文件夹: {folder} -> 标签: {mapping['tag_name']} (ID: {mapping['tag_id']})")
            else:
                logger.warning("映射文件未创建")
        except Exception as e:
            logger.error(f"测试标签映射失败: {str(e)}")
        
        await service.close()

async def test_process_collections():
    """测试集合处理功能"""
    config = Config()
    kb_handler = KnowledgeBaseHandler(config)
    async with kb_handler:
        service = TaggingService(config, kb_handler)
        await service.initialize()
        
        logger.info("开始测试集合处理...")
        try:
            await service.process_collections()
        except Exception as e:
            logger.error(f"处理集合失败: {str(e)}")
        
        await service.close()

async def run_all_tests():
    """运行所有测试"""
    logger.info("=== 开始运行所有测试 ===")
    
    # 测试文件夹名称处理
    logger.info("\n1. 测试文件夹名称处理")
    await test_process_folder_name()
    
    # 测试标签映射
    logger.info("\n2. 测试标签映射功能")
    await test_create_and_load_mappings()
    
    # 测试集合处理
    logger.info("\n3. 测试集合处理")
    await test_process_collections()
    
    logger.info("\n=== 所有测试完成 ===")

if __name__ == "__main__":
    asyncio.run(run_all_tests())