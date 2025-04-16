import os
import asyncio
import logging
from pathlib import Path
from config.config import Config
from storage.knowledge_base import KnowledgeBaseHandler
from main import VideoProcessor

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def test_tagging_initialization():
    """测试标签服务的初始化"""
    config = Config()
    processor = VideoProcessor(config)
    
    try:
        await processor.initialize()
        assert processor.tagging_service is not None, "标签服务未正确初始化"
        logger.info("标签服务初始化测试通过")
    except Exception as e:
        logger.error(f"标签服务初始化测试失败: {str(e)}")
        raise
    finally:
        if processor.tagging_service:
            await processor.tagging_service.close()

async def test_batch_processing_with_tagging():
    """测试带标签功能的批处理"""
    config = Config()
    processor = VideoProcessor(config)
    
    try:
        await processor.initialize()
        
        # 测试目录路径
        test_dir = "/home/jinpeng/slice_for_video/video_input/问界M7"
        
        logger.info(f"开始测试目录: {test_dir}")
        
        # 检查目录是否存在
        if not os.path.exists(test_dir):
            logger.error(f"测试目录不存在: {test_dir}")
            return
        
        # 执行批处理
        results = await processor.process_all_videos_batch(test_dir)
        
        # 检查结果
        logger.info("\n处理结果统计:")
        success_count = sum(1 for r in results if r.get('status') == 'success')
        failed_count = sum(1 for r in results if r.get('status') == 'failed')
        logger.info(f"成功处理: {success_count} 个视频")
        logger.info(f"处理失败: {failed_count} 个视频")
        
        # 检查标签映射文件
        if os.path.exists(processor.tagging_service.mapping_file):
            logger.info("\n标签映射内容:")
            with open(processor.tagging_service.mapping_file, 'r', encoding='utf-8') as f:
                import json
                mappings = json.load(f)
                for folder, mapping in mappings.items():
                    logger.info(f"文件夹: {folder} -> 标签: {mapping['tag_name']} (ID: {mapping['tag_id']})")
        else:
            logger.warning("标签映射文件不存在")
        
    except Exception as e:
        logger.error(f"批处理测试失败: {str(e)}")
        raise
    finally:
        if processor.tagging_service:
            await processor.tagging_service.close()

async def test_single_video_tagging():
    """测试单个视频的标签处理"""
    config = Config()
    processor = VideoProcessor(config)
    
    try:
        await processor.initialize()
        
        # 测试视频路径
        test_video = "/home/jinpeng/slice_for_video/video_input/BYD ATTO3/7f341c8d-06e9-4a07-b185-69253052159e.mp4"
        
        if not os.path.exists(test_video):
            logger.error(f"测试视频不存在: {test_video}")
            return
        
        logger.info(f"开始处理视频: {test_video}")
        
        # 处理单个视频
        result = await processor.process_single_video(test_video)
        
        # 检查结果
        logger.info("\n处理结果:")
        logger.info(f"状态: {result.get('status')}")
        logger.info(f"数据集ID: {result.get('dataset_id')}")
        logger.info(f"处理时间: {result.get('processing_time')}秒")
        
        # 检查是否成功添加标签
        if result.get('status') == 'success':
            video_dir = os.path.basename(os.path.dirname(test_video))
            if video_dir in processor.tagging_service.tag_mappings:
                tag_info = processor.tagging_service.tag_mappings[video_dir]
                logger.info(f"已添加标签: {tag_info['tag_name']} (ID: {tag_info['tag_id']})")
            else:
                logger.warning(f"未找到目录 {video_dir} 的标签映射")
        
    except Exception as e:
        logger.error(f"单视频处理测试失败: {str(e)}")
        raise
    finally:
        if processor.tagging_service:
            await processor.tagging_service.close()

async def run_all_tests():
    """运行所有测试"""
    logger.info("=== 开始运行标签功能测试 ===")
    
    # 1. 测试初始化
    # logger.info("\n1. 测试标签服务初始化")
    # await test_tagging_initialization()
    
    # 2. 测试批处理
    # logger.info("\n2. 测试批处理功能")
    # await test_batch_processing_with_tagging()
    
    # 3. 测试单视频处理
    logger.info("\n3. 测试单视频处理")
    await test_single_video_tagging()
    
    logger.info("\n=== 所有测试完成 ===")

if __name__ == "__main__":
    asyncio.run(run_all_tests()) 