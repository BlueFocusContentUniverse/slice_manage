# config.py
import yaml
from pathlib import Path
from typing import Any, Dict
import os

with open('config.yaml', 'r') as file:
    _config = yaml.safe_load(file)

class Config:
    """配置管理类
    功能：加载和管理所有配置信息
    """   
    def __init__(self):
        # 视频处理配置
        self.video_config = {
            "input_dir": _config['VideoProcessPath']['input_dir'],
            "output_dir": _config['VideoProcessPath']['output_dir'],
            "frames_dir": _config['VideoProcessPath']['frames_dir'],
            "mask_dir": _config['VideoProcessPath']['mask_dir']
        }
        
        # MinIO 配置
        self.minio_config = {
            "endpoint": _config['MINIO']['endpoint'],
            "access_key": _config['MINIO']['access_key'],
            "secret_key": _config['MINIO']['secret_key'],
            "bucket": _config['MINIO']['bucket'],
            "prefix": _config['MINIO']['prefix']
        }
        
        # API 配置
        self.api_config = {
            "zhipuai_key": _config['API']['zhipuai_api_key'],
            "stepfun_api_key": _config['API']['stepfun_api_key'],
            "new_api_key": _config['API']['openai_api_key']

        }

        # 知识库配置
        self.knowledge_base_config = {
            "username": _config['KnowledgeBase']['username'],
            "password": _config['KnowledgeBase']['password'],
            "datasetId": _config['KnowledgeBase']['datasetId'],
            "parentId": _config['KnowledgeBase']['parentId'],
            "base_url": _config['KnowledgeBase']['base_url'],
            "teamId": _config['KnowledgeBase']['teamId']
        }

        # 切片配置
        self.slice_config = {
            'output_dir': _config['sliceService']['output_dir'],
            'temp_dir': _config['sliceService']['temp_dir'],
            'min_duration': _config['sliceService']['min_duration'],  # 最小分片时长（秒）
            'max_duration': _config['sliceService']['max_duration'],
            'slice_threshold': _config['sliceService']['threshold'],
            'min_scene_len': _config['sliceService']['min_scene_len'],
            'fps': _config['sliceService']['fps'],
            'finish_dir': _config['sliceService']['finish_dir']
        }

        # 分析配置
        self.analyse_config = {
            'api_key': _config['GeminiService']['api_key'],
            'model_name': _config['GeminiService']['model_name'],
            'api_base_url': _config['GeminiService']['api_base_url'],
            'output_dir': _config['GeminiService']['output_dir'],
            'analyze_point': _config['GeminiService']['prompt']
        }


    