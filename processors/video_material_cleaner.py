import os
import json
import shutil
import logging
from pathlib import Path
from typing import List, Dict
import cv2
import numpy as np
import tempfile
from datetime import datetime
from .video_analyzer_gemini import VideoAnalyzerGemini

class DictToObject:
    """将字典转换为对象，同时保持字典访问方式"""
    def __init__(self, dictionary):
        for key, value in dictionary.items():
            if isinstance(value, dict):
                setattr(self, key, DictToObject(value))
            else:
                setattr(self, key, value)
    
    def __getitem__(self, key):
        return getattr(self, key)

class VideoMaterialCleaner:
    """视频素材清洗器"""
    
    def __init__(self, config: Dict):
        self.config = DictToObject(config)
        self.analyzer = VideoAnalyzerGemini(self.config)
        self.logger = logging.getLogger(__name__)
        self.base_backup_dir = Path("/home/jinpeng/slice_for_video/backup")
        self.failed_log_path = Path("failed_videos.json")
        self.base_backup_dir.mkdir(parents=True, exist_ok=True)
        
        # 配置日志
        self._setup_logging()
        
    def _setup_logging(self):
        """设置日志系统"""
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"material_cleaning_{timestamp}.log"
        
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        
        self.logger.handlers.clear()
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        self.logger.setLevel(logging.INFO)
    
    def _extract_frames(self, video_path: str, num_frames: int = 8) -> List[str]:
        """从视频中抽取指定数量的帧"""
        try:
            cap = cv2.VideoCapture(str(video_path))
            total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            frame_indices = np.linspace(0, total_frames-1, num_frames, dtype=int)
            
            frames = []
            temp_dir = tempfile.mkdtemp()
            
            for idx, frame_no in enumerate(frame_indices):
                cap.set(cv2.CAP_PROP_POS_FRAMES, frame_no)
                ret, frame = cap.read()
                if ret:
                    frame_path = os.path.join(temp_dir, f"frame_{idx}.jpg")
                    cv2.imwrite(frame_path, frame)
                    frames.append(frame_path)
            
            cap.release()
            return frames
            
        except Exception as e:
            self.logger.error(f"抽帧失败: {str(e)}")
            raise
    
    def _check_text_in_video(self, video_path: str) -> int:
        """检查视频中是否包含文字"""
        prompt = """检查画面中是否有需要删除的文字内容。请只输出数字0或1，不要有任何其他说明。

判断标准如下：
1. 需要删除的文字（输出1）：
   - 后期添加的字幕或花字
   - 视频配音的字幕
   - 在8帧内频繁变化的文字内容
   - 遮挡主要画面内容的文字

2. 不需要删除的文字（输出0）：
   - 汽车原有的实体文字（如方向盘、仪表盘、中控屏幕上的文字）
   - 在8帧内保持不变的文字
   - 仅在1-2帧中出现的文字
   - 场景中自然存在的文字（如路牌、广告牌等）

请严格按照以上标准，仅输出0或1。"""
        try:
            # 抽取帧
            frames = self._extract_frames(video_path)
            self.logger.info(f"成功从视频抽取 {len(frames)} 帧")
            
            # 构建API请求
            import base64
            messages = [{
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": prompt
                    }
                ]
            }]
            
            # 添加图片内容
            for frame in frames:
                with open(frame, "rb") as image_file:
                    image_data = base64.b64encode(image_file.read()).decode('utf-8')
                    messages[0]["content"].append({
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/jpeg;base64,{image_data}"
                        }
                    })
            
            # 发送API请求
            response = self.analyzer.client.chat.completions.create(
                model="gemini-1.5-pro",
                messages=messages,
                max_tokens=2048,
                temperature=0.7
            )
            
            # 解析结果
            if not response or not response.choices:
                self.logger.error("API返回无效响应")
                return -1
            
            result = response.choices[0].message.content.strip()
            self.logger.info(f"Gemini返回结果: {result}")
            
            if result == "1":
                return 1
            elif result == "0":
                return 0
            else:
                self.logger.warning(f"无效的返回结果: {result}")
                return -1
            
        except Exception as e:
            self.logger.error(f"处理视频时发生错误: {str(e)}")
            return -1
        finally:
            # 清理临时文件
            for frame in frames:
                try:
                    os.remove(frame)
                except:
                    pass
    
    def _record_failed_video(self, video_path: str):
        """记录处理失败的视频"""
        failed_videos = []
        if self.failed_log_path.exists():
            with open(self.failed_log_path, 'r', encoding='utf-8') as f:
                failed_videos = json.load(f)
                
        failed_videos.append(str(video_path))
        
        with open(self.failed_log_path, 'w', encoding='utf-8') as f:
            json.dump(failed_videos, f, ensure_ascii=False, indent=2)
   
    def _get_all_videos(self, input_dir: str) -> List[Path]:
        """递归获取所有视频文件"""
        video_extensions = {'.mp4', '.avi', '.mov', '.mkv'}
        input_path = Path(input_dir)
        
        video_files = []
        for file_path in input_path.rglob("*"):
            if file_path.suffix.lower() in video_extensions:
                video_files.append(file_path)
                
        self.logger.info(f"找到 {len(video_files)} 个视频文件")
        return video_files
    
    def clean_materials(self, input_dir: str):
        """清洗视频素材"""
        input_path = Path(input_dir)
        # 获取输入目录的最后一个文件夹名称
        folder_name = input_path.name
        # 创建对应的备份目录
        backup_dir = self.base_backup_dir / folder_name
        backup_dir.mkdir(parents=True, exist_ok=True)
        video_files = self._get_all_videos(input_dir)
        
        for video_path in video_files:
            self.logger.info(f"处理视频: {video_path}")
            
            result = self._check_text_in_video(str(video_path))
            
            if result == 1:
                # 移动到备份目录
                backup_path = backup_dir / video_path.name
                shutil.move(str(video_path), str(backup_path))
                self.logger.info(f"发现文字，已移动视频到: {backup_path}")
                
            elif result == -1:
                # 记录处理失败的视频
                self._record_failed_video(str(video_path))
                self.logger.warning(f"处理失败，已记录: {video_path}")
                
            else:
                self.logger.info(f"未发现文字，保留视频: {video_path}")

    def process_directories(self, input_dirs: List[str]):
        """处理多个输入目录"""
        self.logger.info("开始视频素材清洗任务")
        
        for dir_path in input_dirs:
            if not os.path.exists(dir_path):
                self.logger.error(f"文件夹不存在: {dir_path}")
                continue
                
            if not os.path.isdir(dir_path):
                self.logger.error(f"不是有效的目录: {dir_path}")
                continue
            
            try:
                self.logger.info(f"开始处理文件夹: {dir_path}")
                self.clean_materials(dir_path)
                self.logger.info(f"完成文件夹处理: {dir_path}")
                
            except Exception as e:
                self.logger.error(f"处理文件夹时出错 {dir_path}: {str(e)}")
                continue
        
        self.logger.info("所有文件夹处理完成")
        
        if self.failed_log_path.exists():
            with open(self.failed_log_path, 'r', encoding='utf-8') as f:
                failed_videos = json.load(f)
                self.logger.info(f"处理失败的视频数量: {len(failed_videos)}")









