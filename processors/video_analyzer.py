from openai import OpenAI
import logging
import json
import os
import time
import cv2
import numpy as np
import tempfile
import subprocess
from dataclasses import dataclass
from typing import Dict, List, Optional
from pathlib import Path
import base64
import httpx
@dataclass
class AnalysisResult:
    """视频分析结果"""
    success: bool
    analysis_info: Dict = None
    message: str = None
    frames: List[str] = None  # 存储抽取的帧路径
    audio_path: str = None    # 音频文件路径
    token_usage: int = 0

class VideoAnalyzer:
    """视频分析器 - 使用 Gemini API 进行视频内容理解"""
    
    def __init__(self, config):
        # 加载配置
        self.config = config.analyse_config
        
        # 初始化日志
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        
        # 设置日志处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        if not self.logger.handlers:
            self.logger.addHandler(console_handler)
        
        # 设置 API 客户端
        self._init_client()

    def _init_client(self) -> None:
        """初始化 API 客户端"""
        try:
            os.environ['OPENAI_API_KEY'] = self.config['api_key']
            api_key = os.environ.get('OPENAI_API_KEY')
            if not api_key:
                raise ValueError("未找到 API 密钥")
            
            self.client = OpenAI(
                api_key=api_key,
                base_url=self.config['api_base_url'],
                http_client=httpx.Client(
                proxies={
                    "http://": f"http://172.22.93.27:1081",
                    "https://": f"http://172.22.93.27:1081"
                })
            )
        except Exception as e:
            self.logger.error(f"API 客户端初始化失败: {str(e)}")
            raise

    def _extract_frames(self, video_path: str, num_frames: int) -> List[str]:
        """从视频中抽取指定数量的帧"""
        try:
            cap = cv2.VideoCapture(video_path)
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

    def _extract_audio(self, video_path: str) -> str:
        """从视频中提取音频"""
        try:
            audio_path = os.path.join(tempfile.mkdtemp(), "audio.wav")
            cmd = [
                'ffmpeg', '-i', video_path,
                '-vn',  # 不处理视频
                '-acodec', 'pcm_s16le',  # 设置音频编码
                '-ar', '44100',  # 采样率
                '-ac', '2',  # 声道数
                audio_path
            ]
            
            subprocess.run(cmd, check=True)
            return audio_path
            
        except Exception as e:
            self.logger.error(f"音频提取失败: {str(e)}")
            raise

    def analyze_video_slice(self, 
                          video_path: str, 
                          title: str,
                          prev_analysis_result: str = "",
                          custom_analysis_dimensions: str = None) -> AnalysisResult:
        """
        分析视频片段
        
        Args:
            video_path: 视频片段路径
            title: 视频标题
            prev_analysis_result: 前一个视频片段的分析结果（用于保持连贯性）
            custom_analysis_dimensions: 自定义的解析维度，可以替换默认的维度列表
            
        Returns:
            AnalysisResult: 分析结果对象
        """
        try:
            self.logger.info(f"开始分析视频片段: {video_path}")
            # 抽取帧和音频
            frames_count = self.config.get('frames_per_slice', 2)  # 每个片段抽取的帧数
            self.logger.info(f"开始从视频片段抽取 {frames_count} 帧")
            frames = self._extract_frames(video_path, frames_count)
            self.logger.info(f"成功抽取 {len(frames)} 帧")
            
            # self.logger.info("开始提取音频")
            # audio_path = self._extract_audio(video_path)
            # self.logger.info(f"音频提取成功: {audio_path}")

            # 构建提示词
            prompt = self._build_prompt(
                title=title, 
                prev_analysis_result=prev_analysis_result,
                custom_analysis_dimensions=custom_analysis_dimensions
            )
            
            # 构建 API 请求内容
            messages = []
            current_message = {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": prompt
                    }
                ]
            }

            # 添加帧内容
            image_start_time = time.time()
            for frame_idx, frame in enumerate(frames):
                try:
                    with open(frame, "rb") as image_file:
                        image_data = base64.b64encode(image_file.read()).decode('utf-8')
                        current_message["content"].append({
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{image_data}"
                            }
                        })
                except Exception as e:
                    self.logger.error(f"处理图片失败 (帧 {frame_idx}): {str(e)}")
                    raise
            
            self.logger.debug(f"图片处理耗时: {time.time() - image_start_time:.2f}秒")

            # 添加音频内容
            # audio_start_time = time.time()
            # try:
            #     with open(audio_path, "rb") as audio_file:
            #         audio_data = base64.b64encode(audio_file.read()).decode('utf-8')
            #         current_message["content"].append({
            #             "type": "audio_url",
            #             "audio_url": {
            #                 "url": f"data:audio/wav;base64,{audio_data}"
            #             }
            #         })
            #     self.logger.debug(f"音频处理耗时: {time.time() - audio_start_time:.2f}秒")
            # except Exception as e:
            #     self.logger.error(f"处理音频失败: {str(e)}")
            #     raise

            messages.append(current_message)

            # 调用 API
            max_retries = 3
            retry_count = 0
            last_error = None
            
            while retry_count < max_retries:
                try:
                    self.logger.info(f"发送API请求 (尝试 {retry_count + 1}/{max_retries})")
                    # self.logger.info(f"messages: {messages}")
                    api_start_time = time.time()
                    
                    response = self.client.chat.completions.create(
                        model=self.config['model_name'],
                        messages=messages
                    )
                    
                    api_duration = time.time() - api_start_time
                    self.logger.info(f"API请求完成，耗时: {api_duration:.2f}秒")
                    
                    # 验证响应
                    if (not response or not response.choices or 
                        not hasattr(response.choices[0], 'message') or 
                        not response.choices[0].message or 
                        not response.choices[0].message.content):
                        raise ValueError("API返回无效响应")
                    
                    result_content = response.choices[0].message.content
                    token_usage = response.usage.total_tokens if hasattr(response, 'usage') else 0
                    
                    # 构建结果数据
                    analysis_info = {
                        "video_path": video_path,
                        "analysis_result": result_content,
                        "token_usage": token_usage
                    }
                    
                    return AnalysisResult(
                        success=True,
                        analysis_info=analysis_info,
                        message="分析完成",
                        frames=frames,
                        audio_path=''
                    )
                    
                except Exception as e:
                    last_error = e
                    retry_count += 1
                    self.logger.error(f"API请求失败 (尝试 {retry_count}/{max_retries}): {str(e)}")
                    
                    if retry_count < max_retries:
                        retry_delay = 2 ** retry_count
                        self.logger.info(f"等待 {retry_delay} 秒后重试...")
                        time.sleep(retry_delay)
            
            # 如果所有重试都失败，返回错误结果
            error_msg = f"视频分析失败: {str(last_error)}"
            self.logger.error(error_msg)
            return AnalysisResult(
                success=False,
                message=f"没有返回有效解析结果。[错误原因：{str(last_error)}]",
                frames=frames,
                audio_path=''
            )

        except Exception as e:
            error_msg = f"视频分析失败: {str(e)}"
            self.logger.error(error_msg)
            return AnalysisResult(False, message=error_msg)

    def _build_prompt(self, title:str = "", prev_analysis_result: str = "", custom_analysis_dimensions: str = None) -> str:
        """
        构建提示词
        
        Args:
            title: 视频标题
            prev_analysis_result: 前一个视频片段的分析结果
            custom_analysis_dimensions: 自定义的解析维度，可以替换默认的维度列表
            
        Returns:
            str: 完整的提示词
        """
        # 默认的解析维度
        default_dimensions = (
            "1. **人物动作**：开车、下车、上车、行走、开车门、敲车门、说话等等。\n"
            "2. **内容主体**：如车头特写、车尾特写、侧面特写、轮胎特写、车灯特写、内饰特写等。\n"
            "3. **主体状态描述**：如加速、刹车、转弯、漂移、越野、稳定巡航、疾驰而过等。\n"
            "4. **相机视角**：俯视视角、仰视视角、平视视角、鸟瞰视角等。\n"
            "5. **地点**：城市街道、高速公路、乡村道路、海滨公路等。\n"
            "6. **时间**：清晨、上午、中午、下午、黄昏、夜晚。\n"
            f"7. **品牌**："
        )
        
        # 使用自定义维度或默认维度
        analysis_dimensions = custom_analysis_dimensions if custom_analysis_dimensions else default_dimensions
        
        # 构建完整提示词
        return (
            "### 汽车推广短视频片段解析提示词\n"
            f"视频解析的目的是从多个角度拆解内容，为剪辑师提供有价值的素材片段信息，"
            "同时视频的标题作为全视频内容的概览提供了重要的参考价值。如果视频标题没有意义，请忽略\n"
            f"**为了保证解析出的内容连贯，我会提供上一个片段的解析信息：'{prev_analysis_result}'。"
            "如果没有提供上一个片段的内容，则说明这是第一个片段。**\n"
            "**请根据以下角度解析视频内容：**\n"
            f"{analysis_dimensions}\n"
            f"输出时请输出一段画面描述和一个表格，其中画面描述包含以上解析维度，"
            "如果没有车，品牌车型直接输出为无！！！\n"
            "请严格按照示例表格输出解析结果，一条视频只有一个镜头，禁止输出多个镜头。如果视频中没有相关信息，直接输出无\n"
            "请将音频时长作为镜头时长，如果音频时长为0，请输出2秒\n"
           
        )