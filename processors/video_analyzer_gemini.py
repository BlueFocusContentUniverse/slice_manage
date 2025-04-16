from dataclasses import dataclass
from typing import Dict, List, Optional
import logging
import os
import json
from pathlib import Path
import cv2
from google.cloud import storage
from google.cloud import aiplatform
from openai import OpenAI
from .video_slicer import VideoSlicer, SliceResult
import numpy as np
import subprocess
import tempfile
import httpx

@dataclass
class AnalysisResultGemini:
    """Gemini视频分析结果"""
    success: bool
    analysis_info: Dict = None
    message: str = None
    frames: List[str] = None  # 存储抽取的帧路径
    audio_path: str = None    # 音频文件路径
    token_usage: int = 0

class VideoAnalyzerGemini:
    """使用Gemini进行视频内容理解的分析器"""
    
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        # 创建控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        # 设置日志格式
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        # 将处理器添加到logger
        if not self.logger.handlers:
            self.logger.addHandler(console_handler)
            
        self.slicer = VideoSlicer(config)
        self._init_client()
        
    def _init_client(self) -> None:
        """初始化 API 客户端"""
        try:
            os.environ['OPENAI_API_KEY'] = self.config.api_config['openai_api_key']
            api_key = os.environ.get('OPENAI_API_KEY')
            if not api_key:
                raise ValueError("未找到 API 密钥")
            
            self.client = OpenAI(
                api_key=api_key,
                base_url=self.config.analyse_config['api_base_url'],
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

    def analyze_video(self, video_path: str, prompt: str = None) -> AnalysisResultGemini:
        """分析视频内容"""
        try:
            # 1. 切片视频
            self.logger.info(f"开始处理视频: {video_path}")
            slice_result = self.slicer.slice_video(video_path)
            if not slice_result.success:
                self.logger.error("视频切片失败")
                raise Exception("视频切片失败")

            self.logger.info(f"视频切片成功，共 {len(slice_result.slice_info['segments'])} 个片段")
            all_results = []
            all_frames = [] 
            total_frames = self.config.analyse_config.get('total_frames', 100)
            self.logger.debug(f"计划提取总帧数: {total_frames}")
            processed_frames = 0
            global_conversation_history = []  # 新增：用于存储所有片段的对话历史

            # 如果没有切片，将原始视频作为单个片段处理
            if len(slice_result.slice_info['segments']) == 0:
                self.logger.info("未检测到场景切换，将整个视频作为单个片段处理")
                single_segment = {
                    'output_path': video_path,
                    'duration': slice_result.slice_info['total_duration']
                }
                slice_result.slice_info['segments'] = [single_segment]

            # 2. 处理每个切片
            segments_info = [] 
            for idx, segment in enumerate(slice_result.slice_info['segments'], 1):
                self.logger.info(f"开始处理第 {idx} 个视频片段")
                segment_path = segment['output_path']
                segment_duration = segment['duration']
                total_duration = slice_result.slice_info['total_duration']
                
                # 计算该片段应该抽取的帧数
                frames_for_segment = int((segment_duration / total_duration) * total_frames)
                self.logger.debug(f"片段 {idx} 时长: {segment_duration:.2f}s, 计划提取 {frames_for_segment} 帧")
                # 如果计算得到的帧数为0，跳过该片段
                if frames_for_segment <= 0:
                    self.logger.warning(f"片段 {idx} 计算得到的帧数为 {frames_for_segment}，太短将跳过处理")
                    continue
                # 抽取帧和音频
                self.logger.debug(f"开始从片段 {idx} 抽取帧")
                frames = self._extract_frames(segment_path, frames_for_segment)
                self.logger.debug(f"成功从片段 {idx} 抽取 {len(frames)} 帧")

                self.logger.debug(f"开始从片段 {idx} 提取音频")
                audio_path = self._extract_audio(segment_path)
                self.logger.debug(f"音频提取成功: {audio_path}")
                
                all_frames.extend(frames)

                segment_info = {
                'start_frame': processed_frames,
                'end_frame': processed_frames + len(frames),
                'total_frames': total_frames
                }
                
                # 构建 API 请求
                self.logger.info(f"开始处理片段 {idx} 的API请求")
                segment_result = self._process_segment_all_frames(
                    frames, 
                    audio_path, 
                    prompt,
                    segment_info,
                    global_conversation_history   
                )
                if 'conversation_history' in segment_result:  # 新增：获取更新后的对话历史
                    global_conversation_history = segment_result['conversation_history']
            
                
                all_results.append({
                    'result': segment_result,
                    'video_path': segment_path
                })
                self.logger.info(f"片段 {idx} 处理完成")
                self.logger.debug(f"当前已处理 {len(all_frames)} 帧")
                processed_frames += len(frames)
                

            # 3. 合并所有结果
            combined_result = self._combine_results(all_results)
            
            return AnalysisResultGemini(
                success=True,
                analysis_info=combined_result,
                frames=all_frames, # 这里可以选择保留一些关键帧
                audio_path=None
            )

        except Exception as e:
            error_msg = f"视频分析失败: {str(e)}"
            self.logger.error(error_msg)
            return AnalysisResultGemini(success=False, message=error_msg)
        
    def _combine_results(self, results: List[Dict]) -> Dict:
        """合并所有片段的分析结果"""
        # 检查输入类型，判断是来自单个片段处理还是整体视频处理
        if results and 'result' in results[0]:
            # 来自整体视频处理（analyze_video方法）
            combined = {
                "segments": [],
                "total_token_usage" : 0
            }
            
            
            for segment_info in results:
                result = segment_info['result']
                video_path = segment_info['video_path']
                
                segment_token_usage = 0
                if isinstance(result, dict):
                    # 如果是整体结果
                    segment_token_usage = result.get('total_token_usage', 0)
                    segments_data = result.get('segments', [])
                
                self.logger.debug(f"片段 token 使用量: {segment_token_usage}")
                
                # 构建片段数据
                segment_data = {
                    "analysis": segments_data,
                    "token_usage": segment_token_usage,
                    "video_path": video_path
                }
                
                combined["segments"].append(segment_data)
                combined["total_token_usage"] += segment_token_usage
        
            
        else:
            # 来自单个片段处理（_process_segment_all_frames方法）
            combined = {
                "segments": results,
                "total_token_usage": sum(r.get('token_usage', 0) for r in results)
            }
        
        return combined
        
    def _process_segment_all_frames(self, 
                              frames: List[str], 
                              audio_path: str, 
                              prompt: str,
                              segment_info: Dict,
                              conversation_history: List = None) -> Dict:
        """一次性处理包含所有帧的视频片段"""
        try:
            import base64
            import json
            from itertools import islice
            import time
            
            process_start_time = time.time()
            MAX_IMAGES_PER_REQUEST = 8
            all_responses = []
            conversation_history = conversation_history or []
            
            # 计算需要分成多少批次
            total_frames = len(frames)
            num_batches = (total_frames + MAX_IMAGES_PER_REQUEST - 1) // MAX_IMAGES_PER_REQUEST
            
            self.logger.info(f"开始处理视频片段，总帧数: {total_frames}, 分成 {num_batches} 批处理")
            self.logger.debug(f"每批最大帧数: {MAX_IMAGES_PER_REQUEST}")

            # 获取段的起始位置信息
            segment_start_frame = segment_info['start_frame']
            video_total_frames = segment_info['total_frames']
            
            # 按批次处理帧
            for batch_idx in range(num_batches):
                batch_start_time = time.time()
                start_idx = batch_idx * MAX_IMAGES_PER_REQUEST
                end_idx = min(start_idx + MAX_IMAGES_PER_REQUEST, total_frames)
                batch_frames = frames[start_idx:end_idx]

                global_start_idx = segment_start_frame + start_idx
                global_end_idx = segment_start_frame + end_idx
                
                self.logger.info(f"开始处理第 {batch_idx + 1}/{num_batches} 批")
                self.logger.debug(f"当前批次帧范围: {start_idx} - {end_idx}, 包含 {len(batch_frames)} 帧")
                
                # 构建消息
                message_start_time = time.time()
                messages = []
                messages.extend(conversation_history)
                
                # 构建当前批次消息
                prompt_text = self._build_prompt(prompt, batch_idx, global_start_idx, global_end_idx, video_total_frames, conversation_history)
                self.logger.debug(f"生成的提示词长度: {len(prompt_text)} 字符")
                
                current_message = {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": prompt_text
                        }
                    ]
                }
                
                # 添加图片内容
                image_start_time = time.time()
                for frame_idx, frame in enumerate(batch_frames):
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
                        self.logger.error(f"处理图片失败 (批次 {batch_idx + 1}, 帧 {frame_idx}): {str(e)}")
                        raise
                
                self.logger.debug(f"图片处理耗时: {time.time() - image_start_time:.2f}秒")
                
                # 只在第一批次添加音频
                if batch_idx == 0:
                    audio_start_time = time.time()
                    try:
                        with open(audio_path, "rb") as audio_file:
                            audio_data = base64.b64encode(audio_file.read()).decode('utf-8')
                            current_message["content"].append({
                                "type": "audio_url",
                                "audio_url": {
                                    "url": f"data:audio/wav;base64,{audio_data}"
                                }
                            })
                        self.logger.debug(f"音频处理耗时: {time.time() - audio_start_time:.2f}秒")
                    except Exception as e:
                        self.logger.error(f"处理音频失败: {str(e)}")
                        raise
                
                messages.append(current_message)
                self.logger.debug(f"消息构建耗时: {time.time() - message_start_time:.2f}秒")
                

                # 发送API请求
                max_retries = 3
                retry_count = 0
                last_error = None
                success = False
                
                while retry_count < max_retries:
                    try:
                        self.logger.info(f"发送第 {batch_idx + 1}/{num_batches} 批API请求 (尝试 {retry_count + 1}/{max_retries})")
                        api_start_time = time.time()
                        
                        response = self.client.chat.completions.create(
                            model="gemini-1.5-pro",
                            messages=messages,
                            max_tokens=2048,
                            temperature=0.7,
                            timeout=90  
                        )
                        
                        api_duration = time.time() - api_start_time
                        self.logger.info(f"API请求完成，耗时: {api_duration:.2f}秒")
                        
                        # 验证响应是否有效
                        if (not response or not response.choices or 
                            not hasattr(response.choices[0], 'message') or 
                            not response.choices[0].message or 
                            not response.choices[0].message.content):
                            raise ValueError("API返回无效响应")
                        
                        # 添加详细的响应日志
                        self.logger.debug(f"完整的API响应: {response}")
                        self.logger.debug(f"响应类型: {type(response)}")
                        self.logger.debug(f"响应属性: {dir(response)}")
                        
                        response_content = response.choices[0].message.content
                        self.logger.debug(f"响应内容长度: {len(response_content)} 字符")
                        
                        # 如果响应有效，保存并退出重试循环
                        assistant_response = {
                            "role": "assistant",
                            "content": [{"type": "text", "text": response_content}]
                        }
                        conversation_history.append(assistant_response)
                        
                        # 记录token使用情况
                        token_usage = response.usage.total_tokens if hasattr(response, 'usage') else 0
                        self.logger.debug(f"本次请求token使用量: {token_usage}")
                        
                        all_responses.append({
                            "batch_idx": batch_idx,
                            "segment_analysis": response_content,
                            "token_usage": token_usage
                        })
                        
                        self.logger.info(f"批次 {batch_idx + 1} 处理完成，总耗时: {time.time() - batch_start_time:.2f}秒")
                        success = True
                        break  # 成功获取响应，退出重试循环
                        
                    except Exception as e:
                        last_error = e
                        retry_count += 1
                        self.logger.error(f"批次 {batch_idx + 1} API请求失败 (尝试 {retry_count}/{max_retries}): {str(e)}")
                        
                        if retry_count < max_retries:
                            retry_delay = 2 ** retry_count  # 指数退避策略
                            self.logger.info(f"等待 {retry_delay} 秒后重试...")
                            time.sleep(retry_delay)
                        else:
                            self.logger.error("已达到最大重试次数，放弃该批次处理")
                            self.logger.exception("最后一次错误的详细信息:")
                if not success:
                    error_response = {
                        "role": "assistant",
                        "content": [{"type": "text", "text": f"视频片段 {batch_idx + 1} 没有返回有效解析结果。"}]
                    }
                    conversation_history.append(error_response)
                    
                    all_responses.append({
                        "batch_idx": batch_idx,
                        "segment_analysis": f"视频片段 {batch_idx + 1} 没有返回有效解析结果。[错误原因：{str(last_error)}]",
                        "token_usage": 0
                    })
                    
                    self.logger.warning(f"批次 {batch_idx + 1} 使用默认错误信息作为解析结果")
                

            
            # 合并所有批次的结果
            self.logger.info("开始合并所有批次结果")
            combined_analysis = self._combine_results(all_responses)
            combined_analysis['conversation_history'] = conversation_history
            
            total_process_time = time.time() - process_start_time
            self.logger.info(f"所有批次处理完成，总耗时: {total_process_time:.2f}秒")
            self.logger.debug(f"平均每批处理时间: {total_process_time/num_batches:.2f}秒")
            
            return combined_analysis
            
        except Exception as e:
            self.logger.error("处理视频片段失败")
            self.logger.exception("详细错误堆栈:")
            raise RuntimeError(f"处理视频片段失败: {str(e)}")

    def _build_prompt(self, 
                base_prompt: str, 
                batch_idx: int, 
                start_idx: int, 
                end_idx: int, 
                total_frames: int, 
                history: list) -> str:
        """构建包含上下文的提示词"""
        # 计算当前位置在整个视频中的百分比
        start_percentage = (start_idx / total_frames) * 100
        end_percentage = (end_idx / total_frames) * 100

        self.logger.info(f"视频处理进度 ==> {start_percentage:.1f}% - {end_percentage:.1f}%")
        self.logger.debug(f"处理帧范围: {start_idx + 1} - {end_idx} / {total_frames} 帧")
        
        if not history:
            self.logger.info("构建首次分析提示词（无历史记录）")
            # 第一批次的提示词
            return f"""{base_prompt}

                当前分析范围：
                - 帧位置：第 {start_idx + 1} 到 {end_idx} 帧（共 {total_frames} 帧）
                - 视频进度：{start_percentage:.1f}% - {end_percentage:.1f}%

                当前是视频的第一段内容，请基于这个时间轴位置分析视频内容。"""
        else:
            self.logger.info("构建后续分析提示词（有历史记录）")
            self.logger.debug(f"历史记录长度: {len(history)}")
            recent_history = history[-3:] if len(history) > 3 else history
            history_summary = "\n".join([
                msg["content"][0]["text"] if isinstance(msg.get("content"), list) else str(msg.get("content", ""))
                for msg in recent_history
            ])
            # 后续批次的提示词，包含之前分析的概要
            return f"""基于之前的分析继续分析视频片段。

                - 分析历史结果: {history_summary}

                当前分析进度：
                - 已完成分析：0% - {start_percentage:.1f}%
                - 当前分析区间：{start_percentage:.1f}% - {end_percentage:.1f}%
                - 待分析区间：{end_percentage:.1f}% - 100%
                - 具体帧位置：第 {start_idx + 1} 到 {end_idx} 帧（共 {total_frames} 帧）
                

                请注意：
                1. 当前正在分析视频的 {start_percentage:.1f}% - {end_percentage:.1f}% 部分
                2. 请与之前分析保持连贯性
                3. 重点关注在这个时间段内的关键变化
                4. 请不要重复输出历史记录中的语音信息和画面信息，只作为理解视频内容的参考
                5. 只输出给到image_url和audio_url的中内容的分析结果，包括画面信息和语音信息，不要输出任何多余的解释性文字。

                原始提示词：{base_prompt}"""
    # def _process_segment(self, 
    #                     frames: List[str], 
    #                     audio_path: str, 
    #                     prompt: str) -> Dict:
    #     """处理单个视频片段"""
    #     try:
    #         # 构建API请求
    #         content = [{"type": "text", "text": prompt}]
            
    #         # 添加帧
    #         for frame in frames:
    #             with open(frame, 'rb') as f:
    #                 content.append({
    #                     "type": "image",
    #                     "image": {
    #                         "data": f.read()
    #                     }
    #                 })
            
    #         # 添加音频
    #         with open(audio_path, 'rb') as f:
    #             content.append({
    #                 "type": "audio",
    #                 "audio": {
    #                     "data": f.read()
    #                 }
    #             })
            
    #         response = self.client.chat.completions.create(
    #             model=self.config.analyse_config['model_name'],
    #             messages=[{
    #                 "role": "user",
    #                 "content": content
    #             }]
    #         )
            
    #         return {
    #             "segment_analysis": response.choices[0].message.content,
    #             "token_usage": response.usage.total_tokens
    #         }
            
    #     except Exception as e:
    #         self.logger.error(f"片段处理失败: {str(e)}")
    #         raise



    # def _build_default_prompt(self) -> str:
    #     """构建默认的提示词"""
    #     return (
    #         "请分析这段视频内容，包括以下方面：\n"
    #         "1. 人物动作和表情\n"
    #         "2. 车辆外观和内饰细节\n"
    #         "3. 场景环境和氛围\n"
    #         "4. 音频内容和背景音乐\n"
    #         "5. 重要的视觉元素和关键时刻"
    #     )