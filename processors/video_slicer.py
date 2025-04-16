# processors/video_slicer.py
import scenedetect
from scenedetect.detectors import ContentDetector
from pathlib import Path
import logging
from dataclasses import dataclass
from typing import List, Dict, Optional
import json
import os
import subprocess
import ffmpeg

@dataclass
class SliceResult:
    """视频分片结果"""
    success: bool
    slice_info: Dict = None
    message: str = None
    frames_count: int = 0
    duration: float = 0

class VideoSlicer:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        
        # 创建控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        
        # 创建格式器
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        
        # 确保处理器只被添加一次
        if not self.logger.handlers:
            self.logger.addHandler(console_handler)
        self._setup_paths()
        self.slice_info = None

    def _setup_paths(self) -> None:
        """设置必要的路径"""
        for path in [
            self.config.slice_config['output_dir'],
            self.config.slice_config['temp_dir']
        ]:
            os.makedirs(path, exist_ok=True)

    def _get_video_duration(self, video_path: str) -> tuple:
        """使用 ffprobe 获取视频时长和帧数"""
        try:
            cmd = [
                'ffprobe', 
                '-v', 'error',
                '-select_streams', 'v:0',
                '-show_entries', 'stream=nb_frames,r_frame_rate',
                '-of', 'json',
                video_path
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            data = json.loads(result.stdout)
            
            # 获取帧率
            fps_str = data['streams'][0]['r_frame_rate']
            fps_num, fps_den = map(int, fps_str.split('/'))
            fps = fps_num / fps_den
            
            # 获取总帧数
            frame_count = int(data['streams'][0]['nb_frames'])
            
            duration = frame_count / fps
            
            return frame_count, duration, fps
            
        except Exception as e:
            self.logger.error(f"获取视频信息失败: {str(e)}")
            raise

    def slice_video(self, video_path: str, threshold: float = 27.0) -> SliceResult:
        """使用场景检测进行视频分片"""
        try:
            print(f"开始处理视频: {video_path}")
            
            # 验证输入文件
            if not os.path.exists(video_path):
                raise FileNotFoundError(f"输入视频文件不存在: {video_path}")
            
            # 验证输出目录
            output_dir = self.config.slice_config['output_dir']
            os.makedirs(output_dir, exist_ok=True)
            
            # 获取视频基本信息
            frame_count, duration, fps = self._get_video_duration(video_path)
            self.logger.debug(f"视频信息 - 帧数: {frame_count}, 时长: {duration}秒, FPS: {fps}")
            
            try:
                self.logger.info(f"准备开始场景检测，阈值: {threshold}")
                print(f"开始场景检测，阈值: {threshold}")
                
                # 添加进度提示
                self.logger.debug("正在初始化场景检测器...")
                detector = ContentDetector(threshold=threshold)
                
                self.logger.debug("开始执行场景检测...")
                scenes = scenedetect.detect(
                    video_path, 
                    detector,
                    show_progress=True  # 显示进度条
                )
                
                self.logger.info(f"场景检测完成，检测到 {len(scenes)} 个场景，详细信息：{scenes}")
                print(f"检测到 {len(scenes)} 个场景")
                
            except Exception as e:
                self.logger.error(f"场景检测过程中发生错误: {str(e)}")
                self.logger.exception("详细错误信息:")
                raise RuntimeError(f"场景检测失败: {str(e)}")
            
            # 执行分片
            self.slice_info = self._perform_slicing(
                video_path, 
                scenes, 
                {"frame_count": frame_count, "duration": duration, "fps": fps}
            )
            
            return SliceResult(
                success=True,
                slice_info=self.slice_info,
                frames_count=frame_count,
                duration=duration
            )

        except Exception as e:
            error_msg = f"视频分片失败: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return SliceResult(success=False, message=error_msg)

    def _perform_slicing(self, video_path: str, scenes: List, video_info: Dict) -> Dict:
        """执行视频分片"""
        
        
        video_name = Path(video_path).stem
        self.logger.debug(f"处理视频: {video_name}")
        
        slice_info = {
            "original_video": video_path,
            "total_duration": video_info["duration"],
            "total_frames": video_info["frame_count"],
            "fps": video_info["fps"],
            "segments": []
        }

        for i, scene in enumerate(scenes):
            try:
                start_frame, end_frame = scene[0].get_frames(), scene[1].get_frames()
                
                # 计算时间点
                start_time = start_frame / video_info["fps"]
                end_time = end_frame / video_info["fps"]
                duration = end_time - start_time
                
                self.logger.debug(
                    f"片段 {i+1} - 开始: {start_time:.2f}s, 结束: {end_time:.2f}s, "
                    f"时长: {duration:.2f}s"
                )

                # 如果片段太短，跳过
                if duration < self.config.slice_config.get('min_duration', 0.1):
                    self.logger.debug(f"片段 {i+1} 太短，跳过")
                    continue
                else:
                    output_path = os.path.join(
                        self.config.slice_config['output_dir'],
                        os.path.basename(os.path.dirname(video_path)),
                        f"{video_name}_segment_{i+1}.mp4"
                    )
                    os.makedirs(os.path.dirname(output_path), exist_ok=True)
                    
                    # 执行实际的视频切片
                    print(f"切割片段 {i+1} 到 {output_path}")
                    # 构建 ffmpeg 命令
                    ffmpeg_cmd = [
                        'ffmpeg',
                        '-i', video_path,
                        '-ss', str(start_time),
                        '-t', str(duration),
                        '-c:v', 'libx264',  # 明确指定使用 h264 编码
                        '-c:a', 'aac',      # 明确指定使用 aac 音频编码
                        '-f', 'mp4',        # 强制输出 mp4 格式
                        '-avoid_negative_ts', '1',
                        '-y',
                        output_path
                    ]
                    
                    # 执行命令
                    try:
                        result = subprocess.run(
                            ffmpeg_cmd,
                            capture_output=True,
                            text=True,
                            check=True
                        )
                        self.logger.debug(f"FFmpeg stdout: {result.stdout}")
                        if result.stderr:
                            self.logger.debug(f"FFmpeg stderr: {result.stderr}")
                            
                    except subprocess.CalledProcessError as e:
                        self.logger.error(f"FFmpeg 执行失败: {e.stderr}")
                        raise
                    
                    segment_info = {
                        "index": i + 1,
                        "start_time": start_time,
                        "end_time": end_time,
                        "duration": duration,
                        "start_frame": int(start_frame),
                        "end_frame": int(end_frame),
                        "output_path": output_path
                    }
                    print("append",f"片段 {i+1} 到 {output_path}")
                    slice_info["segments"].append(segment_info)
                    print(f"片段 {i+1} 处理完成")

            except Exception as e:
                self.logger.error(f"处理片段 {i+1} 时出错: {str(e)}", exc_info=True)
                continue

        print(f"视频分片完成，共生成 {len(slice_info['segments'])} 个片段")
        return slice_info
    
    def get_slice_count(self) -> int:
        """获取切片数量"""
        return len(self.slice_info.get("segments", []))
    
    def get_slice_paths(self) -> List[str]:
        """获取所有切片的路径"""
        return [segment["output_path"] for segment in self.slice_info.get("segments", [])]
    