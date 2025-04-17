# processors/video_preprocessor.py
import os
import subprocess
import logging
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Tuple

#from propainter.pre_process import preprocess_video

@dataclass
class ProcessResult:
    success: bool
    output_path: str = None
    message: str = None

class VideoPreprocessor:
    """视频预处理类，包含完整的处理流程"""
    
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.project_root = Path(__file__).parent.parent

    def process_video(self, video_path: str):
        """完整的视频处理流水线"""
        try:
            # 1. 验证视频
            if not self._validate_video(video_path):
                return False, "视频验证失败"

            video_name = Path(video_path).stem
            
            # 准备各种输出路径
            frames_path = self._get_frames_path(video_name)
            mask_path = self._get_mask_path(video_name)
            temp_output_path = self._get_temp_output_path(video_name)
            final_output_path = self._get_final_output_path(video_name)

            # 创建必要的目录
            self._ensure_directories()

            # 2. 提取视频帧
            frames_result = self._extract_frames(video_path, temp_output_path, frames_path)
            if not frames_result.success:
                return False, frames_result.message

            # 3. 生成掩码
            mask_result = self._generate_masks(frames_path, mask_path)
            if not mask_result.success:
                return False, mask_result.message

            # 4. 移除水印
            final_result = self._remove_watermark(
                temp_output_path, 
                mask_path, 
                final_output_path
            )
            
            # 5. 清理临时文件
            #self._cleanup(video_name)

            return final_result.output_path

        except Exception as e:
            error_msg = f"视频处理失败: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg

    def _validate_video(self, video_path: str) -> bool:
        """验证视频文件"""
        if not os.path.exists(video_path):
            self.logger.error(f"视频文件不存在: {video_path}")
            return False
        # 可以添加更多验证...
        return True

    def _extract_frames(self, video_path: str, temp_output_path: str,frames_path: str) -> ProcessResult:
        """提取视频帧"""
        try:
            os.makedirs(frames_path, exist_ok=True)
            # 这里实现帧提取逻辑...
            # 例如使用 ffmpeg 提取帧
            #preprocess_video(video_path,temp_output_path,frames_path)
            return ProcessResult(True, output_path=frames_path)
        except Exception as e:
            return ProcessResult(False, message=f"帧提取失败: {str(e)}")

    def _generate_masks(self, frames_path: str, mask_path: str) -> ProcessResult:
        """生成掩码"""
        try:
            os.makedirs(mask_path, exist_ok=True)
            # 运行 RTL-Inpainting
            subprocess.run(
                ['bash', 'propainter/RTL-Inpainting/run.sh', frames_path, mask_path],
                check=True
            )
            subprocess.run(['bash', '/home/jinpeng/ProPainter/propainter/RTL-Inpainting/cleanup.sh'])
            return ProcessResult(True, output_path=mask_path)
        except Exception as e:
            return ProcessResult(False, message=f"掩码生成失败: {str(e)}")

    def _remove_watermark(self, video_path: str, mask_path: str, 
                         output_path: str) -> ProcessResult:
        """使用 ProPainter 去除水印"""
        try:
            subprocess.run([
                'python',
                str(self.project_root / 'propainter/inference_propainter.py'),
                '--video', video_path,
                '--mask', mask_path,
                '--output', output_path
            ], check=True)
            return ProcessResult(True, output_path=output_path)
        except Exception as e:
            return ProcessResult(False, message=f"水印去除失败: {str(e)}")

    def _cleanup(self, video_name: str) -> None:
        """清理临时文件"""
        try:
            # 实现清理逻辑...
            pass
        except Exception as e:
            self.logger.warning(f"清理临时文件失败: {str(e)}")

    def _get_frames_path(self, video_name: str) -> str:
        """获取帧输出路径"""
        return os.path.join(self.config.video_config['frames_dir'], video_name)

    def _get_mask_path(self, video_name: str) -> str:
        """获取掩码输出路径"""
        return os.path.join(self.config.video_config['mask_dir'], video_name)

    def _get_temp_output_path(self, video_name: str) -> str:
        """获取临时输出路径"""
        return os.path.join(
            self.config.video_config['output_dir'], 
            f"{video_name}_preprocessed.mp4"
        )

    def _get_final_output_path(self, video_name: str) -> str:
        """获取最终输出路径"""
        return os.path.join(
            self.config.video_config['output_dir'], 
            f"{video_name}_processed"
        )

    def _ensure_directories(self) -> None:
        """确保所有必要的目录存在"""
        os.makedirs(self.config.video_config['frames_dir'], exist_ok=True)
        os.makedirs(self.config.video_config['mask_dir'], exist_ok=True)
        os.makedirs(self.config.video_config['output_dir'], exist_ok=True)

    def process_batch(self, input_dir: str) -> list:
        """批量处理视频"""
        results = []
        video_files = [
            f for f in os.listdir(input_dir) 
            if f.endswith(('.mp4', '.avi', '.mov'))
        ]

        for video_file in video_files:
            video_path = os.path.join(input_dir, video_file)
            success, result = self.process_video(video_path)
            results.append({
                'video': video_file,
                'success': success,
                'result': result
            })

        return results