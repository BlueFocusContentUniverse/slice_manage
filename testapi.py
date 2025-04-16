from concurrent.futures import ThreadPoolExecutor
import requests
import time

def process_video(video_path):
    try:
        response = self.client.chat.completions.create(
            model=self.config['model_name'],
            messages=[{
                "role": "user",
                "content": [
                    {"type": "video_url", "video_url": {"url": video_path}},
                    {"type": "text", "text": prompt}
                ]
            }]
        )
        
        while not response.choices:
            time.sleep(1)
            
        return {
            "video_path": video_path,
            "analysis_result": response.choices[0].message.content,
            "token_usage": response.usage.total_tokens
        }
    except Exception as e:
        return f'Error processing {video_path}: {str(e)}'

# 测试不同并发数
video_paths = ['video1.mp4', 'video2.mp4', ...]  # 你的视频列表
max_workers = 3  # 建议从小数值开始测试

start_time = time.time()
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    results = list(executor.map(process_video, video_paths))

print(f'总耗时: {time.time() - start_time:.2f}秒')
print(f'成功数: {sum(1 for r in results if isinstance(r, dict))}')