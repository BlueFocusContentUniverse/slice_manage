import streamlit as st
import os
from pathlib import Path
from processors.video_analyzer_gemini import VideoAnalyzerGemini
import tempfile
from dataclasses import dataclass

# 设置页面配置
st.set_page_config(
    page_title="视频分析工具",
    page_icon="🎥",
    layout="wide"
)

@dataclass
class Config:
    """配置类"""
    api_config: dict
    analyse_config: dict
    slice_config: dict

# 初始化 VideoAnalyzerGemini 配置
@st.cache_resource
def get_analyzer():
    config = Config(
        api_config={
            'openai_api_key': 'sk-u1lDoRu9zddCGt41Ws8v3btypD8e7mDnuek41du7r1joHm5f'
        },
        analyse_config={
            'api_base_url': 'https://nwxbqdio.cloud.sealos.io/v1/',
            'model_name': 'gemini-1.5-pro',
            'total_frames': 100,  # 总抽帧数
            'output_dir': 'output'  # 输出目录
        },
        slice_config={
            'output_dir': 'output/slices',
            'temp_dir': 'temp',
            'min_segment_length': 0.1,  # 最小片段长度（秒）
            'detection_threshold': 27.0  # 场景检测阈值
        }
    )
    return VideoAnalyzerGemini(config)

def main():
    st.title("🎥 Gemini视频分析")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("上传和设置")
        uploaded_file = st.file_uploader("选择要分析的视频文件", type=['mp4', 'avi', 'mov'])
        
        # 添加视频处理参数配置
        st.markdown("### 分析参数设置")
        total_frames = st.number_input("总抽帧数", min_value=1, value=200, help="将从视频中提取的总帧数")
        
        # 更新提示词模板
        default_prompt = (
            "### 视频内容分析提示词\n"
            "请分析这段视频内容，重点关注：\n"
            "1. 人物动作和场景\n"
            "2. 车辆外观和内饰细节\n"
            "3. 相机视角和拍摄手法\n"
            "4. 音频内容和氛围\n"
            "5. 品牌特点和产品亮点"
        )
        prompt = st.text_area("分析提示词", value=default_prompt, height=200)
        
        analyze_button = st.button("开始分析", type="primary")
        
        if uploaded_file:
            st.video(uploaded_file)

            if analyze_button:

                try:


                    with tempfile.NamedTemporaryFile(delete=False, suffix='.mp4') as tmp_file:
                        tmp_file.write(uploaded_file.read())
                        video_path = tmp_file.name
                    
                    analyzer = get_analyzer()
                    
                    # 添加处理进度展示
                    progress_container = st.container()
                    with progress_container:
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        # 处理阶段展示
                        status_text.text("正在切分视频...")
                        progress_bar.progress(20)
                        
                        result = analyzer.analyze_video(video_path, prompt)
                        
                        if result.success:
                            st.session_state['analysis_result'] = result.analysis_info
                            progress_bar.progress(100)
                            status_text.text("分析完成！")
                            st.success('视频分析成功完成！')
                        else:
                            st.error(f'分析失败: {result.message}')
                    
                    os.unlink(video_path)
                    
                except Exception as e:
                    st.error(f'发生错误: {str(e)}')
    
    with col2:
        st.subheader("分析结果")
        
        # 显示视频预览
        
        
        # 优化结果显示
        if 'analysis_result' in st.session_state:
            result = st.session_state['analysis_result']
            print(result)
            # 分析结果标签页
            tabs = st.tabs(["分析结果"])
            
            with tabs[0]:
                # 创建标题栏的两列布局
                title_col, button_col = st.columns([6, 4])
                with title_col:
                    st.markdown("### 视频内容分析")
                with button_col:
                    if st.button("📋 复制所有分析结果", key="copy_all_results"):
                        # 收集所有片段的分析结果
                        all_analysis = []
                        for idx, segment in enumerate(result.get('segments', []), 1):
                            if 'analysis' in segment and len(segment['analysis']) > 0:
                                all_analysis.append(f"\n### 片段 {idx}")
                                analysis_list = sorted(segment['analysis'], key=lambda x: x['batch_idx'])
                                for batch in analysis_list:
                                    all_analysis.append(batch['segment_analysis'])
                        
                        # 显示完整的分析文本
                        full_analysis = "\n\n".join(all_analysis)
                        st.code(full_analysis)
                        st.toast("已复制到剪贴板！")
                if 'segments' in result:
                    # 遍历所有片段
                    for idx, segment in enumerate(result['segments'], 1):
                        if 'analysis' in segment and len(segment['analysis']) > 0:
                            st.markdown(f"#### 片段 {idx}")
                            
                            # 创建左右两栏
                            left_col, right_col = st.columns([6, 4])  # 左右比例为6:4
                            
                            with left_col:
                            # 分析结果
                                analysis_list = sorted(segment['analysis'], key=lambda x: x['batch_idx'])
                                for batch in analysis_list:
                                    with st.expander(f"分析结果{idx}.{batch['batch_idx'] + 1}", expanded=True):
                                        st.markdown(batch['segment_analysis'])
                                        st.caption(f"Token使用量：{batch['token_usage']}")
                                
                                st.caption(f"Token使用量：{segment.get('token_usage', 0)}")
                            with right_col:
                                # 显示对应的视频片段
                                video_path = segment.get('video_path')  # 直接从 segment 中获取 video_path
                                if video_path and os.path.exists(video_path):
                                    st.video(video_path)
                                else:
                                    st.warning("视频片段不可用")
                                    st.write(f"Debug - 视频路径: {video_path}") 
                            
                            st.markdown("---")  # 添加分隔线
                    st.caption(f"Token使用量：{result['total_token_usage']}")

if __name__ == "__main__":
    main()