import os
import tempfile
import uuid
import time
from datetime import datetime
from pathlib import Path
import streamlit as st
from tasks import process_video_task
from celery_app import app
from celery.result import AsyncResult
import yaml
import cv2
from io import BytesIO
import base64

# 设置页面配置
st.set_page_config(
    page_title="视频解析处理系统(单条)",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 调试：打印Broker URL (移动到 set_page_config 之后)
#st.write(f"主页面 - Broker URL: {app.conf.broker_url}")

# 自定义CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1E88E5;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #0D47A1;
        margin-bottom: 0.5rem;
    }
    .info-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #E3F2FD;
        margin-bottom: 1rem;
    }
    .success-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #E8F5E9;
        margin-bottom: 1rem;
    }
    .warning-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #FFF8E1;
        margin-bottom: 1rem;
    }
    .error-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #FFEBEE;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)

# 加载默认配置
def load_default_config():
    try:
        with open('config.yaml', 'r', encoding='utf-8') as file:
            return yaml.safe_load(file)
    except Exception as e:
        st.warning(f"无法加载配置文件: {e}，将使用默认配置")
        # 返回默认配置
        return {
            'KnowledgeBase': {'datasetId': ''},
            'GeminiService': {'prompt': ''},
            # 可以添加其他默认配置项
        }

# 创建临时目录函数
def ensure_temp_dir():
    temp_dir = os.path.join(tempfile.gettempdir(), "streamlit_video_uploads")
    os.makedirs(temp_dir, exist_ok=True)
    return temp_dir

# 将上传的文件保存到临时目录
def save_uploaded_file(uploaded_file):
    temp_dir = ensure_temp_dir()
    file_path = os.path.join(temp_dir, uploaded_file.name)
    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return file_path

# 生成视频预览
def generate_video_preview(video_path, max_width=400):
    cap = cv2.VideoCapture(video_path)
    
    # 获取视频信息
    frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    fps = cap.get(cv2.CAP_PROP_FPS)
    duration = frame_count / fps
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    
    # 调整预览图尺寸
    preview_width = min(width, max_width)
    preview_height = int(height * (preview_width / width))
    
    # 取视频中间帧作为预览图
    middle_frame_idx = frame_count // 2
    cap.set(cv2.CAP_PROP_POS_FRAMES, middle_frame_idx)
    ret, frame = cap.read()
    
    cap.release()
    
    if not ret:
        return None, None
    
    # 调整图像大小
    frame = cv2.resize(frame, (preview_width, preview_height))
    
    # 转换为base64
    _, buffer = cv2.imencode('.jpg', frame)
    img_str = base64.b64encode(buffer).decode()
    
    # 返回预览图和视频信息
    video_info = {
        "分辨率": f"{width}x{height}",
        "时长": f"{duration:.2f}秒",
        "帧数": frame_count,
        "帧率": f"{fps:.2f}fps"
    }
    
    return img_str, video_info

# 获取任务状态并显示进度
def display_task_progress(task_id):
    if not task_id:
        return
    
    # 获取任务结果
    task_result = AsyncResult(task_id)
    
    # 显示进度
    if task_result.state == 'PENDING':
        st.info("任务排队中，请稍候...")
        return None
    elif task_result.state == 'STARTED':
        st.info("任务已开始处理...")
        return None
    elif task_result.state == 'PROGRESS':
        meta = task_result.info or {}
        current = meta.get('current', 0)
        total = meta.get('total', 100)
        step = meta.get('step', '处理中...')
        progress = meta.get('progress', (current / total * 100)) if total else 0
        
        # 显示进度条
        st.progress(int(progress) / 100)
        st.info(f"处理中: {step} ({current}/{total})")
        return None
    elif task_result.state == 'SUCCESS':
        st.success("处理完成!")
        # 显示处理结果
        result = task_result.result
        if result:
            st.json(result)
            return result
        return {}
    elif task_result.state == 'FAILURE':
        st.error(f"处理失败: {task_result.traceback}")
        return None
    else:
        st.warning(f"任务状态: {task_result.state}")
        return None
    
# 保存任务历史记录
def save_task_history(task_info):
    if 'task_history' not in st.session_state:
        st.session_state.task_history = []
    
    # 检查任务是否已存在
    for i, task in enumerate(st.session_state.task_history):
        if task['task_id'] == task_info['task_id']:
            # 更新现有任务
            st.session_state.task_history[i] = task_info
            return
    
    # 添加新任务
    st.session_state.task_history.append(task_info)

# 主页面
def main():
    # 初始化session_state
    if 'task_id' not in st.session_state:
        st.session_state.task_id = None
    if 'user_config' not in st.session_state:
        # 加载默认配置作为初始值，但每个用户会单独保存
        st.session_state.user_config = load_default_config()
    if 'task_history' not in st.session_state:
        st.session_state.task_history = []
    
    # 侧边栏导航
    st.sidebar.title("视频解析处理系统（单条）")
    
    # 分隔线
    st.sidebar.markdown("---")
    
    # 状态信息
    with st.sidebar.expander("系统状态", expanded=False):
        st.markdown("**当前队列状态**")
        st.caption("待处理任务数：计算中...")  # 这里可以从Redis获取实际数据
        st.caption("处理中任务数：计算中...")
        st.caption("Worker 数量：7")  # 从配置获取
        
        if st.button("刷新状态", key="refresh_status"):
            st.experimental_rerun()
    
    # 主要内容区域
    st.markdown("<h1 class='main-header'>视频解析处理系统（单条）</h1>", unsafe_allow_html=True)
    
    # 创建两列布局
    col1, col2 = st.columns([3, 2])
    
    # 左列：输入参数区域
    with col1:
        st.markdown("<h2 class='sub-header'>输入参数</h2>", unsafe_allow_html=True)
        
        with st.form("upload_form"):
            # 知识库ID输入
            knowledge_base_id = st.text_input(
                "知识库ID (必填)", 
                value=st.session_state.user_config['KnowledgeBase'].get('datasetId', ''),
                placeholder="输入知识库ID", 
                key="kb_id"
            )
            
            # 视频上传
            uploaded_file = st.file_uploader(
                "上传视频文件", 
                type=["mp4", "avi", "mov"], 
                key="video_upload"
            )
            
            # 解析维度模板选择
            dimension_templates = {
                "汽车视频解析": """1. **人物动作**：开车、下车、上车、行走、开车门、敲车门、说话等等。
2. **车辆局部描述**：车头特写、车尾特写、侧面特写、轮胎特写、车灯特写、内饰特写等。
3. **车辆状态描述**：加速、刹车、转弯、漂移、越野、稳定巡航、疾驰而过等。
4. **相机视角**：俯视视角、仰视视角、平视视角、鸟瞰视角等。
5. **地点**：城市街道、高速公路、乡村道路、海滨公路等。
6. **时间**：清晨、上午、中午、下午、黄昏、夜晚。
7. **品牌车型**：{车型}""",
                
                "手机视频解析": """1. **人物动作**：打电话、发信息、浏览网页、拍照、录像等。
2. **手机局部描述**：屏幕特写、摄像头特写、边框特写、按键特写等。
3. **手机状态描述**：开机、关机、充电、运行应用、显示界面等。
4. **相机视角**：俯视视角、仰视视角、平视视角等。
5. **地点**：室内、办公室、户外、家庭等。
6. **时间**：白天、黑夜等。
7. **品牌型号**：{品牌}""",
                
                "无需解析维度": ""
            }
            
            selected_template = st.selectbox(
                "选择解析维度模板", 
                options=list(dimension_templates.keys()),
                key="template_select"
            )
            
            default_dimensions = dimension_templates[selected_template]
            
            # 自定义解析维度
            custom_dimensions = st.text_area(
                "自定义视频解析维度", 
                value=default_dimensions,
                height=250,
                key="custom_dims",
                help="每行一个解析角度，可以根据需要自定义。系统会自动替换其中的{车型}或{品牌}占位符。"
            )
            
            # 品牌车型输入
            brand_model = st.text_input(
                "品牌车型/产品", 
                value=st.session_state.user_config['GeminiService'].get('prompt', ''),
                placeholder="例如：理想L9、华为Mate60、小米14等", 
                key="brand_model",
                help="将替换解析维度中的{车型}或{品牌}占位符"
            )
            
            # 提交按钮
            submit = st.form_submit_button("开始处理", disabled=st.session_state.task_id is not None)
        
        # 如果点击提交按钮
        if submit:
            # 验证输入
            if not knowledge_base_id:
                st.error("请输入知识库ID")
            elif not uploaded_file:
                st.error("请上传视频文件")
            else:
                # 替换占位符
                if "{车型}" in custom_dimensions:
                    custom_dimensions = custom_dimensions.replace("{车型}", brand_model)
                if "{品牌}" in custom_dimensions:
                    custom_dimensions = custom_dimensions.replace("{品牌}", brand_model)
                
                # 保存上传的文件
                file_path = save_uploaded_file(uploaded_file)
                
                # 生成唯一ID
                processing_uuid = str(uuid.uuid4())
                
                # 更新用户会话中的配置（不再写入全局配置文件）
                st.session_state.user_config['KnowledgeBase']['datasetId'] = knowledge_base_id
                st.session_state.user_config['GeminiService']['prompt'] = brand_model
                
                # 准备用户配置参数
                user_config = {
                    'knowledge_base': {
                        'datasetId': knowledge_base_id
                    },
                    'gemini_service': {
                        'prompt': brand_model
                    }
                }
                
                # 启动任务并传递用户配置
                task = process_video_task.apply_async(args=[
                    file_path,
                    knowledge_base_id,
                    custom_dimensions if custom_dimensions.strip() else None,
                    uploaded_file.name,
                    processing_uuid,
                    user_config  # 传递用户特定的配置
                ])
                
                # 保存任务ID和信息
                st.session_state.task_id = task.id
                
                # 记录任务历史
                task_info = {
                    'task_id': task.id,
                    'video_name': uploaded_file.name,
                    'knowledge_base_id': knowledge_base_id,
                    'brand_model': brand_model,
                    'start_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'status': 'STARTED',
                    'processing_uuid': processing_uuid
                }
                save_task_history(task_info)
                
                st.success(f"任务已提交，ID: {task.id}. 请稍候...")
                
                # 刷新页面开始显示进度
                st.experimental_rerun()
    
    # 右列：进度显示和视频预览
    with col2:
        st.markdown("<h2 class='sub-header'>处理进度</h2>", unsafe_allow_html=True)
        
        # 显示上传视频的预览
        if uploaded_file and 'preview_generated' not in st.session_state:
            # 保存上传的文件
            file_path = save_uploaded_file(uploaded_file)
            
            # 生成预览
            preview_img, video_info = generate_video_preview(file_path)
            
            if preview_img:
                st.session_state.preview_img = preview_img
                st.session_state.video_info = video_info
                st.session_state.preview_generated = True
        
        # 显示预览图和视频信息
        if 'preview_img' in st.session_state and 'video_info' in st.session_state:
            st.markdown("### 视频预览")
            st.markdown(f"<img src='data:image/jpg;base64,{st.session_state.preview_img}' style='max-width:100%;'>", unsafe_allow_html=True)
            
            st.markdown("### 视频信息")
            for key, value in st.session_state.video_info.items():
                st.text(f"{key}: {value}")
        
        # 显示处理进度
        status_placeholder = st.empty()
        with status_placeholder:
            if st.session_state.task_id:
                result = display_task_progress(st.session_state.task_id)
                
                # 如果任务完成，更新任务历史
                if result and result.get('status') == 'success':
                    for task in st.session_state.task_history:
                        if task['task_id'] == st.session_state.task_id:
                            task['status'] = 'SUCCESS'
                            task['end_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            task['dataset_id'] = result.get('dataset_id')
                            task['total_slices'] = result.get('total_slices')
                            break
    
    # 任务历史
    st.markdown("<h2 class='sub-header'>任务历史</h2>", unsafe_allow_html=True)
    
    # 显示任务历史记录表格
    if st.session_state.task_history:
        task_data = []
        for task in st.session_state.task_history:
            task_data.append({
                "视频名称": task.get('video_name', ''),
                "开始时间": task.get('start_time', ''),
                "结束时间": task.get('end_time', '-'),
                "状态": task.get('status', ''),
                "知识库ID": task.get('knowledge_base_id', ''),
                "数据集ID": task.get('dataset_id', '-'),
                "切片数量": task.get('total_slices', '-'),
            })
        
        st.dataframe(task_data, use_container_width=True)
    else:
        st.info("暂无任务历史记录")
    
    # 底部信息
    st.markdown("---")
    st.caption("系统说明: 本系统用于自动解析视频内容，支持将视频切片并分析，结果保存到指定知识库。")
    st.caption("使用流程: 1. 输入知识库ID  2. 上传视频文件  3. 选择或自定义解析维度  4. 点击开始处理")

if __name__ == "__main__":
    main() 