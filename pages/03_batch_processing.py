import streamlit as st
import os
import tempfile
import uuid
from datetime import datetime
import time
import pandas as pd
import yaml
import glob
import json  # 添加json导入用于文件持久化
import math
import subprocess
from tasks import process_video_task
from celery_app import app
from celery.result import AsyncResult
import logging

# 设置日志
logger = logging.getLogger(__name__)

# 定义历史记录文件路径
HISTORY_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'batch_history.json')

# 定义视频分片大小（200MB）
CHUNK_SIZE_MB = 200
CHUNK_SIZE_BYTES = CHUNK_SIZE_MB * 1024 * 1024

# 确保data目录存在
os.makedirs(os.path.dirname(HISTORY_FILE), exist_ok=True)

# 设置页面配置
st.set_page_config(
    page_title="批量处理 - 视频解析处理系统",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 调试：打印Broker URL (移动到 set_page_config 之后)
#st.write(f"批量处理页面 - Broker URL: {app.conf.broker_url}")

# 加载配置
def load_config():
    with open('config.yaml', 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)

# 创建临时目录函数
def ensure_temp_dir():
    temp_dir = os.path.join(tempfile.gettempdir(), "streamlit_batch_uploads")
    os.makedirs(temp_dir, exist_ok=True)
    return temp_dir

# 将上传的文件保存到临时目录
def save_uploaded_file(uploaded_file):
    temp_dir = ensure_temp_dir()
    file_path = os.path.join(temp_dir, uploaded_file.name)
    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return file_path

# 检查视频文件大小并决定是否需要分片
def check_file_size(file_path):
    file_size = os.path.getsize(file_path)
    return file_size > CHUNK_SIZE_BYTES, file_size

# 使用FFmpeg分割视频文件
def split_video_file(file_path, file_name):
    # 创建存放分片的目录
    chunks_dir = os.path.join(ensure_temp_dir(), f"chunks_{uuid.uuid4()}")
    os.makedirs(chunks_dir, exist_ok=True)
    
    # 获取视频时长（秒）
    duration_cmd = f'ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "{file_path}"'
    duration = float(subprocess.check_output(duration_cmd, shell=True).decode('utf-8').strip())
    
    # 计算文件大小（字节）
    file_size = os.path.getsize(file_path)
    
    # 计算每秒视频的平均大小
    bytes_per_second = file_size / duration
    
    # 计算每个分片的时长（秒）
    chunk_duration = CHUNK_SIZE_BYTES / bytes_per_second
    
    # 计算需要多少个分片
    num_chunks = math.ceil(duration / chunk_duration)
    
    chunk_files = []
    
    # 使用FFmpeg分割视频
    for i in range(num_chunks):
        start_time = i * chunk_duration
        # 最后一个分片可能不足chunk_duration
        if i == num_chunks - 1:
            chunk_file = os.path.join(chunks_dir, f"{os.path.splitext(file_name)[0]}_part{i+1}{os.path.splitext(file_name)[1]}")
            cmd = f'ffmpeg -y -i "{file_path}" -ss {start_time} -c copy "{chunk_file}"'
        else:
            chunk_file = os.path.join(chunks_dir, f"{os.path.splitext(file_name)[0]}_part{i+1}{os.path.splitext(file_name)[1]}")
            cmd = f'ffmpeg -y -i "{file_path}" -ss {start_time} -t {chunk_duration} -c copy "{chunk_file}"'
        
        # 执行分割命令
        subprocess.run(cmd, shell=True, check=True)
        chunk_files.append((f"{os.path.splitext(file_name)[0]}_part{i+1}{os.path.splitext(file_name)[1]}", chunk_file))
    
    return chunk_files

# 从文件加载历史记录
def load_task_history():
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            st.error(f"加载历史记录文件失败: {e}")
            return []
    return []

# 保存历史记录到文件
def save_task_history_to_file(history):
    try:
        with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
    except Exception as e:
        st.error(f"保存历史记录文件失败: {e}")

# 保存任务历史记录
def save_task_history(task_info):
    if 'batch_task_history' not in st.session_state:
        st.session_state.batch_task_history = load_task_history()
    
    # 检查任务是否已存在
    updated = False
    for i, task in enumerate(st.session_state.batch_task_history):
        if task['task_id'] == task_info['task_id']:
            # 更新现有任务
            st.session_state.batch_task_history[i] = task_info
            updated = True
            break
    
    # 添加新任务
    if not updated:
        st.session_state.batch_task_history.append(task_info)
    
    # 保存到文件
    save_task_history_to_file(st.session_state.batch_task_history)

# 获取任务状态
def get_task_status(task_id):
    """
    从Celery获取任务状态，这是状态的唯一可靠来源
    """
    try:
        result = AsyncResult(task_id)
        # 获取详细进度信息
        status = result.state
        return status, result.info if status == 'PROGRESS' else None
    except Exception as e:
        logger.error(f"获取任务状态失败: {e}")
        return "UNKNOWN", None

# 初始化session_state
if 'config' not in st.session_state:
    st.session_state.config = load_config()
if 'batch_task_history' not in st.session_state:
    st.session_state.batch_task_history = load_task_history()
if 'batch_processing' not in st.session_state:
    st.session_state.batch_processing = False
if 'batch_tasks' not in st.session_state:
    st.session_state.batch_tasks = []

# 侧边栏
st.sidebar.title("视频解析处理系统")
st.sidebar.info("批量处理页面，可以上传多个视频并批量处理")

# 主标题
st.title("批量处理")

# 两列布局
col1, col2 = st.columns([3, 1])

with col1:
    st.subheader("批量上传")
    
    with st.form("batch_upload_form"):
        # 知识库ID输入
        knowledge_base_id = st.text_input(
            "知识库ID (必填)", 
            value=st.session_state.config['KnowledgeBase']['datasetId'],
            placeholder="输入知识库ID", 
            key="batch_kb_id"
        )
        
        # 视频上传
        uploaded_files = st.file_uploader(
            "上传视频文件 (多选)", 
            type=["mp4", "avi", "mov"], 
            accept_multiple_files=True,
            key="batch_video_upload"
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
            key="batch_template_select"
        )
        
        default_dimensions = dimension_templates[selected_template]
        
        # 自定义解析维度
        custom_dimensions = st.text_area(
            "自定义视频解析维度", 
            value=default_dimensions,
            height=150,
            key="batch_custom_dims",
            help="每行一个解析角度，可以根据需要自定义。系统会自动替换其中的{车型}或{品牌}占位符。"
        )
        
        # 品牌车型输入
        brand_model = st.text_input(
            "品牌车型/产品", 
            value=st.session_state.config['GeminiService']['prompt'],
            placeholder="例如：理想L9、华为Mate60、小米14等", 
            key="batch_brand_model",
            help="将替换解析维度中的{车型}或{品牌}占位符"
        )
        
        # 并发处理数
        concurrency = st.slider(
            "并发处理数", 
            min_value=1, 
            max_value=7, 
            value=3, 
            help="同时处理的视频数量，最大值为7"
        )
        
        # 自动分片选项
        auto_split = st.checkbox(
            "自动分片处理大文件", 
            value=True,
            help=f"自动将超过{CHUNK_SIZE_MB}MB的视频分割成较小的片段进行处理"
        )
        
        # 任务提交按钮
        submit = st.form_submit_button("开始批量处理", disabled=st.session_state.batch_processing)
    
    # 如果提交了表单
    if submit:
        if not knowledge_base_id:
            st.error("请输入知识库ID")
        elif not uploaded_files:
            st.error("请上传至少一个视频文件")
        else:
            # 替换占位符
            if "{车型}" in custom_dimensions:
                custom_dimensions = custom_dimensions.replace("{车型}", brand_model)
            if "{品牌}" in custom_dimensions:
                custom_dimensions = custom_dimensions.replace("{品牌}", brand_model)
            
            # 更新配置
            st.session_state.config['KnowledgeBase']['datasetId'] = knowledge_base_id
            st.session_state.config['GeminiService']['prompt'] = brand_model
            
            # 准备批处理
            batch_tasks = []
            
            with st.spinner(f"准备处理 {len(uploaded_files)} 个视频文件..."):
                # 保存所有上传的文件并处理大文件分片
                all_files_to_process = []
                
                for uploaded_file in uploaded_files:
                    # 保存上传的文件
                    original_file_path = save_uploaded_file(uploaded_file)
                    
                    # 检查文件大小是否需要分片
                    need_split, file_size = check_file_size(original_file_path)
                    
                    if need_split and auto_split:
                        st.info(f"文件 '{uploaded_file.name}' 大小为 {file_size/1024/1024:.1f}MB，超过{CHUNK_SIZE_MB}MB，将自动分片处理")
                        
                        try:
                            # 分割视频
                            chunk_files = split_video_file(original_file_path, uploaded_file.name)
                            
                            # 添加所有分片到处理列表
                            for chunk_name, chunk_path in chunk_files:
                                all_files_to_process.append((chunk_name, chunk_path, True))
                                
                            st.success(f"已将 '{uploaded_file.name}' 分割为 {len(chunk_files)} 个片段")
                            
                        except Exception as e:
                            st.error(f"分割视频文件时出错: {e}")
                            st.warning(f"将尝试处理原始文件: {uploaded_file.name}")
                            all_files_to_process.append((uploaded_file.name, original_file_path, False))
                    else:
                        # 如果不需要分片或不自动分片，直接处理原始文件
                        all_files_to_process.append((uploaded_file.name, original_file_path, False))
                
                # 设置批处理标志
                st.session_state.batch_processing = True
                
                # 创建任务
                for i, (file_name, file_path, is_chunk) in enumerate(all_files_to_process):
                    # 生成唯一ID
                    processing_uuid = str(uuid.uuid4())
                    
                    # 创建任务信息
                    task_info = {
                        'file_name': file_name,
                        'file_path': file_path,
                        'knowledge_base_id': knowledge_base_id,
                        'custom_dimensions': custom_dimensions if custom_dimensions.strip() else None,
                        'processing_uuid': processing_uuid,
                        'status': 'PENDING',
                        'task_id': None,
                        'start_time': None,
                        'end_time': None,
                        'is_chunk': is_chunk  # 标记是否为分片
                    }
                    
                    # 添加到任务列表
                    batch_tasks.append(task_info)
                
                # 保存任务列表
                st.session_state.batch_tasks = batch_tasks
                
                st.success(f"成功准备 {len(batch_tasks)} 个任务，将以 {concurrency} 个并发执行")
                st.session_state.batch_concurrency = concurrency

# 任务列表和处理逻辑
if st.session_state.batch_processing:
    # 添加手动刷新按钮和任务状态指示器
    col_task_header1, col_task_header2 = st.columns([5, 1])
    with col_task_header1:
        st.subheader("任务列表")
    with col_task_header2:
        if st.button("刷新状态", key="refresh_status"):
            # 强制刷新页面
            st.rerun()
    
    # 更新所有任务状态（从Celery获取最新状态）
    for i, task in enumerate(st.session_state.batch_tasks):
        if task['task_id']:
            new_status, progress_info = get_task_status(task['task_id'])
            # 如果状态变化，更新当前状态
            if new_status != task['status']:
                # 记录之前的状态
                old_status = task['status']
                # 更新状态
                task['status'] = new_status
                
                # 记录进度信息
                if progress_info:
                    task['progress_info'] = progress_info
                
                # 如果任务完成(从PROGRESS/STARTED变为SUCCESS/FAILURE)，记录结束时间
                if new_status in ['SUCCESS', 'FAILURE'] and old_status not in ['SUCCESS', 'FAILURE']:
                    task['end_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                    # 更新历史记录
                    history_entry = {
                        'task_id': task['task_id'],
                        'video_name': task['file_name'],
                        'knowledge_base_id': task['knowledge_base_id'],
                        'brand_model': st.session_state.config['GeminiService']['prompt'],
                        'start_time': task['start_time'],
                        'end_time': task['end_time'],
                        'status': new_status,
                        'processing_uuid': task['processing_uuid'],
                        'is_chunk': task.get('is_chunk', False)  # 添加分片标记
                    }
                    
                    # 检查是否已存在
                    updated = False
                    for j, existing_task in enumerate(st.session_state.batch_task_history):
                        if existing_task.get('task_id') == task['task_id']:
                            st.session_state.batch_task_history[j] = history_entry
                            updated = True
                            break
                    
                    if not updated:
                        st.session_state.batch_task_history.append(history_entry)
                    
                    # 保存到文件
                    save_task_history_to_file(st.session_state.batch_task_history)
                
                # 更新session_state
                st.session_state.batch_tasks[i] = task
    
    # 任务状态计数
    pending_count = len([t for t in st.session_state.batch_tasks if t['status'] == 'PENDING'])
    running_count = len([t for t in st.session_state.batch_tasks if t['status'] in ['STARTED', 'PROGRESS']])
    completed_count = len([t for t in st.session_state.batch_tasks if t['status'] in ['SUCCESS', 'FAILURE']])
    total_count = len(st.session_state.batch_tasks)
    
    # 显示简洁的任务进度
    st.write(f"状态: 等待中 {pending_count} | 处理中 {running_count} | 已完成 {completed_count} | 总计 {total_count}")
    
    # 显示进度条
    progress = completed_count / total_count if total_count > 0 else 0
    st.progress(progress)
    
    # 准备任务数据
    task_data = []
    for i, task in enumerate(st.session_state.batch_tasks):
        status = task['status']
        
        # 美化状态显示
        status_display = status
        if status == 'SUCCESS':
            status_display = "✅ 成功"
        elif status == 'FAILURE':
            status_display = "❌ 失败"
        elif status == 'PENDING':
            status_display = "⏳ 等待中"
        elif status == 'STARTED':
            status_display = "🔄 处理中"
        elif status == 'PROGRESS':
            # 显示进度信息
            progress_info = task.get('progress_info', {})
            if isinstance(progress_info, dict) and 'current' in progress_info and 'total' in progress_info:
                progress_percent = int((progress_info['current'] / progress_info['total']) * 100)
                status_display = f"🔄 处理中 ({progress_percent}%)"
            else:
                status_display = "🔄 处理中"
        
        # 文件名显示（添加分片标记）
        file_name_display = task['file_name']
        if task.get('is_chunk', False):
            file_name_display = f"{file_name_display} (分片)"
        
        # 添加任务信息
        task_data.append({
            "序号": i + 1,
            "文件名": file_name_display,
            "状态": status_display,
            "开始时间": task['start_time'] or '-',
            "结束时间": task['end_time'] or '-',
            "详情": task.get('progress_info', {}).get('step', '') if status == 'PROGRESS' else ''
        })
    
    # 显示任务表格
    task_df = pd.DataFrame(task_data)
    st.dataframe(task_df, use_container_width=True)
    
    # 处理任务逻辑
    if st.session_state.batch_processing:
        # 获取当前正在处理的任务数
        current_running = len([t for t in st.session_state.batch_tasks 
                              if t['status'] in ['STARTED', 'PROGRESS'] and t['task_id'] is not None])
        
        # 获取最大并发数
        max_concurrency = st.session_state.batch_concurrency
        
        # 检查是否有等待处理的任务
        pending_tasks = [i for i, t in enumerate(st.session_state.batch_tasks) 
                        if t['status'] == 'PENDING' and t['task_id'] is None]
        
        # 如果有等待的任务且当前处理任务数小于最大并发数
        if pending_tasks and current_running < max_concurrency:
            # 计算可以启动的任务数
            to_start = min(len(pending_tasks), max_concurrency - current_running)
            
            for i in range(to_start):
                # 获取下一个待处理任务索引
                task_idx = pending_tasks[i]
                task = st.session_state.batch_tasks[task_idx]
                st.write("尝试发送任务...")
                try:
                    # 显式建立连接并发送任务
                    with app.connection_or_acquire() as connection:
                        # 发送任务
                        celery_task = app.send_task(
                            'tasks.process_video_task',
                            args=[
                                task['file_path'],
                                task['knowledge_base_id'],
                                task['custom_dimensions'],
                                task['file_name'],
                                task['processing_uuid']
                            ],
                            connection=connection  # 显式指定连接
                        )
                        
                        st.success(f"任务已发送! ID: {celery_task.id}")
                        
                except Exception as e:
                    st.error(f"发送任务时出错: {e}")
                    import traceback
                    st.error(traceback.format_exc())
                    # 标记任务失败
                    task['status'] = 'FAILURE'
                    task['end_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    continue  # 跳过此任务，继续处理下一个
                
                # 更新任务信息
                task['task_id'] = celery_task.id
                task['status'] = 'PENDING'  # 初始状态为PENDING，Celery会更新为STARTED
                task['start_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                # 创建任务历史记录
                history_entry = {
                    'task_id': celery_task.id,
                    'video_name': task['file_name'],
                    'knowledge_base_id': task['knowledge_base_id'],
                    'brand_model': st.session_state.config['GeminiService']['prompt'],
                    'start_time': task['start_time'],
                    'status': 'PENDING',
                    'processing_uuid': task['processing_uuid'],
                    'is_chunk': task.get('is_chunk', False)  # 添加分片标记
                }
                save_task_history(history_entry)
                
                st.session_state.batch_tasks[task_idx] = task
    
    # 检查是否所有任务都已结束
    if all(task['status'] not in ['STARTED', 'PROGRESS', 'PENDING'] for task in st.session_state.batch_tasks):
        # 如果之前在处理中，现在标记为已完成
        if st.session_state.batch_processing:
            st.session_state.batch_processing = False
            st.success("所有批处理任务已完成")
            st.rerun()

# 历史批处理记录
st.subheader("历史批处理记录")

# 添加清空历史按钮
col_history_header1, col_history_header2 = st.columns([5, 1])
with col_history_header2:
    if st.button("清空历史记录", key="clear_history"):
        if st.session_state.batch_task_history:
            st.session_state.batch_task_history = []
            save_task_history_to_file([])
            st.success("历史记录已清空")
            st.rerun()

if st.session_state.batch_task_history:
    # 准备历史数据
    history_data = []
    for task in st.session_state.batch_task_history:
        status = task.get('status', '')
        # 美化状态显示
        status_display = status
        if status == 'SUCCESS':
            status_display = "✅ 成功"
        elif status == 'FAILURE':
            status_display = "❌ 失败"
        elif status == 'PENDING':
            status_display = "⏳ 等待中"
        elif status == 'STARTED' or status == 'PROGRESS':
            status_display = "🔄 处理中"
        
        # 文件名显示（添加分片标记）
        video_name = task.get('video_name', '')
        if task.get('is_chunk', False):
            video_name = f"{video_name} (分片)"
            
        history_data.append({
            "视频名称": video_name,
            "开始时间": task.get('start_time', ''),
            "结束时间": task.get('end_time', '-'),
            "状态": status_display,
            "知识库ID": task.get('knowledge_base_id', ''),
            "任务ID": task.get('task_id', '')
        })
    
    # 显示历史记录表格
    history_df = pd.DataFrame(history_data)
    st.dataframe(history_df, use_container_width=True)
    
    # 显示历史记录统计
    st.info(f"共有 {len(history_data)} 条历史记录，"
           f"其中成功 {len([t for t in st.session_state.batch_task_history if t.get('status') == 'SUCCESS'])} 条，"
           f"失败 {len([t for t in st.session_state.batch_task_history if t.get('status') == 'FAILURE'])} 条")
else:
    st.info("暂无历史批处理记录")

# 自动刷新批处理状态
if st.session_state.batch_processing:
    time.sleep(1)  # 小延迟，避免过快刷新
    st.rerun()

# 底部信息
st.markdown("---")
st.caption("批量处理页面支持上传多个视频文件并同时处理，可以控制并发处理的数量。")
st.caption("处理过程中可以随时查看任务状态和进度。")
st.caption(f"支持自动分片处理超过{CHUNK_SIZE_MB}MB的大型视频文件。") 