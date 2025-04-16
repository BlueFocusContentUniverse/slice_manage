# ProPainter视频处理项目

## 项目介绍

本项目构建了一套完整的视频处理系统，包括视频分析、切片处理、标签管理和知识库存储功能。主要用于汽车视频的智能分析和处理。

## 项目结构

```
ProPainter/
├── config/                   # 配置文件
│   └── config.py             # 配置加载和管理类
├── processors/               # 视频处理相关模块
│   ├── video_analyzer.py     # 基于OpenAI的视频分析器
│   ├── video_analyzer_gemini.py # 基于Google Gemini的视频分析器
│   ├── video_material_cleaner.py # 视频素材清洗工具
│   ├── video_preprocessor.py # 视频预处理器
│   └── video_slicer.py       # 视频切片工具
├── services/                 # 服务组件
│   └── tagging_service.py    # 视频标签管理服务
├── storage/                  # 存储相关组件
│   ├── knowledge_base.py     # 知识库处理模块
│   └── minio_handler.py      # MinIO对象存储处理
├── main.py                   # 主程序入口
├── tagging.py                # 标签处理服务
├── test_front.py             # 基于Streamlit的前端界面，用于测试视频解析提示词
├── config.yaml               # 全局配置文件
└── requirements.txt          # 项目依赖
```

## 主要组件说明

### 1. 核心处理组件 (main.py)

`VideoProcessor` 类是整个系统的核心协调器，负责整合各个组件完成完整的视频处理流程：

- 初始化各功能组件(预处理器、切片器、分析器等)
- 协调完整的视频处理流程(预处理→切片→分析→存储)
- 支持单视频处理和批量处理
- 实现目录监控功能，自动处理新增视频

### 2. 视频处理组件 (processors/)

#### VideoPreprocessor
视频预处理组件，负责：
- 验证视频文件完整性
- 提取视频帧
- 准备各种输出路径
- 视频格式转换和调整

#### VideoSlicer
视频切片器，基于场景检测将长视频分割为多个短片段：
- 使用ContentDetector进行场景检测
- 支持阈值调整和最小片段长度设置
- 使用ffmpeg切割视频
- 返回片段信息和路径

#### VideoAnalyzer / VideoAnalyzerGemini
视频内容分析器，分别基于OpenAI和Google Gemini API：
- 抽取视频关键帧和音频
- 构建分析提示词
- 调用AI模型分析视频内容
- 生成结构化分析结果

#### VideoMaterialCleaner
视频素材清洗工具：
- 检测视频中的文字内容
- 筛选符合要求的视频素材
- 自动归类和存档不符合要求的素材

### 3. 存储组件 (storage/)

#### MinIOHandler
对象存储管理器：
- 处理视频和分析结果的上传和下载
- 生成对象访问URL
- 管理存储桶和前缀
- 处理重试和错误恢复

#### KnowledgeBaseHandler
知识库处理器：
- 创建和管理数据集和集合
- 添加和检索视频分析数据
- 支持标签管理和查询
- 实现异步操作和批处理

### 4. 服务组件 (services/)

#### TaggingService
视频标签服务：
- 目录名称到标准标签的映射
- 自动为视频集合添加标签
- 管理标签映射关系
- LLM辅助识别视频内容类型

## 使用方法

### 环境配置

1. 创建并激活conda环境：
```bash
conda create -n propainter python=3.8 -y
conda activate propainter
pip install -r requirements.txt
```

2. 配置config.yaml文件，包括：
   - API密钥(OpenAI、ZhipuAI等)
   - 知识库连接信息
   - MinIO对象存储配置
   - 视频处理路径配置

### 运行视频处理

运行主程序：
```bash
python main.py
```

此命令将启动视频处理程序，自动监控配置的输入目录，处理新增视频。

### 单独运行标签服务

```bash
python tagging.py
```

### 前端测试界面

```bash
streamlit run test_front.py
```

## 注意事项

1. 确保config.yaml中的API密钥和存储路径已正确配置
2. 视频处理需要大量计算资源，尤其是高分辨率视频
3. 为达到最佳性能，建议使用GPU加速

## 扩展功能

本项目可以通过以下方式扩展：

1. 添加新的视频分析器(实现相同接口)
2. 扩展标签管理功能
3. 对接不同的AI模型和知识库
4. 自定义视频处理流程

## 授权说明

代码和模型仅供非商业用途。

