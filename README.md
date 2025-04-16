
## Dependencies and Installation

1. Clone Repo

   ```bash
   git clone https://github.com/sczhou/ProPainter.git
   ```

2. Create Conda Environment and Install Dependencies

   ```bash
   # create new anaconda env
   conda create -n propainter python=3.8 -y
   conda activate propainter

   # install python dependencies
   pip3 install -r requirements.txt
   ```

   - CUDA >= 9.2
   - PyTorch >= 1.7.1
   - Torchvision >= 0.8.2
   - Other required packages in `requirements.txt`

## 项目结构

```
ProPainter/
├── assets/                   # 项目资源文件（图像、GIF等）
├── config/                   # 配置文件
├── configs/                  # ProPainter原始配置
├── core/                     # ProPainter核心代码
├── datasets/                 # 数据集处理
├── inputs/                   # 输入视频和掩码
├── processors/               # 视频处理器
├── propainter/               # ProPainter主要实现
├── RAFT/                     # RAFT光流模型
├── results/                  # 处理结果输出
├── scripts/                  # 实用脚本
├── services/                 # 服务实现
├── storage/                  # 存储相关代码
├── utils/                    # 工具函数
├── web-demos/                # Web演示
├── weights/                  # 模型权重
└── RTL-Inpainting/           # RTL-Inpainting模块
```

## 目录说明

| 目录 | 说明 |
|------|------|
| `config/` | 包含项目配置文件 |
| `processors/` | 视频处理相关模块，包括预处理、切片、分析等 |
| `storage/` | 包含存储相关代码，如MinIO对象存储和知识库处理 |
| `services/` | 提供标签服务相关实现 |
| `propainter/` | ProPainter主要实现，包括预处理、修复等 |
| `RTL-Inpainting/` | 用于掩码生成和图像修复的RTL-Inpainting模块 |

## 主要文件功能

### 根目录文件

| 文件 | 功能 |
|------|------|
| `main.py` | 项目主入口，实现视频处理流程 |
| `tagging.py` | 视频标签处理服务 |
| `upload.py` | 上传处理结果到MinIO存储 |
| `test_front.py` | 测试前端界面，基于Streamlit |
| `inference_propainter.py` | ProPainter的推理脚本 |
| `train.py` | ProPainter的训练脚本 |
| `config.yaml` | 配置文件（含API密钥、存储设置等） |

### config/ 目录

| 文件 | 功能 |
|------|------|
| `config.py` | 配置加载和管理类，从YAML文件加载配置 |

### processors/ 目录

| 文件 | 功能 |
|------|------|
| `video_preprocessor.py` | 视频预处理，包括帧提取、验证等 |
| `video_slicer.py` | 视频分割器，基于场景检测将视频分割成片段 |
| `video_analyzer.py` | 视频分析器，使用OpenAI API分析视频内容 |
| `video_analyzer_gemini.py` | 使用Google Gemini分析视频内容 |
| `video_material_cleaner.py` | 视频素材清洗，检测和处理有文字的帧 |

### storage/ 目录

| 文件 | 功能 |
|------|------|
| `knowledge_base.py` | 知识库处理，管理视频分析结果存储和检索 |
| `minio_handler.py` | MinIO对象存储处理，上传下载视频和分析结果 |

### services/ 目录

| 文件 | 功能 |
|------|------|
| `tagging_service.py` | 视频标签服务，自动为视频创建和管理标签 |

## 主要组件说明

### VideoProcessor 类 (main.py)

主要视频处理协调器，整合各个组件完成视频处理流程：
- 初始化各组件（preprocessor, slicer, analyzer等）
- 管理视频处理流程（预处理、切片、分析、存储）
- 实现批处理和目录监控功能

### 处理器组件

1. **VideoPreprocessor**: 视频预处理
   - 验证视频
   - 提取视频帧
   - 生成掩码
   - 移除水印

2. **VideoSlicer**: 视频切片
   - 使用场景检测切分视频
   - 支持阈值调整和最小片段长度设置
   - 输出片段信息和路径

3. **VideoAnalyzer/VideoAnalyzerGemini**: 视频内容分析
   - 抽取视频帧和音频
   - 调用AI模型分析视频内容
   - 生成结构化分析结果

4. **VideoMaterialCleaner**: 视频素材清洗
   - 检测视频中的文字内容
   - 自动筛选和清理不符合要求的素材

### 存储组件

1. **MinIOHandler**: 对象存储管理
   - 上传/下载视频文件
   - 生成访问URL
   - 确保存储桶和前缀管理

2. **KnowledgeBaseHandler**: 知识库处理
   - 创建和管理数据集
   - 添加和检索视频分析数据
   - 支持标签管理和查询

### 服务组件

**TaggingService**: 标签服务
   - 将目录名称映射为标准化标签
   - 自动为视频集合添加标签
   - 管理标签映射关系

