# 知识库系统登录工具

这是一个独立的Python脚本，专门用于执行知识库系统的登录操作，可以验证登录凭据是否有效，并获取认证令牌。

## 功能特点

- 支持密码登录
- 支持团队切换
- 可以从命令行参数或配置文件读取登录信息
- 可以将令牌保存到文件
- 详细的日志输出

## 安装依赖

```bash
pip install requests
```

## 使用方法

### 方法一：使用命令行参数

```bash
python login_tool.py --url "https://ai.blue-converse.com" --username "your_username" --password "your_password" --team "your_team_id"
```

### 方法二：使用配置文件

1. 创建配置文件 `login_config.json`：

```json
{
    "base_url": "https://ai.blue-converse.com",
    "username": "your_username@example.com",
    "password": "your_password_hash",
    "team_id": "your_team_id"
}
```

2. 运行脚本：

```bash
python login_tool.py --config login_config.json
```

### 保存令牌到文件

```bash
python login_tool.py --config login_config.json --save-token token.txt
```

### 显示详细信息

```bash
python login_tool.py --config login_config.json --verbose
```

## 参数说明

- `--url`: API基础URL
- `--username`: 用户名
- `--password`: 密码
- `--team`: 团队ID (可选)
- `--config`: 配置文件路径
- `--save-token`: 保存令牌到文件
- `--verbose`: 显示详细信息

## 注意事项

- 命令行参数的优先级高于配置文件
- 密码应该是经过处理的哈希值，而不是明文密码
- 建议使用配置文件方式，避免在命令行中暴露敏感信息 