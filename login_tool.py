#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
知识库系统登录工具

这是一个独立的Python脚本，专门用于执行登录操作，
可以验证登录凭据是否有效，并获取认证令牌。
"""

import requests
import json
import logging
import argparse
import os
from typing import Dict, Optional, Tuple


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("知识库登录工具")


def login(base_url: str, username: str, password: str, team_id: Optional[str] = None) -> Tuple[bool, Dict, str]:
    """
    登录知识库系统
    
    Args:
        base_url: API基础URL
        username: 用户名
        password: 密码
        team_id: 团队ID (可选)
        
    Returns:
        成功标志, 响应数据, 错误信息
    """
    try:
        # 设置请求头
        headers = {
            'User-Agent': 'Apifox/1.0.0 (https://apifox.com)',
            'Content-Type': 'application/json',
            'Accept': '*/*'
        }
        
        # 创建会话
        session = requests.Session()
        session.headers.update(headers)
        
        # 第一步：密码登录
        login_url = f"{base_url}/api/support/user/account/loginByPassword"
        login_payload = {
            "username": username,
            "password": password
        }
        
        logger.info(f"正在尝试登录: {username}")
        response = session.post(login_url, json=login_payload)
        
        # 检查响应状态
        if response.status_code != 200:
            return False, {}, f"登录失败: HTTP {response.status_code}, 响应: {response.text}"
        
        # 解析响应数据
        data = response.json()
        token = data.get('data', {}).get('token')
        
        if not token:
            return False, {}, "登录成功但未找到令牌"
        
        logger.info("初始登录成功")
        
        # 如果提供了团队ID，则执行团队切换
        if team_id:
            team_switch_url = f"{base_url}/api/proApi/support/user/team/switch"
            team_payload = {
                "teamId": team_id
            }
            
            # 更新会话头部
            session.headers.update({'Cookie': f"token={token}"})
            
            logger.info(f"正在切换到团队: {team_id}")
            team_response = session.put(team_switch_url, json=team_payload)
            
            if team_response.status_code != 200:
                return False, data, f"团队切换失败: HTTP {team_response.status_code}, 响应: {team_response.text}"
            
            # 获取新令牌
            team_data = team_response.json()
            new_token = team_data.get('data', {}).get('token')
            
            if not new_token:
                return False, data, "团队切换成功但未找到新令牌"
            
            logger.info("团队切换成功")
            return True, team_data, ""
        
        return True, data, ""
        
    except Exception as e:
        return False, {}, f"登录过程中发生错误: {str(e)}"


def load_config(config_file: str) -> Dict:
    """
    从配置文件加载登录信息
    
    Args:
        config_file: 配置文件路径
        
    Returns:
        配置字典
    """
    try:
        if not os.path.exists(config_file):
            logger.error(f"配置文件不存在: {config_file}")
            return {}
            
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
            
        return config
    except Exception as e:
        logger.error(f"加载配置文件失败: {str(e)}")
        return {}


def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="知识库系统登录工具")
    parser.add_argument("--url", help="API基础URL")
    parser.add_argument("--username", help="用户名")
    parser.add_argument("--password", help="密码")
    parser.add_argument("--team", help="团队ID (可选)")
    parser.add_argument("--config", help="配置文件路径")
    parser.add_argument("--save-token", help="保存令牌到文件")
    parser.add_argument("--verbose", action="store_true", help="显示详细信息")
    
    args = parser.parse_args()
    
    # 设置日志级别
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # 初始化参数
    base_url = None
    username = None
    password = None
    team_id = None
    
    # 如果提供了配置文件，从配置文件加载
    if args.config:
        config = load_config(args.config)
        base_url = config.get('base_url')
        username = config.get('username')
        password = config.get('password')
        team_id = config.get('team_id')
    
    # 命令行参数优先级高于配置文件
    if args.url:
        base_url = args.url
    if args.username:
        username = args.username
    if args.password:
        password = args.password
    if args.team:
        team_id = args.team
    
    # 检查必要参数
    if not all([base_url, username, password]):
        logger.error("缺少必要参数: 需要提供 url, username 和 password")
        return 1
    
    # 执行登录
    success, data, error = login(base_url, username, password, team_id)
    
    if success:
        logger.info("登录成功!")
        token = data.get('data', {}).get('token', '')
        
        # 打印令牌
        print(f"\n令牌: {token}\n")
        
        # 保存令牌到文件
        if args.save_token and token:
            try:
                with open(args.save_token, 'w', encoding='utf-8') as f:
                    f.write(token)
                logger.info(f"令牌已保存到文件: {args.save_token}")
            except Exception as e:
                logger.error(f"保存令牌失败: {str(e)}")
        
        # 如果需要详细信息，打印完整响应
        if args.verbose:
            print("完整响应:")
            print(json.dumps(data, ensure_ascii=False, indent=2))
    else:
        logger.error(f"登录失败: {error}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main()) 