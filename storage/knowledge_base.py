# handlers/knowledge_base_handler.py
import aiohttp
import logging
from typing import Dict, Optional
import json
import requests
from datetime import datetime
from typing import List, Dict, Any
import math
import asyncio
import os
from openai import OpenAI
class KnowledgeBaseHandler:
    """知识库处理器"""
    
    def __init__(self, config):
        self.config = config.knowledge_base_config
        self.base_url = self.config['base_url']
        self.logger = logging.getLogger(__name__)
        self.session = requests.Session()
        self._headers = {
            'User-Agent': 'Apifox/1.0.0 (https://apifox.com)',
            'Content-Type': 'application/json',
            'Accept': '*/*'
        }

    async def __aenter__(self):
        """异步上下文管理器入口"""
        self.session = aiohttp.ClientSession(headers=self._headers)
        # 登录并更新 session 的 headers
        await self._login()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        if self.session:
            await self.session.close()

    async def _login(self):
        """登录知识库系统并更新 session headers"""
        try:
            # 使用同步方式登录获取token
            sync_session = self._login_sync()
            if not sync_session:
                raise Exception("同步登录失败")
            
            # 从同步session获取token并更新异步session的headers
            token = sync_session.headers.get('Cookie', '').replace('token=', '')
            if not token:
                raise ValueError("登录后未找到有效token")
            
            # 更新异步session的headers
            self.session.headers.update({'Cookie': f"token={token}"})
            self.logger.info("登录成功")
            
        except Exception as e:
            self.logger.error(f"登录失败: {str(e)}")
            raise
        
    def _login_sync(self):
        """同步方式登录并获取session"""
        retry_count = 0 
        max_retries = 2 
        login_url = f"{self.base_url}/api/support/user/account/loginByPassword"
        team_switch_url = f"{self.base_url}/api/proApi/support/user/team/switch"
        
        login_payload = {
            "username": self.config['username'],
            "password": self.config['password']
        }

        team_payload = {
            "teamId": self.config['teamId']
        }
        
        sync_session = requests.Session()
        sync_session.headers.update(self._headers)

        while retry_count <= max_retries:
            try:
                # 第一步：密码登录
                response = sync_session.post(login_url, json=login_payload)
                if response.status_code != 200:
                    self.logger.warning(f"登录失败: {response.status_code}, 正在重试 ({retry_count + 1}/{max_retries + 1})")
                    retry_count += 1
                    continue
                
                # 获取并验证初始 token
                data = response.json()
                token = data.get('data', {}).get('token')
                if not token:
                    self.logger.warning(f"未找到 token, 正在重试 ({retry_count + 1}/{max_retries + 1})")
                    retry_count += 1
                    continue
                
                # 更新 session headers
                sync_session.headers.update({'Cookie': f"token={token}"})
                self.logger.debug(f"初始登录成功, token: {token}")
                
                # 第二步：team switch
                team_response = sync_session.put(team_switch_url, json=team_payload)
                if team_response.status_code != 200:
                    self.logger.warning(f"团队切换失败: {team_response.status_code}, 正在重试 ({retry_count + 1}/{max_retries + 1})")
                    retry_count += 1
                    continue
                
                # 获取并验证新 token
                team_data = team_response.json()
                new_token = team_data.get('data', {}).get('token')
                if not new_token:
                    self.logger.warning(f"团队切换未返回 token, 正在重试 ({retry_count + 1}/{max_retries + 1})")
                    retry_count += 1
                    continue
                
                # 使用新 token 更新 session headers
                sync_session.headers.update({'Cookie': f"token={new_token}"})
                self.logger.info(f"登录成功 (尝试 {retry_count + 1}/{max_retries + 1})")
                return sync_session
                
            except Exception as e:
                self.logger.warning(f"登录过程出错: {e}, 正在重试 ({retry_count + 1}/{max_retries + 1})")
                retry_count += 1

        self.logger.error("所有登录尝试均失败")
        return None

    async def create_dataset(self, video_name: str) -> str:
        """
        创建新的数据集
        
        Args:
            video_name: 视频名称
            
        Returns:
            数据集 ID
        """
        try:
            dataset_name = f"video_analysis_{video_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            payload = {
                #"parentId": " ",
                "datasetId": self.config['datasetId'],
                "name": dataset_name,
                "type": "virtual"
            }
            
            # 使用同步请求但保持异步接口
            sync_session = requests.Session()
            sync_session.headers.update(self._headers)
            
            # 如果有token，确保添加到请求头中
            if hasattr(self, 'session') and 'Cookie' in self.session.headers:
                sync_session.headers.update({'Cookie': self.session.headers['Cookie']})
            
            response = sync_session.post(
                f"{self.base_url}/api/core/dataset/collection/create",
                json=payload
            )
            
            if response.status_code == 200:
                data = response.json()
                print(f"完整的 API 响应: {data}")
                collection_id = data.get('data')
                if not collection_id:
                    raise ValueError("创建数据集响应中未找到 collection_id")
                return collection_id
            else:
                raise Exception(f"创建数据集失败: HTTP {response.status_code}")
                
        except Exception as e:
            self.logger.error(f"创建数据集失败: {str(e)}")
            raise

    async def create_data(self, collection_id: str, question: str, answer: str, parent_id:Optional[str] = None):
        """
        创建数据
        
        Args:
            collection_id: 数据集 ID
            question: 问题
            answer: 答案
        """
        try:
            payload = {
                "collectionId": collection_id,
                #"parentId": " ",
                "trainingMode": "chunk",
                "data": [
                    {
                        "q": question,
                        "a": answer,
                        "indexes": []
                    }
                ]
            }
            
            # 使用同步请求但保持异步接口
            sync_session = requests.Session()
            sync_session.headers.update(self._headers)
            
            # 如果有token，确保添加到请求头中
            if hasattr(self, 'session') and 'Cookie' in self.session.headers:
                sync_session.headers.update({'Cookie': self.session.headers['Cookie']})
            
            response = sync_session.post(
                f"{self.base_url}/api/core/dataset/data/pushData",
                json=payload
            )
            
            if response.status_code == 200:
                print(f"成功创建数据 - Q: {question}")
                response_text = response.text
                self.logger.debug(f"Response: {response_text}")
            else:
                response_text = response.text
                raise Exception(f"创建数据失败: HTTP {response.status_code}, 详细信息: {response_text}")
                
        except Exception as e:
            self.logger.error(f"创建数据失败: {str(e)}")
            raise

    async def add_tags_to_collections(self, collection_ids: list, dataset_id: str, tag_id: str) -> Dict:
        """
        为指定的数据集集合添加标签
        
        Args:
            collection_ids: 集合ID列表
            dataset_id: 数据集ID
            tag_id: 标签ID
            
        Returns:
            API 响应数据
        """
        try:
            payload = {
                "collectionIds": collection_ids,
                "datasetId": dataset_id,
                "originCollectionIds": [],
                "tag": tag_id
            }
            
            # 使用同步请求但保持异步接口
            sync_session = requests.Session()
            sync_session.headers.update(self._headers)
            
            # 如果有token，确保添加到请求头中
            if hasattr(self, 'session') and 'Cookie' in self.session.headers:
                sync_session.headers.update({'Cookie': self.session.headers['Cookie']})
            
            response = sync_session.post(
                f"{self.base_url}/api/proApi/core/dataset/tag/addToCollections",
                json=payload
            )
            
            if response.status_code == 200:
                data = response.json()
                self.logger.info(f"成功为集合添加标签")
                return data
            else:
                response_text = response.text
                raise Exception(f"添加标签失败: HTTP {response.status_code}, 详细信息: {response_text}")
                
        except Exception as e:
            self.logger.error(f"添加标签失败: {str(e)}")
            raise

    async def get_all_collections(self, dataset_id: str, page_size: int = 20) -> List[Dict[str, Any]]:
        """
        获取指定数据集下的所有集合数据
        
        Args:
            dataset_id: 数据集ID
            page_size: 每页数据量，默认20
            
        Returns:
            所有集合数据的列表
        """
        try:
            # 首先获取第一页数据以获取总数
            first_page_data = await self._get_collections_page(
                dataset_id=dataset_id,
                page_num=1,
                page_size=page_size
            )
            
            total_items = first_page_data['data']['total']
            total_pages = math.ceil(total_items / page_size)
            
            print(f"总数据量: {total_items}, 总页数: {total_pages}")
            
            # 存储所有数据
            all_collections = first_page_data['data']['data']
            
            # 获取剩余页的数据
            if total_pages > 1:
                tasks = []
                for page in range(2, total_pages + 1):
                    tasks.append(self._get_collections_page(
                        dataset_id=dataset_id,
                        page_num=page,
                        page_size=page_size
                    ))
                
                # 并发获取所有页的数据
                results = await asyncio.gather(*tasks)
                
                # 合并数据
                for result in results:
                    all_collections.extend(result['data']['data'])
            
            self.logger.info(f"成功获取所有集合数据，共 {len(all_collections)} 条")
            return all_collections
            
        except Exception as e:
            self.logger.error(f"获取集合数据失败: {str(e)}")
            raise

    async def _get_collections_page(self, dataset_id: str, page_num: int, page_size: int) -> Dict[str, Any]:
        """
        获取指定页码的集合数据
        
        Args:
            dataset_id: 数据集ID
            page_num: 页码
            page_size: 每页数据量
            
        Returns:
            当前页的数据
        """
        try:
            payload = {
                "datasetId": dataset_id,
                "filterTags": [],
                "pageNum": page_num,
                "pageSize": page_size,
                "parentId": "673722ec6d1f09e4d217b915",
                "searchText": ""
            }
            
            response = await self.session.post(
                f"{self.base_url}/api/core/dataset/collection/list",
                json=payload
            )
            
            if response.status == 200:
                data = await response.json()
                return data
            else:
                response_text = await response.text()
                raise Exception(f"获取集合数据失败: HTTP {response.status}, 详细信息: {response_text}")
                
        except Exception as e:
            self.logger.error(f"获取第 {page_num} 页数据失败: {str(e)}")
            raise

    async def process_collections_questions(self, collections: List[Dict[str, Any]], max_questions: int = 15) -> str:
        """
        处理所有集合的问题并获取LLM回答
        
        Args:
            collections: 集合数据列表
            max_questions: 每个集合最大处理的问题数量，默认10
            
        Returns:
            LLM的单个综合回答
        """
        try:
            all_questions = []
            
            # 创建所有集合的任务
            tasks = []
            for collection in collections:
                collection_id = collection['_id']
                tasks.append(self._process_single_collection(collection_id, max_questions))
            
            # 并发执行所有任务
            results = await asyncio.gather(*tasks)
            
            # 合并所有问题
            for questions in results:
                if questions:  # 确保有问题数据
                    all_questions.extend(questions)
            
            # 如果有问题，则调用LLM处理
            if all_questions:
                answer = await self._process_questions_with_llm(all_questions)
                self.logger.info("成功处理所有问题并获得LLM回答")
                return answer
            else:
                return "没有找到任何q需要处理"
            
        except Exception as e:
            self.logger.error(f"处理集合问题失败: {str(e)}")
            raise

    async def process_filtered_collections_questions(self, collections: List[Dict[str, Any]], max_questions: int = 15) -> str:
        """
        处理符合条件的集合的问题并获取LLM回答
        
        Args:
            collections: 集合数据列表
            max_questions: 每个集合最大处理的问题数量，默认15
            
        Returns:
            LLM的单个综合回答
        """
        try:
            all_questions = []
            target_date = datetime.fromisoformat("2025-02-12T07:30:22.692Z".replace("Z", "+00:00"))
            
            # 筛选符合条件的集合
            filtered_collections = []
            for collection in collections:
                collection_id = collection['_id']
                collection_type = collection.get('type', '')
                update_time_str = collection.get('updateTime', '')
                
                if 'tags' in collection:
                    self.logger.debug(f"跳过集合 {collection_id}: 已存在 tags")
                    continue
                
                # 检查类型是否为 virtual 或 file
                if collection_type not in ['virtual', 'file']:
                    self.logger.debug(f"跳过集合 {collection_id}: 类型 {collection_type} 不符合要求")
                    continue
                
                try:
                    # 将时间字符串转换为 datetime 对象
                    update_time = datetime.fromisoformat(update_time_str.replace("Z", "+00:00"))
                    
                    # 检查更新时间是否早于目标时间
                    if update_time > target_date:
                        self.logger.debug(f"跳过集合 {collection_id}: 更新时间 {update_time_str} 晚于目标时间")
                        continue
                        
                    filtered_collections.append(collection)
                    
                except (ValueError, TypeError) as e:
                    self.logger.error(f"处理集合 {collection_id} 的时间格式时出错: {str(e)}")
                    continue
            
            # 创建符合条件的集合的任务
            tasks = []
            for collection in filtered_collections:
                collection_id = collection['_id']
                tasks.append(self._process_single_collection(collection_id, max_questions))
            
            # 并发执行所有任务
            results = await asyncio.gather(*tasks)
            
            # 合并所有问题
            for questions in results:
                if questions:  # 确保有问题数据
                    all_questions.extend(questions)
            
            # 如果有问题，则调用LLM处理
            if all_questions:
                answer = await self._process_questions_with_llm(all_questions)
                self.logger.info("成功处理所有问题并获得LLM回答")
                return answer
            else:
                return "没有找到任何需要处理的问题"
            
        except Exception as e:
            self.logger.error(f"处理集合问题失败: {str(e)}")
            raise
    
    async def _process_single_collection(self, collection_id: str, max_questions: int) -> List[str]:
        """
        获取单个集合的问题数据
        
        Args:
            collection_id: 集合ID
            max_questions: 最大问题数量
            
        Returns:
            问题列表
        """
        try:
            payload = {
                "collectionId": collection_id,
                "offset": 0,
                "pageSize": max_questions,
                "searchText": ""
            }
            
            response = await self.session.post(
                f"{self.base_url}/api/core/dataset/data/list",
                json=payload
            )
            
            if response.status == 200:
                data = await response.json()
                questions = []
                items = data.get('data', {}).get('list', [])
                
                # 限制问题数量
                for item in items[:max_questions]:
                    if 'q' in item:
                        questions.append(item['q'])
                
                self.logger.debug(f"从集合 {collection_id} 获取到 {len(questions)} 个问题")
                return questions
            else:
                response_text = await response.text()
                raise Exception(f"获取集合数据失败: HTTP {response.status}, 详细信息: {response_text}")
                
        except Exception as e:
            self.logger.error(f"处理集合 {collection_id} 失败: {str(e)}")
            return []
        
    async def _process_questions_with_llm(self, questions: List[str]) -> str:
        """
        使用LLM处理问题列表，将所有问题作为一个整体输入
        
        Args:
            questions: 问题列表
            
        Returns:
            LLM的单个回答
        """
        try:
            # 读取 tag_mappings.json 文件
            with open('tag_mappings.json', 'r', encoding='utf-8') as f:
                tag_mappings = json.load(f)
            
            # 提取所有唯一的 tag_name
            tag_names = sorted(set(item['tag_name'] for item in tag_mappings.values()))
            
            # 将标签名称格式化为列表字符串
            available_tags = "\n".join([f"- {tag}" for tag in tag_names])
            
            # 将所有问题组合成一个文本
            combined_questions = "\n".join([f"- {q}" for q in questions])
            
            prompt = f"""以下是一些解析结果：
{combined_questions}

请从以下车型列表中选择一个最匹配的车型名称：
{available_tags}

要求：
1. 只能从上述列表中选择一个车型
2. 直接输出车型名称，不要有任何多余文字
3. 如果找不到完全匹配的，选择最接近的一个
4. 注意，如果品牌车型中只有品牌没有具体型号，如宝马全车型，宝马智能座舱，则直接输出宝马"""
            
            # 调用LLM处理
            answer = await self._call_llm(prompt)
            return answer.strip()
            
        except Exception as e:
            self.logger.error(f"LLM处理失败: {str(e)}")
            raise

    async def _call_llm(self, prompt: str) -> str:
        """
        调用LLM处理完整的prompt
        
        Args:
            prompt: 完整的提示文本
            
        Returns:
            LLM的回答
        """
        # 这里需要实现实际的LLM调用逻辑
        # 示例实现：
        client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'),base_url="https://nwxbqdio.cloud.sealos.io/v1/")
        response = client.chat.completions.create(
            model="anthropic.claude-3-5-sonnet-20241022-v2:0",
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content
    
    async def delete_empty_collections(self, collections: List[Dict[str, Any]]) -> List[str]:
        """
        删除数据量为0的集合
        
        Args:
            collections: 集合数据列表
            
        Returns:
            已删除的集合ID列表
        """
        try:
            deleted_collections = []
            target_date = datetime.fromisoformat("2025-02-12T07:30:22.692Z".replace("Z", "+00:00"))
            
            # 筛选符合条件的集合
            for collection in collections:
                collection_id = collection['_id']
                collection_type = collection.get('type', '')
                update_time_str = collection.get('updateTime', '')
                data_amount = collection.get('dataAmount', -1)  # 如果不存在则返回-1
                
                # 检查是否存在 tags 字段
                if 'tags' in collection:
                    self.logger.debug(f"跳过集合 {collection_id}: 已存在 tags")
                    continue
                
                # 检查类型是否为 virtual 或 file
                if collection_type not in ['virtual', 'file']:
                    self.logger.debug(f"跳过集合 {collection_id}: 类型 {collection_type} 不符合要求")
                    continue
                
                try:
                    # 将时间字符串转换为 datetime 对象
                    update_time = datetime.fromisoformat(update_time_str.replace("Z", "+00:00"))
                    
                    
                    
                    # 检查数据量是否为0
                    if data_amount != 0:
                        self.logger.debug(f"跳过集合 {collection_id}: 数据量不为0 (dataAmount: {data_amount})")
                        continue
                        
                    # 删除集合
                    try:
                        response = await self.session.delete(
                            f"{self.base_url}/api/core/dataset/collection/delete",
                            params={"id": collection_id}
                        )
                        
                        if response.status == 200:
                            self.logger.info(f"成功删除集合: {collection_id}")
                            deleted_collections.append(collection_id)
                        else:
                            response_text = await response.text()
                            self.logger.error(f"删除集合 {collection_id} 失败: HTTP {response.status}, 详细信息: {response_text}")
                            
                    except Exception as e:
                        self.logger.error(f"删除集合 {collection_id} 时发生错误: {str(e)}")
                        continue
                    
                except (ValueError, TypeError) as e:
                    self.logger.error(f"处理集合 {collection_id} 的时间格式时出错: {str(e)}")
                    continue
            
            self.logger.info(f"删除操作完成，共删除 {len(deleted_collections)} 个集合")
            return deleted_collections
            
        except Exception as e:
            self.logger.error(f"删除空集合操作失败: {str(e)}")
            raise


    async def delete_nonexist_collections(self, collections: List[Dict[str, Any]]) -> List[str]:
        """
        删除视频资源不存在的集合
        
        Args:
            collections: 集合数据列表
            
        Returns:
            已删除的数据项ID列表
        """
        try:
            deleted_items = []
            
            # 遍历所有集合
            for collection in collections:
                collection_id = collection['_id']
                collection_type = collection.get('type', '')
                
                
                # 检查类型是否为 virtual 或 file
                if collection_type not in ['virtual', 'file']:
                    self.logger.debug(f"跳过集合 {collection_id}: 类型 {collection_type} 不符合要求")
                    continue
                
                try:
                    # 获取集合的所有数据
                    payload = {
                        "collectionId": collection_id,
                        "offset": 0,
                        "pageSize": 1000,  # 设置较大的页面大小以获取所有数据
                        "searchText": ""
                    }
                    
                    response = await self.session.post(
                        f"{self.base_url}/api/core/dataset/data/list",
                        json=payload
                    )
                    
                    if response.status == 200:
                        data = await response.json()
                        items_list = data.get('data', {}).get('list', [])
                        
                        # 遍历每个数据项
                        for item in items_list:
                            item_id = item.get('_id')
                            video_path = item.get('a', '')
                            
                            # 检查视频文件是否存在
                            if video_path and not os.path.exists(video_path):
                                self.logger.info(f"视频文件不存在: {video_path}")
                                
                                # 删除数据项
                                try:
                                    delete_response = await self.session.delete(
                                        f"{self.base_url}/api/core/dataset/data/delete",
                                        params={"id": item_id}
                                    )
                                    
                                    if delete_response.status == 200:
                                        self.logger.info(f"成功删除数据项: {item_id}")
                                        deleted_items.append(item_id)
                                    else:
                                        response_text = await delete_response.text()
                                        self.logger.error(f"删除数据项 {item_id} 失败: HTTP {delete_response.status}, 详细信息: {response_text}")
                                        
                                except Exception as e:
                                    self.logger.error(f"删除数据项 {item_id} 时发生错误: {str(e)}")
                                    continue
                    else:
                        response_text = await response.text()
                        self.logger.error(f"获取集合 {collection_id} 数据失败: HTTP {response.status}, 详细信息: {response_text}")
                    
                except Exception as e:
                    self.logger.error(f"处理集合 {collection_id} 时出错: {str(e)}")
                    continue
            
            self.logger.info(f"删除操作完成，共删除 {len(deleted_items)} 个数据项")
            return deleted_items
            
        except Exception as e:
            self.logger.error(f"删除不存在视频的数据项操作失败: {str(e)}")
            raise



async def test_knowledge_base():
    """测试知识库处理功能"""
    try:
        # 测试配置
        config = type('Config', (), {
            'knowledge_base_config': {
                'base_url': 'https://ai.blue-converse.com',  # 替换为实际的 URL
                'username': 'jin.peng@bluefocus.com',  # 替换为实际的用户名
                'password': 'dbe3f19da9003fb6d486b71fd177546e990e915f2639875111ef3cd3007a0564',  # 替换为实际的密码
                'teamId': '65f407209e12313ab6e42dca',    # 替换为实际的团队 ID
                'datasetId': '67247389e826ef2d809ecaa7'  # 替换为实际的数据集 ID
            }
        })()

        print("初始化知识库处理器...")
        async with KnowledgeBaseHandler(config) as handler:
            print("成功初始化并登录")
            
            # 获取所有集合
            print("获取所有集合...")
            collections = await handler.get_all_collections(config.knowledge_base_config['datasetId'])
            print(f"获取到 {len(collections)} 个集合")
            
            # 删除空集合
            print("开始删除空集合...")
            deleted_collections = await handler.delete_nonexist_collections(collections)
            print(f"已删除的集合: {deleted_collections}")
            
            return deleted_collections
            
    except Exception as e:
        print(f"测试过程中发生错误: {str(e)}")
        raise

if __name__ == "__main__":
    import asyncio
    import os
    
    # 设置环境变量（如果需要）
    #os.environ['OPENAI_API_KEY'] = 'sk-u1lDoRu9zddCGt41Ws8v3btypD8e7mDnuek41du7r1joHm5f'  # 替换为实际的 API key
    
    # 运行测试
    print("开始测试...")
    result = asyncio.run(test_knowledge_base())
    print("测试完成")