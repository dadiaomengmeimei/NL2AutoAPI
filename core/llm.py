"""LLM 调用封装。"""
import json
import re
import time
from typing import Optional, Any

import requests

from .config import llm_config
from .logger import get_logger

logger = get_logger()


def _build_chat_url() -> str:
    """构造 OpenAI 兼容聊天接口地址。"""
    base_url = (llm_config.base_url or "").strip().rstrip("/")
    if not base_url:
        raise ValueError("未配置 llm.base_url")

    if base_url.endswith("/chat/completions"):
        return base_url
    if "/chat/completions" in base_url:
        return base_url
    return f"{base_url}/chat/completions"


def _extract_content(response_json: dict) -> str:
    """从 OpenAI 兼容响应中提取文本。"""
    choices = response_json.get("choices") or []
    if not choices:
        return json.dumps(response_json, ensure_ascii=False)

    message = choices[0].get("message") or {}
    content = message.get("content", "")

    if isinstance(content, str):
        return content

    if isinstance(content, list):
        chunks = []
        for item in content:
            if isinstance(item, str):
                chunks.append(item)
            elif isinstance(item, dict) and item.get("type") == "text":
                chunks.append(item.get("text", ""))
        return "\n".join(part for part in chunks if part)

    return json.dumps(content, ensure_ascii=False)


def call_llm(prompt: str) -> str:
    """基础 LLM 调用，含 403/429 自动重试。"""
    url = _build_chat_url()
    headers = {"Content-Type": "application/json"}
    if llm_config.api_key:
        headers["Authorization"] = f"Bearer {llm_config.api_key}"

    payload = {
        "model": llm_config.model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": llm_config.temperature,
        "top_p": llm_config.top_p,
        "max_tokens": llm_config.max_tokens,
    }

    last_exc = None
    for attempt in range(llm_config.max_retries):
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=llm_config.timeout)
            if response.status_code in (403, 429, 502, 503):
                wait = llm_config.retry_delay * (attempt + 1)
                logger.warning(
                    "LLM HTTP %d (attempt %d/%d), retrying in %.1fs... url=%s model=%s",
                    response.status_code, attempt + 1, llm_config.max_retries, wait, url, llm_config.model,
                )
                time.sleep(wait)
                last_exc = RuntimeError(f"HTTP {response.status_code}: {response.text[:200]}")
                continue
            response.raise_for_status()
            return _extract_content(response.json())
        except requests.exceptions.HTTPError as exc:
            last_exc = exc
            logger.error("LLM HTTP error (attempt %d): %s", attempt + 1, exc)
            time.sleep(llm_config.retry_delay)
        except requests.exceptions.Timeout as exc:
            last_exc = exc
            logger.warning("LLM timeout (attempt %d/%d)", attempt + 1, llm_config.max_retries)
            time.sleep(llm_config.retry_delay)
        except Exception as exc:
            last_exc = exc
            logger.error("LLM request failed (attempt %d): %s", attempt + 1, exc)
            raise RuntimeError(f"LLM request failed: {exc}") from exc

    raise RuntimeError(f"LLM request failed after {llm_config.max_retries} retries: {last_exc}") from last_exc


def call_llm_json(prompt: str, retry: int = None) -> Optional[dict]:
    """
    调用LLM并解析JSON结果
    
    Args:
        prompt: 提示词
        retry: 重试次数，默认使用配置
    
    Returns:
        解析后的JSON对象，失败返回None
    """
    if retry is None:
        retry = llm_config.max_retries
    
    for attempt in range(retry):
        try:
            response = call_llm(prompt).strip()
            
            # 清理markdown代码块
            response = re.sub(r"^```json\s*", "", response)
            response = re.sub(r"^```\s*", "", response)
            response = re.sub(r"\s*```$", "", response)
            
            return json.loads(response)
            
        except json.JSONDecodeError as e:
            logger.warning(f"LLM JSON parse error attempt {attempt+1}: {e}")
            logger.debug(f"原始响应: {response[:200]}...")
        except Exception as e:
            logger.error(f"LLM error attempt {attempt+1}: {e}")
        
        time.sleep(llm_config.retry_delay)
    
    return None


def call_llm_with_schema(prompt: str, schema_class: type, retry: int = None) -> Optional[Any]:
    """
    调用LLM并验证返回符合Pydantic模型
    
    Args:
        prompt: 提示词
        schema_class: Pydantic模型类
        retry: 重试次数
    
    Returns:
        模型实例，失败返回None
    """
    from pydantic import ValidationError
    
    for attempt in range(retry or llm_config.max_retries):
        data = call_llm_json(prompt, retry=1)  # 内部不重试，由外层控制
        if data is None:
            time.sleep(llm_config.retry_delay)
            continue
        
        try:
            return schema_class(**data)
        except ValidationError as e:
            print(f"  [Schema validation error attempt {attempt+1}] {e}")
            time.sleep(llm_config.retry_delay)
    
    return None