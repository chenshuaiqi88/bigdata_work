import logging
import time
from typing import List, Dict, Any
from lxml import html
import ChromeDebugControllerUtils

# ================= 日志配置 =================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('./outfile/spider_chinazy_policy_full.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ================= 浏览器调试配置 =================
local_debug_dir = r'D:\dev-env\chrome_debug_dir'
local_chrome_debug_port = 9222

chrome_debug_controller = ChromeDebugControllerUtils.ChromeDebugController(
    port=local_chrome_debug_port,
    user_data_dir=local_debug_dir
)


def start_chrome_driver():
    """启动并连接 Chrome 调试实例"""
    chrome_debug_controller.start()
    return chrome_debug_controller.connect_drissionpage()


def extract_policies(html_content: str) -> List[Dict[str, str]]:
    """提取每页的政策名称与链接"""
    policies = []
    try:
        tree = html.fromstring(html_content)
        policy_links = tree.xpath("//div[@class='fylb']//ul/li/a")

        for link in policy_links:
            href = link.get('href', '').strip()
            title = link.get('title', '').strip()

            # 如果 title 为空，则从文本提取（去除日期前缀）
            if not title:
                full_text = ''.join(link.itertext()).strip()
                if len(full_text) > 10 and '-' in full_text[:10]:
                    title = full_text[10:].strip()
                else:
                    title = full_text

            # 拼接完整链接
            if href and title:
                if not href.startswith('http'):
                    href = "https://www.chinazy.org/" + href.lstrip('/')
                policies.append({'link': href, 'name': title})
                logger.info(f"提取到政策: {title} -> {href}")

    except Exception as e:
        logger.error(f"解析页面时出错: {e}")

    return policies


def extract_policy_details(driver, url: str) -> Dict[str, Any]:
    """提取政策详情（包含元信息和正文内容）"""
    details = {
        'title': '',  # 页面标题
        'publish_dept': '',  # 发布部门
        'publish_date': '',  # 发布日期
        'doc_number': '',  # 文号
        'source': '',  # 来源
        'content': ''  # 正文内容
    }

    try:
        logger.info(f"爬取政策详情: {url}")
        driver.get(url)
        driver.wait(2, 5)  # 延长等待时间确保正文加载
        html_content = driver.html
        tree = html.fromstring(html_content)

        # 1. 提取标题（页面主标题）
        title_elems = tree.xpath("//h1[@align='center']/text()")
        if title_elems:
            details['title'] = title_elems[0].strip()

        # 2. 提取发布部门、发布日期、来源（页面顶部信息栏）
        info_elems = tree.xpath("//p[@class='xx']//text()")
        if info_elems:
            info_text = ''.join(info_elems).strip()
            # 提取来源（可能包含发布部门）
            if '来源：' in info_text:
                details['source'] = info_text.split('来源：')[-1].split()[0].strip()
                details['publish_dept'] = details['source']  # 来源通常即发布部门
            # 提取发布日期
            if '发布日期：' in info_text:
                details['publish_date'] = info_text.split('发布日期：')[-1].strip().split()[0]

        # 3. 提取文号（带"号"的官方编号）
        doc_num_elems = tree.xpath(
            "//*[contains(text(), '号') and (contains(@class, 'xx') or not(contains(@class, 'v_news_content')))]//text()")
        if doc_num_elems:
            # 过滤出符合文号格式的内容（如"教基〔2025〕6号"）
            valid_nums = [num.strip() for num in doc_num_elems if '〔' in num and '〕' in num]
            if valid_nums:
                details['doc_number'] = valid_nums[0]

        # 4. 提取正文内容（核心新增逻辑）
        # 正文通常在 class 为 'v_news_content' 的 div 中
        content_container = tree.xpath("//div[@class='v_news_content']")
        if content_container:
            # 获取容器内所有文本节点（包括嵌套标签中的内容）
            content_texts = content_container[0].xpath(".//text()")
            # 清洗文本：过滤空字符串、合并连续空白、保留段落结构
            cleaned_content = []
            for text in content_texts:
                stripped = text.strip()
                if stripped:  # 跳过纯空白内容
                    cleaned_content.append(stripped)
            # 用换行符连接段落，保留原始排版感
            details['content'] = '\n\n'.join(cleaned_content)

        logger.info(f"成功提取详情: {details['title']}（正文长度: {len(details['content'])}字）")

    except Exception as e:
        logger.error(f"提取详情失败（{url}）: {e}")

    return details


if __name__ == "__main__":
    drissionpager = start_chrome_driver()
    all_policies = []
    policy_details_list = []  # 存储所有政策的完整信息（含正文）

    # 生成104页的URL列表
    page_urls = ["https://www.chinazy.org/zcfg.htm"] + [
        f"https://www.chinazy.org/zcfg/{i}.htm" for i in range(103, 0, -1)
    ]

    # 第一步：爬取所有政策的名称和链接
    for idx, url in enumerate(page_urls, 1):
        try:
            logger.info(f"开始爬取第 {idx} 页: {url}")
            drissionpager.get(url)
            drissionpager.wait(2, 5)
            html_content = drissionpager.html
            policies = extract_policies(html_content)
            all_policies.extend(policies)
            logger.info(f"第 {idx} 页提取到 {len(policies)} 条政策")
        except Exception as e:
            logger.error(f"第 {idx} 页抓取失败: {e}")

    logger.info(f"共提取到 {len(all_policies)} 条政策链接，开始提取完整信息（含正文）...")

    # 第二步：遍历链接，提取完整信息（可修改切片控制数量，如[:5]只爬前5条）
    for i, policy in enumerate(all_policies[:], 1):  # 去掉[:]可爬全部
        # 控制爬取速度，避免请求过于频繁
        time.sleep(1)
        details = extract_policy_details(drissionpager, policy['link'])
        # 合并政策基本信息和详情（含正文）
        full_info = {**policy, **details}
        policy_details_list.append(full_info)

        # 输出结果（含正文预览）
        print(f"\n{i}. 名称: {full_info['name']}")
        print(f"   链接: {full_info['link']}")
        print(f"   页面标题: {full_info['title']}")
        print(f"   发布部门: {full_info['publish_dept']}")
        print(f"   发布日期: {full_info['publish_date']}")
        print(f"   文号: {full_info['doc_number']}")
        print(f"   正文预览: {full_info['content'][:200]}..." if full_info[
            'content'] else "   无正文内容")  # 显示前200字预览

    # 保存完整结果到文件
    import json

    with open('./outfile/policy_full_details.json', 'w', encoding='utf-8') as f:
        json.dump(policy_details_list, f, ensure_ascii=False, indent=2)
    logger.info(f"所有政策完整信息已保存到 policy_full_details.json")

