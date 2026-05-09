"""Content classifier — assigns items to one of 13 content categories.

Two-stage design:
  1. RULES     — keyword matching, ~70% coverage, 0ms, 0¥
  2. LLM       — only runs for items rules couldn't decide, batched for cost

Categories (see CATEGORIES below) cover Dale's real information diet: AI,
科技, 商业, 财经, 健康, 社会, 时政, 娱乐, 体育, 游戏, 人文, 生活, other.

The classifier does NOT block ingest. Items enter with category=NULL, and a
periodic scheduler job drains the backlog. A POST admin endpoint re-runs
rules on ALL items (safe & cheap) whenever rules are tuned.
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Iterable

import httpx
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import session_factory
from ..models.item import Item
from ..models.source import Source

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Category definitions
# ---------------------------------------------------------------------------

CATEGORIES: dict[str, dict] = {
    "news": {
        "zh": "民生新闻",
        "en": "News",
        "desc": "Social events, public affairs, politics, international",
    },
    "knowledge": {
        "zh": "AI · 科技 · 创业",
        "en": "AI · Tech · Startups",
        "desc": "AI, LLMs, dev tools, hardware, open source, Product Hunt, VC",
    },
    "health": {
        "zh": "健康医疗",
        "en": "Health",
        "desc": "Diseases, medicine, fitness, mental health, biotech",
    },
    "growth": {
        "zh": "情感成长",
        "en": "Growth",
        "desc": "Psychology, relationships, self-improvement, philosophy, life reflections",
    },
    "other": {
        "zh": "其他",
        "en": "Other",
        "desc": "Entertainment, sports, gaming, lifestyle, markets — not of interest by default",
    },
}

VALID_CATEGORIES = set(CATEGORIES.keys())


# ---------------------------------------------------------------------------
# Rule-based classifier
# ---------------------------------------------------------------------------

# Order matters: first match wins. Tuned from real samples in the DB.
# Keywords are case-insensitive for English; Chinese is already case-free.
_RULES: list[tuple[str, list[str]]] = [
    # ============================================================
    # OTHER — things Dale explicitly doesn't care about. Match these
    # FIRST so obvious stuff skips the LLM entirely.
    # ============================================================

    # Entertainment: celebrities, shows, concerts, K-pop
    ("other", [
        r"百想", r"金鸡", r"金马奖", r"金像奖", r"艾美奖", r"奥斯卡",
        r"格莱美", r"MTV大赏", r"五月天", r"周杰伦", r"林俊杰", r"蔡依林",
        r"\bBTS\b", r"\bBLACKPINK\b", r"\bIVE\b", r"Newjeans", r"爱豆",
        r"韩剧", r"日剧", r"美剧", r"电视剧", r"综艺", r"脱口秀", r"相声",
        r"演唱会", r"明星", r"出道", r"解约", r"塌房", r"炒cp",
        r"视后", r"影后", r"视帝", r"影帝", r"最佳男主", r"最佳女主",
        r"王俊凯", r"宋亚轩", r"刘耀文", r"范丞丞", r"朴宝英", r"文佳煐",
        r"肖战", r"王一博", r"杨紫", r"赵丽颖", r"迪丽热巴", r"鞠婧祎",
        r"张艺兴", r"易烊千玺", r"王源", r"李现", r"白鹿", r"刘亦菲",
        r"杨幂", r"赵露思", r"沈腾", r"贾玲", r"黄渤", r"张颂文",
        r"朱一龙", r"张婧仪", r"黄晓明", r"李金铭",
        r"电影票房", r"Netflix", r"Disney\+", r"Hulu", r"HBO",
        r"\bMV\b", r"单曲", r"片场", r"开机", r"杀青", r"定档", r"撤档",
        r"阿凡达", r"变形金刚", r"速度与激情", r"复仇者联盟",
    ]),

    # Sports
    ("other", [
        r"国乒", r"世乒赛", r"乒乓球", r"奥运", r"冬奥", r"世界杯",
        r"欧洲杯", r"美洲杯", r"亚运", r"金牌", r"银牌", r"铜牌",
        r"\bNBA\b", r"\bCBA\b", r"\bMLB\b", r"\bNFL\b", r"\bNHL\b",
        r"英超", r"西甲", r"德甲", r"意甲", r"法甲", r"中超",
        r"欧冠", r"\bUEFA\b", r"\bFIFA\b", r"皇马", r"巴萨", r"曼联",
        r"湖人", r"勇士", r"凯尔特人", r"雷霆", r"快船",
        r"梅西", r"\bC罗\b", r"詹姆斯", r"库里",
        r"网球", r"高尔夫", r"羽毛球", r"马拉松",
        r"夺冠", r"晋级四强", r"出线", r"夺金", r"卫冕",
        r"斯诺克", r"台球", r"赵心童", r"丁俊晖", r"\bF1\b", r"Formula 1",
        r"\bUFC\b", r"拳击", r"孙杨", r"全红婵",
    ]),

    # Gaming
    ("other", [
        r"原神", r"王者荣耀", r"英雄联盟", r"\bLOL\b", r"星穹铁道",
        r"崩坏", r"米哈游", r"Steam游戏", r"Epic Games", r"PS5", r"Xbox",
        r"任天堂", r"Switch", r"塞尔达", r"EDG", r"TES", r"RNG",
        r"KPL", r"LPL", r"\bS\d+赛季\b", r"开黑", r"上分",
        r"Roblox", r"Minecraft", r"\bGTA\b", r"CS:GO", r"\bCS2\b",
        r"Valorant", r"\bPUBG\b", r"电竞", r"游戏主播", r"丰川祥子",
    ]),

    # Lifestyle / consumption
    ("other", [
        r"美食", r"菜谱", r"探店", r"餐厅", r"咖啡", r"奶茶", r"火锅",
        r"穿搭", r"时尚", r"潮流", r"球鞋", r"Supreme", r"优衣库", r"Zara",
        r"旅行攻略", r"民宿", r"打卡",
        r"装修", r"家居", r"宜家", r"\bIKEA\b",
        r"护肤", r"化妆", r"彩妆", r"香水", r"口红", r"面膜",
        r"洞洞鞋", r"潮鞋", r"tutu裙", r"穿裙子",
    ]),

    # Finance / markets (Dale said he's not interested in finance per-se)
    ("other", [
        r"股市", r"股票", r"A股", r"港股", r"美股", r"纳指", r"道指",
        r"上证", r"深证", r"恒生", r"标普", r"S&P 500", r"NASDAQ",
        r"基金", r"\bETF\b", r"财报", r"涨停", r"跌停", r"涨超", r"跌超",
        r"美联储", r"加息", r"降息", r"通胀", r"\bCPI\b", r"\bPPI\b",
        r"\bGDP\b", r"\bPMI\b", r"汇率", r"巴菲特",
    ]),

    # ============================================================
    # KNOWLEDGE — AI, tech, open source, startups. High priority —
    # match before NEWS because "OpenAI lawsuit" should stay knowledge.
    # ============================================================
    ("knowledge", [
        # AI
        r"\bAI\b", r"\bLLM\b", r"\bAGI\b", r"\bGPT\b", r"ChatGPT",
        r"Claude", r"Gemini", r"Anthropic", r"OpenAI", r"Midjourney",
        r"Stable Diffusion", r"Copilot", r"DeepSeek", r"Qwen", r"Llama",
        r"Mistral", r"Grok", r"Sora", r"\bagent\b", r"\bprompt\b",
        r"embedding", r"RAG", r"fine-?tune",
        r"大模型", r"人工智能", r"机器学习", r"深度学习", r"神经网络",
        r"生成式", r"多模态", r"智能体", r"通义千问", r"豆包", r"文心",
        r"混元", r"Kimi", r"千问", r"文心一言",

        # Dev / open source
        r"GitHub", r"\bGit\b", r"Linux", r"Ubuntu", r"Debian",
        r"macOS", r"Windows", r"\biOS\b", r"Android", r"HarmonyOS", r"鸿蒙",
        r"Kubernetes", r"\bK8s\b", r"Docker", r"Redis", r"Postgres",
        r"MySQL", r"MongoDB", r"Rust", r"\bPython\b", r"JavaScript",
        r"TypeScript", r"React", r"Vue", r"Next\.js", r"Svelte",
        r"开源", r"API", r"SDK", r"framework",
        r"CVE-\d{4}", r"\bGPU\b", r"\bCPU\b", r"Wi-?Fi",

        # Hardware / platforms
        r"iPhone", r"iPad", r"\bMac\b", r"MacBook", r"Apple Watch",
        r"Pixel", r"小米", r"华为", r"\bOPPO\b", r"vivo", r"荣耀",
        r"芯片", r"半导体", r"台积电", r"高通", r"英伟达", r"NVIDIA",
        r"特斯拉", r"SpaceX", r"星链", r"Starlink", r"航天",
        r"CRISPR", r"量子",

        # Chinese tech brands
        r"比亚迪", r"理想汽车", r"理想MEGA", r"蔚来", r"小鹏",
        r"极氪", r"问界", r"阿维塔", r"网易云", r"B站", r"字节跳动",
        r"Meta", r"Instagram", r"WhatsApp", r"Signal", r"Telegram",

        # Startups / business (the "创业" part of knowledge)
        r"创业", r"融资", r"\bIPO\b", r"\bM&A\b", r"收购", r"并购",
        r"估值", r"独角兽", r"A轮", r"B轮", r"C轮", r"Pre-IPO",
        r"Y Combinator", r"\bYC\b", r"红杉", r"Andreessen", r"a16z",
        r"Product Hunt", r"\bSaaS\b", r"\bPLG\b", r"bootstrap",
        r"Shopify", r"Stripe", r"Airbnb", r"Uber",
        r"startup", r"founder", r"\bMVP\b", r"venture", r"\bVC\b",
    ]),

    # ============================================================
    # HEALTH — disease, medicine, fitness, mental health
    # ============================================================
    ("health", [
        r"病毒", r"疫情", r"疫苗", r"确诊", r"癌症", r"糖尿病",
        r"高血压", r"心血管", r"艾滋", r"HIV", r"COVID",
        r"汉坦", r"流感", r"发烧", r"卵巢", r"子宫", r"乳腺",
        r"白血病", r"阿尔茨海默", r"帕金森", r"\bWHO\b", r"疾控",
        r"医保", r"医院", r"住院", r"手术", r"临床",
        r"蚕豆病", r"消杀",
        # Fitness / wellness (the physical health side)
        r"减肥", r"健身", r"瑜伽", r"跑步机", r"养生",
    ]),

    # ============================================================
    # GROWTH — psychology, relationships, self-improvement, philosophy,
    # life reflections. Many of Dale's Zhihu feed lives here.
    # ============================================================
    ("growth", [
        # Mental / emotional
        r"抑郁", r"焦虑", r"失眠", r"心理健康", r"精神内耗",
        r"情绪", r"孤独", r"内耗", r"自我认知", r"自我",
        r"冥想", r"正念", r"\bMBTI\b", r"\bINFP\b", r"\bINTJ\b",
        r"\bENFP\b", r"\bISFJ\b",
        r"原生家庭", r"亲密关系", r"分手", r"相亲", r"婚姻",
        r"恋爱", r"追求", r"喜欢的人", r"前任",

        # Philosophy / humanities reflection
        r"哲学", r"意义", r"人生", r"为什么活", r"活着",
        r"读书", r"书评", r"书单",

        # Career / self-improvement (often fuzzy; match on question form)
        r"如何自学", r"如何应对", r"如何处理", r"怎么做",
        r"职场", r"辞职", r"跳槽", r"内卷", r"躺平", r"摆烂",
        r"考研", r"保研", r"高考",
    ]),

    # ============================================================
    # NEWS — social events, politics, controversies. This catches
    # hot-search-style headlines after the above buckets are applied.
    # ============================================================
    ("news", [
        # Politics & international
        r"总统", r"首相", r"外长", r"大使", r"\bG20\b", r"\bG7\b",
        r"联合国", r"\bUN\b", r"\bNATO\b", r"北约", r"欧盟", r"\bEU\b",
        r"白宫", r"克里姆林宫", r"国会", r"国防部", r"国务院",
        r"特朗普", r"拜登", r"普京", r"泽连斯基", r"马克龙",
        r"俄乌", r"乌克兰战争", r"以色列", r"巴勒斯坦", r"哈马斯",
        r"加沙", r"台海", r"朝鲜", r"制裁", r"关税", r"外交", r"免签",

        # Social events / public affairs
        r"行拘", r"逮捕", r"判刑", r"获刑", r"起诉", r"劳动仲裁",
        r"仅退款", r"爆炸", r"火灾", r"地震", r"洪水", r"台风",
        r"车祸", r"坠楼", r"坠机", r"事故",
        r"央视曝光", r"央视记者", r"新华社", r"人民日报",
        r"派出所", r"公安", r"检察院", r"法院", r"警方",
        r"官方回应", r"最新通报", r"执法",
    ]),
]

# Compile once at import
_COMPILED: list[tuple[str, re.Pattern]] = [
    (cat, re.compile("|".join(f"(?:{p})" for p in pats), re.IGNORECASE))
    for cat, pats in _RULES
]


def rule_classify(title: str) -> str | None:
    """Return the first matching category, or None if no rule matched."""
    if not title:
        return None
    for cat, pattern in _COMPILED:
        if pattern.search(title):
            return cat
    return None


# ---------------------------------------------------------------------------
# LLM fallback (Azure OpenAI chat deployment)
# ---------------------------------------------------------------------------

_LLM_SYSTEM_PROMPT = (
    "You classify news headlines for a personal dashboard. The user ONLY "
    "cares about four topics; everything else is 'other'.\n\n"
    "Categories (closed set):\n"
    "- news:      social events, public affairs, controversies, politics, international, "
    "law enforcement, disasters — the stuff you'd see on a news site's front page.\n"
    "- knowledge: AI, LLMs, developer tools, open source, hardware, science, "
    "startups, funding, Product Hunt, tech companies.\n"
    "- health:    diseases, epidemics, vaccines, medicine, fitness, mental health, biotech.\n"
    "- growth:    psychology, relationships, self-improvement, philosophy, "
    "life reflections, career advice, introspective Zhihu-style questions.\n"
    "- other:     entertainment, celebrities, sports, video games, lifestyle/shopping, "
    "stock markets, crypto, food — the user explicitly does NOT want these highlighted.\n\n"
    "Rules:\n"
    "- Return ONLY a JSON object: {\"results\": [{\"i\": <index>, \"c\": \"<category>\"}]}\n"
    "- Chinese titles are OK — classify by meaning, not language.\n"
    "- When a headline spans multiple buckets, pick the primary intent. "
    "E.g. 'OpenAI sued by NYT' → knowledge (it's about an AI company).\n"
    "- Zhihu-style philosophical/personal questions → growth.\n"
    "- Breaking hot-search events about people/places/crimes → news.\n"
    "- When truly in doubt, 'other' is a safe default.\n"
)


async def _llm_classify_batch(titles: list[str]) -> list[str]:
    """Classify a batch of titles via Azure OpenAI. Returns same-length list.

    Uses the same Azure OpenAI resource ImageLingo already has configured:
        AZURE_OPENAI_ENDPOINT       (e.g. https://foundry-llm-zg.services.ai.azure.com/openai/v1)
        AZURE_OPENAI_API_KEY
        AZURE_OPENAI_CHAT_MODEL     (optional; default 'gpt-4o-mini')

    On any failure, returns ['other'] * len(titles) without raising — the
    calling job will retry the items on the next tick.
    """
    if not titles:
        return []

    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    api_key = os.getenv("AZURE_OPENAI_API_KEY")
    model = os.getenv("AZURE_OPENAI_CHAT_MODEL", "gpt-4o-mini")

    if not (endpoint and api_key):
        logger.warning(
            "content_collector: LLM classify skipped (missing AZURE_OPENAI_*). "
            "%d items stay 'other'.",
            len(titles),
        )
        return ["other"] * len(titles)

    # Foundry 'Chat Completions' compat endpoint: {endpoint}/chat/completions
    url = endpoint.rstrip("/") + "/chat/completions"
    user_payload = "\n".join(f"{i}. {t[:200]}" for i, t in enumerate(titles))

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                url,
                headers={
                    "api-key": api_key,
                    "Content-Type": "application/json",
                },
                json={
                    "model": model,
                    "messages": [
                        {"role": "system", "content": _LLM_SYSTEM_PROMPT},
                        {"role": "user", "content": user_payload},
                    ],
                    "response_format": {"type": "json_object"},
                    "temperature": 0.0,
                    "max_tokens": 1500,
                },
            )
            if resp.status_code >= 400:
                # Azure returns the useful error body; surface it so we can
                # diagnose issues like content-filter rejects, quota limits,
                # or model mismatches that would otherwise be invisible.
                logger.warning(
                    "content_collector: LLM classify HTTP %d — body: %s",
                    resp.status_code,
                    resp.text[:500],
                )
                return ["other"] * len(titles)
            content = resp.json()["choices"][0]["message"]["content"]
            parsed = json.loads(content)
            results = parsed.get("results") or []
    except Exception as e:
        logger.warning("content_collector: LLM classify failed: %s", e)
        return ["other"] * len(titles)

    out = ["other"] * len(titles)
    for r in results:
        try:
            idx = int(r["i"])
            cat = str(r["c"]).strip().lower()
            if 0 <= idx < len(titles) and cat in VALID_CATEGORIES:
                out[idx] = cat
        except (KeyError, ValueError, TypeError):
            continue
    return out


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

async def _apply_categories(
    session: AsyncSession,
    items: Iterable[Item],
    categories: Iterable[str],
    source_tag: str,
) -> int:
    now = datetime.now(timezone.utc)
    count = 0
    for item, cat in zip(items, categories):
        if cat not in VALID_CATEGORIES:
            cat = "other"
        item.category = cat
        item.category_source = source_tag
        item.categorized_at = now
        count += 1
    return count


async def categorize_backlog(
    batch_size: int = 50, use_llm: bool = True
) -> dict:
    """Classify all items with category IS NULL.

    Applies rules first; whatever rules can't decide goes through the LLM
    (if configured). If the LLM fails, items are LEFT uncategorized so the
    next tick retries them — avoids permanent 'other' pollution when LLM
    has a transient outage.
    """
    factory = session_factory()

    async with factory() as session:
        items = (
            await session.execute(
                select(Item).where(Item.category.is_(None)).limit(batch_size)
            )
        ).scalars().all()

        if not items:
            return {"processed": 0, "rule": 0, "llm": 0, "deferred": 0}

        rule_hits: list[tuple[Item, str]] = []
        unresolved: list[Item] = []
        for it in items:
            cat = rule_classify(it.title)
            if cat:
                rule_hits.append((it, cat))
            else:
                unresolved.append(it)

        # Apply rule matches
        rule_count = await _apply_categories(
            session,
            [x[0] for x in rule_hits],
            [x[1] for x in rule_hits],
            "rule",
        )

        llm_count = 0
        deferred_count = 0
        if unresolved:
            if use_llm:
                llm_cats = await _llm_classify_batch([it.title for it in unresolved])
                # If the LLM call truly answered, apply results. Otherwise leave
                # category=NULL so the next tick retries — prevents permanent
                # 'other' pollution from a transient 400/5xx.
                answered = any(c != "other" for c in llm_cats)
                if answered:
                    await _apply_categories(session, unresolved, llm_cats, "llm")
                    llm_count = len(unresolved)
                else:
                    # Leave them NULL. Do not update last_error-style fields
                    # since categorization isn't a fetch. Just log & retry.
                    logger.info(
                        "content_collector: %d items deferred — LLM didn't "
                        "return useful labels; will retry next tick",
                        len(unresolved),
                    )
                    deferred_count = len(unresolved)
            else:
                # LLM explicitly disabled: commit them as 'other' so they
                # don't block forever.
                await _apply_categories(
                    session,
                    unresolved,
                    ["other"] * len(unresolved),
                    "rule_fallback",
                )
                deferred_count = 0

        await session.commit()

    result = {
        "processed": len(items),
        "rule": rule_count,
        "llm": llm_count,
        "deferred": deferred_count,
    }
    logger.info("content_collector: categorize_backlog %s", result)
    return result


async def reclassify_all_with_rules() -> dict:
    """Re-apply rules to EVERY item (overwrites previous rule assignments).
    Useful after tuning keywords. Leaves LLM-tagged items untouched unless
    rules now match them.
    """
    factory = session_factory()
    now = datetime.now(timezone.utc)
    changed = 0
    total = 0

    async with factory() as session:
        # Stream in chunks to keep memory bounded
        offset = 0
        while True:
            chunk = (
                await session.execute(
                    select(Item).order_by(Item.id).offset(offset).limit(500)
                )
            ).scalars().all()
            if not chunk:
                break
            offset += 500
            total += len(chunk)
            for it in chunk:
                cat = rule_classify(it.title)
                if cat and (it.category != cat):
                    it.category = cat
                    it.category_source = "rule"
                    it.categorized_at = now
                    changed += 1
            await session.commit()

    return {"total": total, "changed": changed}
