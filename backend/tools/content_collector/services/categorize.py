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
    # 'other' kept internally as a fallback bucket so the classifier always
    # has somewhere to put unmatched items, but the UI doesn't surface it.
    "other": {
        "zh": "其他",
        "en": "Other",
        "desc": "Fallback bucket — hidden from the UI by default.",
    },
}

VALID_CATEGORIES = set(CATEGORIES.keys())


# ---------------------------------------------------------------------------
# Source-level pre-classification
# ---------------------------------------------------------------------------

# Some sources are single-topic publications. Any post from them maps to the
# same category regardless of what the headline says. This is checked BEFORE
# keyword rules and BEFORE the LLM, so Dale's Tech tab stops showing "News"
# mislabels for IT 之家 / Hacker News / Simon Willison and similar.
#
# Keep this conservative: only add a source here if ~95% of its posts belong
# to one category. Mixed sources (Weibo 热搜, 微博要闻) should stay blank so
# per-item rules/LLM decide.
SOURCE_FORCED_CATEGORY: dict[str, str] = {
    # Tech / AI / dev / startups — everything they publish is "knowledge"
    "ithome": "knowledge",
    "hackernews": "knowledge",
    "github_trending": "knowledge",
    "simon_willison": "knowledge",
    "hf_daily_papers": "knowledge",
    # Follow Builders — curated AI X/blog/podcast feed
    "follow_builders_x": "knowledge",
    "follow_builders_blogs": "knowledge",
    "follow_builders_podcasts": "knowledge",
}

# Sources whose `Source.category == 'tech'` / 'china' / 'world' get a
# weaker fallback after rules fail but before LLM.
_SOURCE_CATEGORY_FALLBACK: dict[str, str] = {
    "tech": "knowledge",
    # Anything clearly journalistic goes to news by default. Better than
    # asking the LLM to decide on every 虎嗅 / 36氪 / BBC headline.
    "china": "news",
    "world": "news",
    # `finance` and other edge categories fall through to 'other' via LLM
    # or rule_fallback.
}


# ---------------------------------------------------------------------------
# Rule-based classifier
# ---------------------------------------------------------------------------

# Order matters: first match wins. Tuned from real samples in the DB.
# Keywords are case-insensitive for English; Chinese is already case-free.
_RULES: list[tuple[str, list[str]]] = [
    # ============================================================
    # OTHER — everything Dale doesn't want in the main feed:
    # entertainment, sports, gaming, lifestyle, markets, AND now also
    # health + growth topics (previously their own categories, demoted
    # because Dale isn't using them day-to-day).
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
    # HEALTH & GROWTH topics — demoted to 'other' so they don't pollute
    # the News or AI·Tech tabs. Dale explicitly opted out of these as
    # first-class categories.
    # ============================================================
    ("other", [
        # Health
        r"病毒", r"疫情", r"疫苗", r"确诊", r"癌症", r"糖尿病",
        r"高血压", r"心血管", r"艾滋", r"HIV", r"COVID",
        r"汉坦", r"流感", r"发烧", r"卵巢", r"子宫", r"乳腺",
        r"白血病", r"阿尔茨海默", r"帕金森", r"\bWHO\b", r"疾控",
        r"医保", r"医院", r"住院", r"手术", r"临床",
        r"蚕豆病", r"消杀",
        # Fitness / wellness
        r"减肥", r"健身", r"瑜伽", r"跑步机", r"养生",

        # Mental / emotional
        r"抑郁", r"焦虑", r"失眠", r"心理健康", r"精神内耗",
        r"情绪", r"孤独", r"内耗", r"自我认知",
        r"冥想", r"正念", r"\bMBTI\b",
        r"原生家庭", r"亲密关系", r"分手", r"相亲", r"婚姻",
        r"恋爱", r"追求",

        # Philosophy / self-improvement
        r"哲学", r"人生意义", r"读书", r"书评", r"书单",
        r"如何自学", r"如何应对", r"如何处理",
        r"辞职", r"跳槽", r"内卷", r"躺平", r"摆烂",
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
    "cares about two topics; everything else is 'other'.\n\n"
    "Categories (closed set):\n"
    "- news:      social events, public affairs, controversies, politics, international, "
    "law enforcement, disasters — the stuff you'd see on a news site's front page.\n"
    "- knowledge: AI, LLMs, developer tools, open source, hardware, science, "
    "startups, funding, Product Hunt, tech companies.\n"
    "- other:     everything else (health, psychology, self-help, entertainment, "
    "celebrities, sports, video games, lifestyle/shopping, stock markets, crypto, food). "
    "The user explicitly does NOT want these highlighted.\n\n"
    "Rules:\n"
    "- Return ONLY a JSON object: {\"results\": [{\"i\": <index>, \"c\": \"<category>\"}]}\n"
    "- Chinese titles are OK — classify by meaning, not language.\n"
    "- When a headline spans multiple buckets, pick the primary intent. "
    "E.g. 'OpenAI sued by NYT' → knowledge (it's about an AI company).\n"
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
            # Newer reasoning-capable models (gpt-5.x, o-series) require
            # `max_completion_tokens`; classic chat models accept `max_tokens`.
            # Use the new param — newer models need it, older ones accept it.
            body = {
                "model": model,
                "messages": [
                    {"role": "system", "content": _LLM_SYSTEM_PROMPT},
                    {"role": "user", "content": user_payload},
                ],
                "response_format": {"type": "json_object"},
                "temperature": 0.0,
                "max_completion_tokens": 1500,
            }
            resp = await client.post(
                url,
                headers={
                    "api-key": api_key,
                    "Content-Type": "application/json",
                },
                json=body,
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


async def _fetch_source_map(
    session: AsyncSession, source_ids: Iterable[int]
) -> dict[int, tuple[str, str]]:
    """Return {source_id: (slug, category)} for the given ids."""
    ids = list(set(source_ids))
    if not ids:
        return {}
    rows = (
        await session.execute(
            select(Source.id, Source.slug, Source.category).where(Source.id.in_(ids))
        )
    ).all()
    return {r[0]: (r[1], r[2]) for r in rows}


async def categorize_backlog(
    batch_size: int = 50,
    use_llm: bool = True,
    llm_cap: int | None = None,
) -> dict:
    """Classify items with category IS NULL.

    Order of attempts per item:
      1. SOURCE_FORCED_CATEGORY    — pinned single-topic sources
      2. keyword rules             — cheap, deterministic
      3. SOURCE_CATEGORY_FALLBACK  — use the publisher's own category (tech,
                                     health, growth) as a cheap fallback.
                                     This lets us skip the LLM for many items.
      4. LLM                       — only if caller opts in AND we still have
                                     unresolved items. Respects `llm_cap`
                                     so a single run can never blow the
                                     token budget.

    The cascade is ordered so that 90%+ of items never touch the LLM. If the
    LLM fails (network/quota), items stay NULL so the next tick retries.
    """
    factory = session_factory()

    async with factory() as session:
        items = (
            await session.execute(
                select(Item).where(Item.category.is_(None)).limit(batch_size)
            )
        ).scalars().all()

        if not items:
            return {
                "processed": 0,
                "source": 0,
                "rule": 0,
                "source_fallback": 0,
                "llm": 0,
                "deferred": 0,
            }

        source_map = await _fetch_source_map(
            session, (it.source_id for it in items)
        )

        source_hits: list[tuple[Item, str]] = []
        rule_hits: list[tuple[Item, str]] = []
        fallback_hits: list[tuple[Item, str]] = []
        unresolved: list[Item] = []

        for it in items:
            slug, src_cat = source_map.get(it.source_id, ("", ""))
            # Tier 1: source is pinned to one category
            forced = SOURCE_FORCED_CATEGORY.get(slug)
            if forced:
                source_hits.append((it, forced))
                continue
            # Tier 2: title keyword rules
            cat = rule_classify(it.title)
            if cat:
                rule_hits.append((it, cat))
                continue
            # Tier 3: the source's publication category as fallback
            # (saves the LLM call for 100% of tech/health/growth-native sources)
            fb = _SOURCE_CATEGORY_FALLBACK.get(src_cat)
            if fb:
                fallback_hits.append((it, fb))
                continue
            unresolved.append(it)

        source_count = await _apply_categories(
            session,
            [x[0] for x in source_hits],
            [x[1] for x in source_hits],
            "source",
        )
        rule_count = await _apply_categories(
            session,
            [x[0] for x in rule_hits],
            [x[1] for x in rule_hits],
            "rule",
        )
        fallback_count = await _apply_categories(
            session,
            [x[0] for x in fallback_hits],
            [x[1] for x in fallback_hits],
            "source_fallback",
        )

        llm_count = 0
        deferred_count = 0
        if unresolved and use_llm:
            # Hard cap — protects token budget even if backlog is huge.
            to_classify = (
                unresolved if llm_cap is None else unresolved[: max(0, llm_cap)]
            )
            remaining_for_next_tick = unresolved[len(to_classify):]

            if to_classify:
                llm_cats = await _llm_classify_batch(
                    [it.title for it in to_classify]
                )
                answered = any(c != "other" for c in llm_cats)
                if answered:
                    await _apply_categories(
                        session, to_classify, llm_cats, "llm"
                    )
                    llm_count = len(to_classify)
                else:
                    logger.info(
                        "content_collector: %d items deferred — LLM did not "
                        "return useful labels; will retry next tick",
                        len(to_classify),
                    )
                    deferred_count += len(to_classify)
            deferred_count += len(remaining_for_next_tick)
        elif unresolved:
            # LLM off entirely: leave them NULL rather than committing 'other'.
            # A manual /admin/categorize-drain with use_llm=true can still
            # clean them up later, or they'll flow through next time the
            # scheduler calls this with use_llm=true.
            deferred_count = len(unresolved)

        await session.commit()

    result = {
        "processed": len(items),
        "source": source_count,
        "rule": rule_count,
        "source_fallback": fallback_count,
        "llm": llm_count,
        "deferred": deferred_count,
    }
    logger.info("content_collector: categorize_backlog %s", result)
    return result


async def reclassify_all_with_rules() -> dict:
    """Re-apply source-level + title-rule classification to EVERY item.

    Useful after tuning keywords or adding entries to SOURCE_FORCED_CATEGORY.
    Leaves LLM-tagged items untouched UNLESS a source or rule now overrides
    them — this is how we retroactively fix historical mislabels like IT 之家
    posts that were previously tagged as 'news'.
    """
    factory = session_factory()
    now = datetime.now(timezone.utc)
    changed = 0
    total = 0

    async with factory() as session:
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

            source_map = await _fetch_source_map(
                session, (it.source_id for it in chunk)
            )

            for it in chunk:
                slug, _src_cat = source_map.get(it.source_id, ("", ""))
                new_cat: str | None = SOURCE_FORCED_CATEGORY.get(slug)
                new_source_tag = "source"
                if not new_cat:
                    # Fall back to title rules
                    rc = rule_classify(it.title)
                    if rc:
                        new_cat = rc
                        new_source_tag = "rule"

                if new_cat and it.category != new_cat:
                    it.category = new_cat
                    it.category_source = new_source_tag
                    it.categorized_at = now
                    changed += 1
            await session.commit()

    return {"total": total, "changed": changed}
