const translations = {
  en: {
    meta: {
      title: "oqbridge | Hot/Cold Tiering Between OpenSearch and Quickwit",
      description:
        "oqbridge is a lightweight proxy that adds automatic hot/cold data tiering, transparent query routing, and historical migration between OpenSearch and Quickwit.",
      lang: "en",
    },
    brand: {
      ariaLabel: "oqbridge home",
    },
    nav: {
      ariaLabel: "Primary navigation",
      overview: "Overview",
      architecture: "Architecture",
      features: "Capabilities",
      lifecycle: "Lifecycle",
      start: "Quick Start",
    },
    hero: {
      eyebrow: "OpenSearch + Quickwit / Hot-Cold Tiering Proxy",
      titleHtml:
        'Keep hot data in <span>OpenSearch</span>, and let cold history settle into <span>Quickwit</span>.',
      text:
        "oqbridge is a lightweight bridge for log and time-series search workloads. It keeps the user-facing query interface OpenSearch-compatible, while moving historical data into Quickwit so longer retention does not automatically mean higher storage cost.",
      primaryCta: "View on GitHub",
      docsCta: "Read Docs",
      docsHref: "https://github.com/leonunix/oqbridge/blob/main/README.md",
      point1:
        "One query entrypoint, without forcing applications to understand two engines.",
      point2:
        "Automatic hot/cold tiering to reduce long-term storage cost.",
      point3:
        "Decoupled proxy and migration roles for gradual production adoption.",
    },
    status: {
      label: "Core profile",
      chip: "lightweight",
      s1: "separate binaries",
      s2: "default hot-data window",
      s3: "default cold retention",
      s4: "OpenSearch-compatible proxy",
    },
    overview: {
      eyebrow: "Project Overview",
      title: "A bridge, not another isolated system.",
      problemTitle: "What it solves",
      problemBody1:
        "Many teams want recent, active data to stay in OpenSearch for mature querying and ecosystem support, but they do not want to keep paying premium storage cost for months or years of historical data.",
      problemBody2:
        "oqbridge does not try to replace your existing platform. It adds a thin, transparent layer in front of it, so hot/cold tiering becomes infrastructure behavior instead of application burden.",
      principlesTitle: "Design principles",
      principle1:
        "Keep the OpenSearch-compatible interface to lower migration cost.",
      principle2:
        "Make cold-data migration resumable and safe across multiple instances.",
      principle3:
        "Use clear time boundaries instead of fragile manual routing rules.",
      principle4:
        "Keep user authentication anchored in OpenSearch, not split across two systems.",
    },
    architecture: {
      eyebrow: "Architecture",
      title: "Separate proxy and migration so each data path stays clear.",
      nodeClient: "Client / Dashboards",
      nodeHot: "OpenSearch<br><span>hot tier</span>",
      nodeCold: "Quickwit<br><span>cold tier</span>",
      proxyTag: "Proxy",
      proxyBody:
        "It receives OpenSearch-style search requests, decides whether the query should hit the hot tier, cold tier, or both, and merges results when a cross-tier search is needed.",
      workerTag: "Worker",
      workerBody:
        "It is best deployed close to OpenSearch, reads historical data locally, and sends compressed batches into Quickwit through parallel sliced scroll workers.",
    },
    features: {
      eyebrow: "Capabilities",
      title: "A feature set shaped by real operational workloads.",
      query: {
        kicker: "Query Proxy",
        title: "On the query path",
        item1:
          "Transparent reverse proxy behavior with familiar OpenSearch APIs.",
        item2:
          "Time-range-aware routing to OpenSearch, Quickwit, or both backends in parallel.",
        item3:
          "Support for wildcard index patterns such as <code>logs-*</code>.",
        item4: "Cross-tier result merging for score-based queries.",
        item5: "Per-index timestamp field and retention overrides.",
      },
      migration: {
        kicker: "Migration Worker",
        title: "On the migration path",
        item1:
          "Wildcard resolution and automatic filtering of system indices.",
        item2:
          "Automatic Quickwit index creation before migration starts.",
        item3:
          "Parallel sliced scroll plus gzip compression for efficient transfer.",
        item4: "Checkpoint and resume support after interruptions.",
        item5:
          "Distributed locking for safe multi-instance coordination.",
      },
      production: {
        kicker: "Production Fit",
        title: "For production reality",
        item1:
          "Quickwit stays behind service credentials and remains invisible to end users.",
        item2:
          "Migration metrics are written into OpenSearch for dashboards and monitoring.",
        item3:
          "Supports both one-shot runs and cron-style daemon scheduling.",
        item4: "Cold retention and automatic cleanup are configurable.",
        item5:
          "Most of the adoption cost lives in configuration rather than application rewrites.",
      },
    },
    lifecycle: {
      eyebrow: "Lifecycle",
      title: "A clear timeline from ingest, to migration, to cleanup.",
      day0: {
        label: "Day 0",
        title: "Ingest",
        body: "Fresh data lands in OpenSearch for the best nearline search experience.",
      },
      day25: {
        label: "Day 25",
        title: "Prepare migration",
        body: "<code>oqbridge-migrate</code> begins handling data that is approaching the cold boundary.",
      },
      day30: {
        label: "Day 30",
        title: "Cold tier takes over",
        body: "Historical data is queryable from Quickwit, and can optionally be removed from OpenSearch.",
      },
      day395: {
        label: "Day 395",
        title: "Automatic cleanup",
        body: "Data older than <code>retention.cold_days</code> can be deleted automatically by Quickwit.",
      },
    },
    value: {
      eyebrow: "Why It Matters",
      title: "The real win is keeping platform change manageable.",
      devTitle: "For developers",
      devBody:
        "Most client code can keep talking to an OpenSearch-compatible interface without learning the hot/cold split underneath.",
      platformTitle: "For platform teams",
      platformBody:
        "You can separate “searches fast” from “stores cheaply,” letting OpenSearch focus on hot data while Quickwit handles long-tail history.",
      opsTitle: "For operations",
      opsBody:
        "Migration progress, resume safety, distributed locks, and metrics are already built in, which means fewer side scripts and fewer manual steps.",
    },
    start: {
      eyebrow: "Quick Start",
      title: "Keep the setup simple first, then tune it later.",
      buildTitle: "Build and run",
      deployTitle: "Recommended deployment",
      deployItem1:
        "Deploy <code>oqbridge-migrate</code> close to your OpenSearch nodes.",
      deployItem2:
        "Deploy <code>oqbridge</code> where your clients can reach it easily.",
      deployItem3: "Reuse one configuration format for both binaries.",
      deployItem4:
        "Start with the default 30-day / 365-day strategy, then refine per index.",
    },
    cta: {
      eyebrow: "GitHub Pages Ready",
      title: "Publish this directory as your project homepage.",
      body:
        "The <code>pages</code> directory is fully static and does not require a build tool. Once GitHub Pages is enabled for the repository, pushes to your main branch can deploy this site as the public project homepage.",
      primaryCta: "Open GitHub",
      secondaryCta: "English README",
      secondaryHref: "https://github.com/leonunix/oqbridge/blob/main/README.md",
    },
    footer: {
      tagline:
        "oqbridge · A lightweight proxy for OpenSearch and Quickwit hot/cold tiering.",
      built: "Built for GitHub Pages",
    },
  },
  zh: {
    meta: {
      title: "oqbridge | OpenSearch 与 Quickwit 的冷热分层桥",
      description:
        "oqbridge 是一个轻量级代理，用于在 OpenSearch 与 Quickwit 之间实现自动冷热数据分层、透明查询路由与历史数据迁移。",
      lang: "zh-CN",
    },
    brand: {
      ariaLabel: "oqbridge 首页",
    },
    nav: {
      ariaLabel: "主导航",
      overview: "项目概览",
      architecture: "架构",
      features: "能力",
      lifecycle: "生命周期",
      start: "快速开始",
    },
    hero: {
      eyebrow: "OpenSearch + Quickwit / 冷热分层代理",
      titleHtml:
        '让热数据留在 <span>OpenSearch</span>，让冷数据安静地去 <span>Quickwit</span>。',
      text:
        "oqbridge 是一个面向日志与时序检索场景的轻量级桥接层。它把用户查询入口保持在 OpenSearch 兼容接口之上，同时把历史数据自动迁移到 Quickwit，以更低的存储成本承接更长周期的数据保留需求。",
      primaryCta: "查看 GitHub",
      docsCta: "查看文档",
      docsHref: "https://github.com/leonunix/oqbridge/blob/main/README_zh.md",
      point1: "统一查询入口，不强迫业务理解双引擎差异。",
      point2: "自动冷热分层，降低长期日志保留成本。",
      point3: "迁移与代理解耦，适合渐进式接入生产环境。",
    },
    status: {
      label: "核心定位",
      chip: "轻量",
      s1: "独立二进制",
      s2: "默认热数据窗口",
      s3: "默认冷数据保留",
      s4: "OpenSearch 兼容代理",
    },
    overview: {
      eyebrow: "项目概览",
      title: "一个桥，而不是另一个孤立系统。",
      problemTitle: "解决的问题",
      problemBody1:
        "许多团队希望把最近的活跃数据保留在 OpenSearch 里，以获得成熟的检索能力与生态支持，但又不愿意为数月甚至数年的历史数据持续支付高昂存储成本。",
      problemBody2:
        "oqbridge 的思路不是替换已有平台，而是在现有查询入口前增加一层足够轻、足够透明的桥接能力，让冷热分层成为基础设施细节，而不是业务负担。",
      principlesTitle: "设计原则",
      principle1: "保留 OpenSearch 兼容接口，降低改造成本。",
      principle2: "让冷数据迁移具备断点续传与多实例安全。",
      principle3: "用清晰的时间边界驱动路由，而不是复杂手工规则。",
      principle4: "把用户认证继续放在 OpenSearch，避免双份账号体系。",
    },
    architecture: {
      eyebrow: "架构",
      title: "代理与迁移分离，读写路径各自清晰。",
      nodeClient: "客户端 / Dashboards",
      nodeHot: "OpenSearch<br><span>热层</span>",
      nodeCold: "Quickwit<br><span>冷层</span>",
      proxyTag: "代理",
      proxyBody:
        "它接住 OpenSearch 风格的搜索请求，判断查询应该命中热层、冷层还是双路并发，并在跨层查询时负责结果合并。",
      workerTag: "迁移器",
      workerBody:
        "它更适合部署在 OpenSearch 附近，本地读取历史数据，再通过并行 sliced scroll worker 和压缩传输把数据送入 Quickwit。",
    },
    features: {
      eyebrow: "能力",
      title: "围绕真实运维场景设计的能力组合。",
      query: {
        kicker: "查询代理",
        title: "查询路径",
        item1: "透明反向代理，尽量保持熟悉的 OpenSearch API 使用方式。",
        item2:
          "基于时间范围智能路由到 OpenSearch、Quickwit 或双路并发。",
        item3: "支持 <code>logs-*</code> 这类通配符索引模式。",
        item4: "支持基于 score 的跨冷热层结果合并。",
        item5: "支持按索引覆盖时间字段和保留策略。",
      },
      migration: {
        kicker: "迁移器",
        title: "迁移路径",
        item1: "自动解析通配符并过滤系统索引。",
        item2: "迁移开始前自动创建 Quickwit 索引。",
        item3: "并行 sliced scroll 加 gzip 压缩，提升迁移效率。",
        item4: "中断后可继续，支持 checkpoint 与 resume。",
        item5: "通过分布式锁实现多实例安全协同。",
      },
      production: {
        kicker: "生产可用性",
        title: "面向生产现实",
        item1: "Quickwit 由服务账号访问，对终端用户保持无感。",
        item2: "迁移指标写入 OpenSearch，便于监控与制作看板。",
        item3: "同时支持一次性执行和 cron 守护模式。",
        item4: "冷数据保留和自动清理均可配置。",
        item5: "接入成本尽量集中在配置，而不是业务代码改写。",
      },
    },
    lifecycle: {
      eyebrow: "生命周期",
      title: "从写入、迁移到清理，一条清楚的数据时间线。",
      day0: {
        label: "第 0 天",
        title: "数据写入",
        body: "新数据进入 OpenSearch，保持最好的近线检索体验。",
      },
      day25: {
        label: "第 25 天",
        title: "准备迁移",
        body: "<code>oqbridge-migrate</code> 开始处理接近冷却边界的数据。",
      },
      day30: {
        label: "第 30 天",
        title: "冷层接管",
        body: "历史数据可以从 Quickwit 查询，并可按需要从 OpenSearch 删除副本。",
      },
      day395: {
        label: "第 395 天",
        title: "自动清理",
        body: "超过 <code>retention.cold_days</code> 的数据可由 Quickwit 自动删除。",
      },
    },
    value: {
      eyebrow: "价值",
      title: "真正重要的是，让平台演进的代价可控。",
      devTitle: "对开发者",
      devBody:
        "大多数客户端代码仍然可以继续对着 OpenSearch 兼容接口发请求，而不用理解底层冷热分层细节。",
      platformTitle: "对平台团队",
      platformBody:
        "你可以把“查得快”和“存得下”拆开处理，让 OpenSearch 专注热数据，让 Quickwit 承担长尾历史归档。",
      opsTitle: "对运维",
      opsBody:
        "迁移进度、断点恢复、分布式锁和指标采集都已经内建，减少额外脚本和人为步骤。",
    },
    start: {
      eyebrow: "快速开始",
      title: "先保持简单跑起来，再逐步调优。",
      buildTitle: "构建与运行",
      deployTitle: "推荐部署方式",
      deployItem1: "将 <code>oqbridge-migrate</code> 部署在 OpenSearch 节点附近。",
      deployItem2: "将 <code>oqbridge</code> 部署在客户端易于访问的位置。",
      deployItem3: "两个二进制复用同一种配置格式。",
      deployItem4: "先用默认 30 天 / 365 天策略，再按索引细化。",
    },
    cta: {
      eyebrow: "GitHub Pages 就绪",
      title: "把这个目录发布成你的项目主页。",
      body:
        "<code>pages</code> 目录是纯静态结构，不依赖任何构建工具。只要仓库启用了 GitHub Pages，推送主分支后就可以把它部署成对外展示的项目主页。",
      primaryCta: "打开 GitHub",
      secondaryCta: "中文 README",
      secondaryHref: "https://github.com/leonunix/oqbridge/blob/main/README_zh.md",
    },
    footer: {
      tagline: "oqbridge · 一个面向 OpenSearch 与 Quickwit 冷热分层的轻量级代理。",
      built: "构建为 GitHub Pages",
    },
  },
};

function getValue(dictionary, path) {
  return path.split(".").reduce((value, key) => value?.[key], dictionary);
}

function applyLanguage(lang) {
  const dictionary = translations[lang] || translations.en;

  document.documentElement.lang = dictionary.meta.lang;
  document.title = dictionary.meta.title;

  const description = document.querySelector('meta[name="description"]');
  if (description) {
    description.setAttribute("content", dictionary.meta.description);
  }

  document.querySelectorAll("[data-i18n]").forEach((element) => {
    const key = element.dataset.i18n;
    const value = getValue(dictionary, key);
    if (typeof value === "string") {
      element.textContent = value;
    }
  });

  document.querySelectorAll("[data-i18n-html]").forEach((element) => {
    const key = element.dataset.i18nHtml;
    const value = getValue(dictionary, key);
    if (typeof value === "string") {
      element.innerHTML = value;
    }
  });

  document.querySelectorAll("[data-i18n-aria-label]").forEach((element) => {
    const key = element.dataset.i18nAriaLabel;
    const value = getValue(dictionary, key);
    if (typeof value === "string") {
      element.setAttribute("aria-label", value);
    }
  });

  document.querySelectorAll("[data-i18n-href]").forEach((element) => {
    const key = element.dataset.i18nHref;
    const value = getValue(dictionary, key);
    if (typeof value === "string") {
      element.setAttribute("href", value);
    }
  });

  document.querySelectorAll(".lang-button").forEach((button) => {
    const isActive = button.dataset.lang === lang;
    button.classList.toggle("is-active", isActive);
    button.setAttribute("aria-pressed", String(isActive));
  });
}

const revealElements = document.querySelectorAll(".reveal");

const observer = new IntersectionObserver(
  (entries) => {
    entries.forEach((entry) => {
      if (!entry.isIntersecting) {
        return;
      }

      entry.target.classList.add("is-visible");
      observer.unobserve(entry.target);
    });
  },
  {
    threshold: 0.16,
    rootMargin: "0px 0px -10% 0px",
  }
);

revealElements.forEach((element) => observer.observe(element));

const savedLanguage = localStorage.getItem("oqbridge-language");
const initialLanguage = savedLanguage && translations[savedLanguage] ? savedLanguage : "en";
applyLanguage(initialLanguage);

document.querySelectorAll(".lang-button").forEach((button) => {
  button.addEventListener("click", () => {
    const nextLanguage = button.dataset.lang;
    if (!translations[nextLanguage]) {
      return;
    }

    localStorage.setItem("oqbridge-language", nextLanguage);
    applyLanguage(nextLanguage);
  });
});

const yearNode = document.getElementById("year");
if (yearNode) {
  yearNode.textContent = new Date().getFullYear();
}
