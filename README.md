# ✈ Airlines Intelligence Dashboard

A comprehensive US domestic airline market intelligence dashboard built as a single self-contained HTML file. No server, no dependencies, no login required.

**🌐 Live:** [https://amitb641.github.io/airlines-dashboard](https://amitb641.github.io/airlines-dashboard)

---

## What's Inside

6 analytical tabs + 7 carrier deep-dives, all with "So What" verdict boxes:

| Tab | Coverage |
|-----|----------|
| **Market Overview** | 921M passengers, load factors, fare trends, demand recovery vs 2019 |
| **Financial Performance** | TRASM vs CASM-ex, operating margins, unit economics scorecard (8 carriers) |
| **Loyalty & Credit Cards** | $20.5B loyalty ecosystem, co-brand deal table, credit card opportunity analysis, marketing channel breakdown |
| **Airport Intelligence** | Animated live route map, HHI concentration, Sun Belt growth, hub dominance cards |
| **Carrier Dynamics** | Switchable deep-dives: Delta · United · American · Southwest · JetBlue · Hawaiian/Alaska · New Entrants |
| **Market Opportunity** | Scored unserved markets, Spirit vacuum analysis, Breeze growth |

---

## Carrier Coverage

| Carrier | Ticker | Verdict |
|---------|--------|---------|
| Delta Air Lines | DAL | ⬆ Clear Winner — premium > main cabin, $7B Amex deal |
| United Airlines | UAL | ⬆ Strong Momentum — world's largest fleet, United Next |
| American Airlines | AAL | ⚠ Recovery Story — NDC backfire, 5.1× leverage |
| Southwest Airlines | LUV | ↻ Forced Transformation — Elliott activist, assigned seats Jan 2026 |
| JetBlue Airways | JBLU | ⬇ Distressed — 6.8× leverage, TrueBlue expires 2026 |
| Hawaiian → Alaska | ALK | ↔ Integrating — Pacific moat, Atmos Rewards Oct 2025 |
| Breeze Airways | — | ⬆ Disruptor — 87% routes uncontested, +78% revenue FY24 |
| Spirit Airlines | — | ⚠ Chapter 22 — fleet 214→94, West Coast exited |

---

## Data Sources

- **BTS T-100** — Domestic segment traffic and capacity
- **DOT O&D Survey** — Fare data and city-pair passenger volumes
- **SEC Form 10-K** — Annual financial filings (revenue, margins, debt)
- **OAG** — Schedule data, seat share, capacity by day-of-week
- **EIA** — Jet fuel price series (Gulf Coast)
- **FAA / ACI-NA** — Airport passenger counts
- **Airlines for America (A4A)** — Industry aggregate data
- **Oliver Wyman** — Pilot shortage projections
- **Cirium / Aviation A2Z** — Fleet and route analytics

*All data as of March 2025. For analytical purposes only.*

---

## Tech Stack

- Pure HTML + CSS + JavaScript — zero build step, zero dependencies
- [Chart.js 4.4.1](https://www.chartjs.org/) — all charts
- Canvas API — animated flight particle route map
- Google Fonts — Syne, JetBrains Mono, Inter
- Deployed via GitHub Pages (artifact-based deployment from `main` branch)

---

## Local Use

Just open `index.html` in any browser. No server needed.

```bash
git clone https://github.com/amitb641/airlines-dashboard.git
open airlines-dashboard/index.html
```

---

*Built with Claude · Last updated March 2025*
