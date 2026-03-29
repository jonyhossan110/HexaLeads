# HexaLeads

```text
██╗  ██╗███████╗██╗  ██╗ █████╗ ██╗     ███████╗ █████╗ ██████╗ ███████╗
██║  ██║██╔════╝╚██╗██╔╝██╔══██╗██║     ██╔════╝██╔══██╗██╔══██╗██╔════╝
███████║█████╗   ╚███╔╝ ███████║██║     █████╗  ███████║██║  ██║███████╗
██╔══██║██╔══╝   ██╔██╗ ██╔══██║██║     ██╔══╝  ██╔══██║██║  ██║╚════██║
██║  ██║███████╗██╔╝ ██╗██║  ██║███████╗███████║██║  ██║██████╔╝███████║
╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝╚══════╝╚══════╝╚═╝  ╚═╝╚═════╝ ╚═════╝

    🤖 AI-Powered Lead Discovery for Cybersecurity Professionals 🤖
    Created by: Md. Jony Hassain (HexaCyberLab)
    LinkedIn:   https://www.linkedin.com/in/md-jony-hassain
    Version:    1.0.0 Pro
```

[![By HexaCyberLab](https://img.shields.io/badge/By-HexaCyberLab-111?style=for-the-badge&logo=shield)](https://www.linkedin.com/in/md-jony-hassain)
[![Created by Md. Jony Hassain](https://img.shields.io/badge/Created%20by-Md.%20Jony%20Hassain-0f0f0f?style=for-the-badge)](https://www.linkedin.com/in/md-jony-hassain)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Profile-0A66C2?style=for-the-badge&logo=linkedin)](https://www.linkedin.com/in/md-jony-hassain)
[![GitHub](https://img.shields.io/badge/GitHub-HexaLeads-181717?style=for-the-badge&logo=github)](https://github.com/HexaCyberLab/HexaLeads)

## Overview

HexaLeads is a professional cybersecurity lead discovery platform built by HexaCyberLab. It fuses automated scraping, browser-based reconnaissance, OSINT enrichment, and intelligent scoring into a polished agency-grade workflow.

## Core Capabilities

- AI-driven lead discovery for cybersecurity service providers
- Google Maps and website analysis
- Telegram command-and-control for remote lead hunting
- Browser display console with dark theme branding
- Branded Excel reports with metadata and agency attribution

## Installation

1. Install Python dependencies:
   ```bash
   python -m pip install -r requirements.txt
   ```
2. Set the Telegram bot token:
   ```powershell
   $env:TELEGRAM_BOT_TOKEN = "<your-token>"
   ```
3. Run the application:
   ```bash
   python main.py
   ```

## Telegram Bot

Once started, the HexaLeads bot responds with agency-branded messages and signature footers.

Supported commands:

- `/start` — Initialize the bot and register your chat
- `/help` — Show the available command list
- `/scrape <country> <city> <category>` — Begin the lead generation workflow
- `/status` — Check scraping progress
- `/download` — Retrieve the completed Excel report

## Browser Display

The browser display window launches with a HexaCyberLab dark theme and status footer.

- Window title: `HexaLeads | HexaCyberLab Edition`
- Status bar: `Created by Md. Jony Hassain | HexaCyberLab`
- Footer label: `Created by Md. Jony Hassain | linkedin.com/in/md-jony-hassain`

## Excel Reports

Reports now include:

- Branded report title: `HexaLeads Report — HexaCyberLab`
- Metadata worksheet with creator, agency, LinkedIn, generation date, and region details
- Agency attribution embedded in report metadata

## Project Structure

- `bot/` — Telegram bot and command logic
- `display/` — Browser display UI
- `exporter/` — Excel generation and file handling
- `hunter/` — Scraping and job orchestration
- `src/` — Core scraping, analysis, OSINT, and scoring modules

## Contact

Md. Jony Hassain | HexaCyberLab

- LinkedIn: https://www.linkedin.com/in/md-jony-hassain
- GitHub: https://github.com/jonyhossan110/HexaLeads
- Website: https://hexacyberlab.com

---

© 2026 HexaCyberLab — Md. Jony Hassain
