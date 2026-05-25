#!/bin/bash
#
# SilverM-quant VPS 一键部署脚本
# 适用于 Ubuntu 20.04/22.04/24.04
#
# 使用方法:
#   chmod +x deploy.sh
#   ./deploy.sh
#
# 部署内容:
#   - A股量化系统 (端口 5001)
#   - 美股量化系统 (端口 5002)
#   - systemd 服务 (开机自启)
#   - 每日定时任务 (数据更新 + 信号扫描)
#

set -e

# ==================== 配置区 ====================
REPO_URL="git@github.com:snowimba/SilverM-quant.git"
BRANCH="feature/us-stock-and-fixes"
DEPLOY_DIR="/opt/silverm-quant"
PYTHON_VERSION="3.11"
NODE_VERSION="20"

# LLM 配置 (本地 proxy)
LLM_API_KEY="sk-wpDspQmxWfZbwFh56"
LLM_BASE_URL="http://127.0.0.1:8317/v1"
LLM_MODEL="gpt-5.5"

# 端口
ASTOCK_PORT=5001
USSTOCK_PORT=5002

# ==================== 颜色输出 ====================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $1"; }
warn() { echo -e "${YELLOW}[$(date '+%H:%M:%S')] WARNING:${NC} $1"; }
err() { echo -e "${RED}[$(date '+%H:%M:%S')] ERROR:${NC} $1"; exit 1; }

# ==================== 1. 系统依赖 ====================
log "安装系统依赖..."
sudo apt-get update -qq
sudo apt-get install -y -qq \
    python${PYTHON_VERSION} python${PYTHON_VERSION}-venv python${PYTHON_VERSION}-dev \
    git curl build-essential

# 安装 Node.js (用于构建前端)
if ! command -v node &>/dev/null; then
    log "安装 Node.js ${NODE_VERSION}..."
    curl -fsSL https://deb.nodesource.com/setup_${NODE_VERSION}.x | sudo -E bash -
    sudo apt-get install -y -qq nodejs
fi

log "Python: $(python${PYTHON_VERSION} --version)"
log "Node: $(node --version)"
log "npm: $(npm --version)"

# ==================== 2. 拉取代码 ====================
if [ -d "$DEPLOY_DIR" ]; then
    log "更新代码..."
    cd "$DEPLOY_DIR"
    git fetch origin
    git checkout "$BRANCH"
    git pull origin "$BRANCH"
else
    log "克隆代码..."
    sudo mkdir -p "$DEPLOY_DIR"
    sudo chown $(whoami):$(whoami) "$DEPLOY_DIR"
    git clone -b "$BRANCH" "$REPO_URL" "$DEPLOY_DIR"
    cd "$DEPLOY_DIR"
fi

# ==================== 3. Python 虚拟环境 ====================
log "创建 Python 虚拟环境..."
if [ ! -d ".venv" ]; then
    python${PYTHON_VERSION} -m venv .venv
fi
source .venv/bin/activate

log "安装 Python 依赖..."
pip install --upgrade pip -q
pip install -r requirements.txt -q
pip install yfinance -q

# ==================== 4. 构建前端 ====================
log "构建 A股前端..."
cd "$DEPLOY_DIR/frontend"
npm install --silent
npm run build

log "构建美股前端..."
cd "$DEPLOY_DIR/us-stock/frontend"
npm install --silent
npx vite build

cd "$DEPLOY_DIR"

# ==================== 5. 初始化数据库 ====================
log "初始化数据库..."
source .venv/bin/activate
python scripts/init_database.py --db data/Astock3.duckdb 2>/dev/null || true

# ==================== 6. 配置 .env ====================
log "写入 .env 配置..."
cat > "$DEPLOY_DIR/.env" << EOF
# LLM API 配置
DEEPSEEK_API_KEY=${LLM_API_KEY}
DEEPSEEK_BASE_URL=${LLM_BASE_URL}
LLM_PROVIDER=deepseek
LLM_MODEL=${LLM_MODEL}

# 数据源 (可选)
TUSHARE_TOKEN=
EOF

# ==================== 7. 创建 systemd 服务 ====================
log "创建 systemd 服务..."

# A股服务
sudo tee /etc/systemd/system/silverm-astock.service > /dev/null << EOF
[Unit]
Description=SilverM A-Stock Quant System
After=network.target

[Service]
Type=simple
User=$(whoami)
WorkingDirectory=${DEPLOY_DIR}
Environment=PATH=${DEPLOY_DIR}/.venv/bin:/usr/bin:/bin
ExecStart=${DEPLOY_DIR}/.venv/bin/python -c "from dashboard.app import app; app.run(host='0.0.0.0', port=${ASTOCK_PORT})"
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

# 美股服务
sudo tee /etc/systemd/system/silverm-usstock.service > /dev/null << EOF
[Unit]
Description=SilverM US-Stock Quant System
After=network.target

[Service]
Type=simple
User=$(whoami)
WorkingDirectory=${DEPLOY_DIR}/us-stock
Environment=PATH=${DEPLOY_DIR}/.venv/bin:/usr/bin:/bin
ExecStart=${DEPLOY_DIR}/.venv/bin/python dashboard/app.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable silverm-astock silverm-usstock
sudo systemctl restart silverm-astock silverm-usstock

# ==================== 8. 定时任务 ====================
log "配置定时任务..."

# A股: 每天16:30更新数据+扫描信号 (收盘后)
# 美股: 每天05:30更新数据+扫描信号 (美东收盘后, UTC+8)
CRON_SCRIPT="$DEPLOY_DIR/scripts/cron_update.sh"
cat > "$CRON_SCRIPT" << 'CRONEOF'
#!/bin/bash
# 每日数据更新 + 信号扫描
DEPLOY_DIR="/opt/silverm-quant"
source "$DEPLOY_DIR/.venv/bin/activate"
cd "$DEPLOY_DIR"

MARKET="$1"  # astock 或 usstock
DATE=$(date +%Y%m%d)
LOG_DIR="$DEPLOY_DIR/logs/cron"
mkdir -p "$LOG_DIR"

if [ "$MARKET" = "astock" ]; then
    echo "[$(date)] A股数据更新开始" >> "$LOG_DIR/astock_${DATE}.log"
    python scripts/workflow_scheduler.py --run --pipeline daily --force-source baostock >> "$LOG_DIR/astock_${DATE}.log" 2>&1
    echo "[$(date)] A股数据更新完成" >> "$LOG_DIR/astock_${DATE}.log"
elif [ "$MARKET" = "usstock" ]; then
    echo "[$(date)] 美股数据更新开始" >> "$LOG_DIR/usstock_${DATE}.log"
    cd "$DEPLOY_DIR/us-stock"
    python -c "
from data.fetcher import USStockFetcher
f = USStockFetcher()
f.update_daily_prices(period='1d')
" >> "$LOG_DIR/usstock_${DATE}.log" 2>&1
    python -c "
from signals.scanner import scan_signals
scan_signals(workers=4)
" >> "$LOG_DIR/usstock_${DATE}.log" 2>&1
    echo "[$(date)] 美股数据更新完成" >> "$LOG_DIR/usstock_${DATE}.log"
fi
CRONEOF
chmod +x "$CRON_SCRIPT"

# 写入 crontab
(crontab -l 2>/dev/null | grep -v "cron_update.sh"; echo "30 16 * * 1-5 $CRON_SCRIPT astock"; echo "30 5 * * 2-6 $CRON_SCRIPT usstock") | crontab -

# ==================== 9. 验证 ====================
log "等待服务启动..."
sleep 5

ASTOCK_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:${ASTOCK_PORT}/api/stats" 2>/dev/null || echo "000")
USSTOCK_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:${USSTOCK_PORT}/api/stats" 2>/dev/null || echo "000")

echo ""
echo "=========================================="
echo "  SilverM-quant 部署完成"
echo "=========================================="
echo ""

if [ "$ASTOCK_STATUS" = "200" ]; then
    echo -e "  A股系统:  ${GREEN}✅ 运行中${NC}  http://VPS_IP:${ASTOCK_PORT}"
else
    echo -e "  A股系统:  ${RED}❌ 启动失败${NC}  检查: journalctl -u silverm-astock"
fi

if [ "$USSTOCK_STATUS" = "200" ]; then
    echo -e "  美股系统: ${GREEN}✅ 运行中${NC}  http://VPS_IP:${USSTOCK_PORT}"
else
    echo -e "  美股系统: ${RED}❌ 启动失败${NC}  检查: journalctl -u silverm-usstock"
fi

echo ""
echo "  定时任务:"
echo "    A股: 每天 16:30 自动更新 (周一至周五)"
echo "    美股: 每天 05:30 自动更新 (周二至周六)"
echo ""
echo "  管理命令:"
echo "    sudo systemctl status silverm-astock"
echo "    sudo systemctl status silverm-usstock"
echo "    sudo systemctl restart silverm-astock"
echo "    journalctl -u silverm-astock -f"
echo ""
echo "  首次使用需要下载历史数据:"
echo "    打开 http://VPS_IP:${ASTOCK_PORT}/data-update"
echo "    选择 Baostock → 日线数据 → 开始更新"
echo "=========================================="
