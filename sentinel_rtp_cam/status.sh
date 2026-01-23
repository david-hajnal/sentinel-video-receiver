#!/usr/bin/env bash
#
# status.sh - Quick status check for sentinel_rtp_cam
#
# Usage:
#   sudo ./status.sh [--logs N]
#
set -euo pipefail

readonly BINARY_NAME="sentinel_rtp_cam"
readonly INSTALL_DIR="/usr/local/bin"
readonly CONFIG_DIR="/etc/${BINARY_NAME}"
readonly STATE_DIR="/var/lib/${BINARY_NAME}"

LOG_LINES=50

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --logs)
            LOG_LINES="$2"
            shift 2
            ;;
        *)
            echo "Unknown argument: $1"
            echo "Usage: $0 [--logs N]"
            exit 1
            ;;
    esac
done

echo -e "${BLUE}=== Sentinel RTP Camera Status ===${NC}"
echo ""

# Binary version
echo -e "${GREEN}Binary:${NC}"
if [[ -x "${INSTALL_DIR}/${BINARY_NAME}" ]]; then
    echo "  Location: ${INSTALL_DIR}/${BINARY_NAME}"
    version=$("${INSTALL_DIR}/${BINARY_NAME}" --version 2>/dev/null | head -n1) || version="unknown"
    echo "  Version: $version"
    
    if [[ -f "${INSTALL_DIR}/${BINARY_NAME}.prev" ]]; then
        echo "  Backup available: ${INSTALL_DIR}/${BINARY_NAME}.prev"
    fi
else
    echo -e "  ${RED}Not installed${NC}"
fi
echo ""

# Service status
echo -e "${GREEN}Service Status:${NC}"
if systemctl is-enabled --quiet "$BINARY_NAME" 2>/dev/null; then
    echo "  Enabled: yes"
else
    echo "  Enabled: no"
fi

if systemctl is-active --quiet "$BINARY_NAME" 2>/dev/null; then
    echo -e "  Active: ${GREEN}running${NC}"
    
    # Get process info
    pid=$(systemctl show "$BINARY_NAME" --property MainPID --value)
    if [[ "$pid" != "0" ]]; then
        echo "  PID: $pid"
        
        # Memory usage
        mem=$(ps -o rss= -p "$pid" 2>/dev/null | awk '{printf "%.1f MB", $1/1024}')
        echo "  Memory: $mem"
        
        # CPU usage (approximate)
        cpu=$(ps -o %cpu= -p "$pid" 2>/dev/null | awk '{print $1}')
        echo "  CPU: ${cpu}%"
    fi
else
    echo -e "  Active: ${RED}stopped${NC}"
fi

# Uptime
if systemctl is-active --quiet "$BINARY_NAME"; then
    uptime=$(systemctl show "$BINARY_NAME" --property ActiveEnterTimestamp --value)
    echo "  Started: $uptime"
fi
echo ""

# Configuration
echo -e "${GREEN}Configuration:${NC}"
if [[ -f "${CONFIG_DIR}/env" ]]; then
    echo "  Config file: ${CONFIG_DIR}/env"
    
    # Extract key settings (without passwords)
    camera_id=$(grep "^CAMERA_ID=" "${CONFIG_DIR}/env" 2>/dev/null | cut -d= -f2) || camera_id="not set"
    server_url=$(grep "^SERVER_BASE_URL=" "${CONFIG_DIR}/env" 2>/dev/null | cut -d= -f2) || server_url="not set"
    
    echo "  Camera ID: $camera_id"
    echo "  Server URL: $server_url"
else
    echo -e "  ${YELLOW}Config file not found${NC}"
fi
echo ""

# Storage
echo -e "${GREEN}Storage:${NC}"
if [[ -d "$STATE_DIR" ]]; then
    du_output=$(du -sh "$STATE_DIR" 2>/dev/null | awk '{print $1}')
    echo "  Data directory: $STATE_DIR"
    echo "  Size: $du_output"
    
    if [[ -d "$STATE_DIR/clips" ]]; then
        clip_count=$(find "$STATE_DIR/clips" -name "*.mp4" 2>/dev/null | wc -l)
        echo "  Clips: $clip_count files"
    fi
else
    echo -e "  ${YELLOW}Data directory not found${NC}"
fi
echo ""

# Recent logs
echo -e "${GREEN}Recent Logs (last $LOG_LINES lines):${NC}"
journalctl -u "$BINARY_NAME" -n "$LOG_LINES" --no-pager
echo ""

# Quick health indicators
echo -e "${GREEN}Health Indicators:${NC}"
recent_errors=$(journalctl -u "$BINARY_NAME" --since "5 minutes ago" -p err --no-pager 2>/dev/null | wc -l)
if [[ $recent_errors -gt 0 ]]; then
    echo -e "  ${RED}⚠ $recent_errors error(s) in last 5 minutes${NC}"
else
    echo -e "  ${GREEN}✓ No errors in last 5 minutes${NC}"
fi

if systemctl is-active --quiet "$BINARY_NAME"; then
    restarts=$(systemctl show "$BINARY_NAME" --property NRestarts --value)
    echo "  Restarts: $restarts"
fi
echo ""
