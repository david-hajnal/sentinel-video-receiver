#!/bin/bash
# Sentinel RTP Camera - Management Script
# Quick access to common management commands

set -e

SERVICE_NAME="sentinel_rtp_cam"
CONFIG_FILE="/etc/sentinel_rtp_cam/env"
CLIPS_DIR="/var/lib/sentinel_rtp_cam/clips"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

show_usage() {
    echo "Usage: $0 <command>"
    echo ""
    echo "Commands:"
    echo "  config       Edit configuration file"
    echo "  clips        List clips in storage directory"
    echo "  restart      Restart the service"
    echo "  stop         Stop the service"
    echo "  start        Start the service"
    echo "  status       Show service status"
    echo "  logs         Follow live logs"
    echo "  logs-recent  Show recent logs (last 50 lines)"
    echo "  clean        Delete all clips (with confirmation)"
    echo ""
    echo "Examples:"
    echo "  $0 config"
    echo "  $0 restart"
    echo "  $0 logs"
}

check_root() {
    if [[ $EUID -ne 0 ]]; then
        echo -e "${YELLOW}This command requires sudo. Re-running with sudo...${NC}"
        exec sudo "$0" "$@"
    fi
}

cmd_config() {
    check_root
    nano "$CONFIG_FILE"
}

cmd_clips() {
    check_root
    echo -e "${GREEN}Clips in $CLIPS_DIR:${NC}"
    ls -lh "$CLIPS_DIR" || echo "No clips found or directory empty"
    echo ""
    echo -e "${BLUE}Disk usage:${NC}"
    du -sh "$CLIPS_DIR" 2>/dev/null || echo "N/A"
    df -h "$CLIPS_DIR"
}

cmd_restart() {
    check_root
    echo -e "${YELLOW}Restarting $SERVICE_NAME...${NC}"
    systemctl restart "$SERVICE_NAME"
    sleep 2
    systemctl status "$SERVICE_NAME" --no-pager
}

cmd_stop() {
    check_root
    echo -e "${YELLOW}Stopping $SERVICE_NAME...${NC}"
    systemctl stop "$SERVICE_NAME"
    systemctl status "$SERVICE_NAME" --no-pager
}

cmd_start() {
    check_root
    echo -e "${GREEN}Starting $SERVICE_NAME...${NC}"
    systemctl start "$SERVICE_NAME"
    sleep 2
    systemctl status "$SERVICE_NAME" --no-pager
}

cmd_status() {
    check_root
    systemctl status "$SERVICE_NAME" --no-pager
}

cmd_logs() {
    check_root
    echo -e "${GREEN}Following logs for $SERVICE_NAME (Ctrl+C to exit)...${NC}"
    journalctl -u "$SERVICE_NAME" -f
}

cmd_logs_recent() {
    check_root
    echo -e "${GREEN}Recent logs for $SERVICE_NAME:${NC}"
    journalctl -u "$SERVICE_NAME" -n 50 --no-pager
}

cmd_clean() {
    check_root
    echo -e "${RED}WARNING: This will delete ALL clips in $CLIPS_DIR${NC}"
    read -p "Are you sure? Type 'yes' to confirm: " confirm
    if [[ "$confirm" == "yes" ]]; then
        echo -e "${YELLOW}Deleting all clips...${NC}"
        rm -f "$CLIPS_DIR"/*.mp4 "$CLIPS_DIR"/*.mp4.part
        echo -e "${GREEN}Done. Clips deleted.${NC}"
    else
        echo "Cancelled."
    fi
}

# Main command dispatcher
case "${1:-}" in
    config)
        cmd_config
        ;;
    clips|ls)
        cmd_clips
        ;;
    restart)
        cmd_restart
        ;;
    stop)
        cmd_stop
        ;;
    start)
        cmd_start
        ;;
    status)
        cmd_status
        ;;
    logs)
        cmd_logs
        ;;
    logs-recent|recent)
        cmd_logs_recent
        ;;
    clean)
        cmd_clean
        ;;
    help|--help|-h|"")
        show_usage
        exit 0
        ;;
    *)
        echo -e "${RED}Error: Unknown command '$1'${NC}"
        echo ""
        show_usage
        exit 1
        ;;
esac
