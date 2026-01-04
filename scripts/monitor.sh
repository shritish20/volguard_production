#!/bin/bash
# Quick monitoring dashboard for terminal

echo "🎯 VolGuard Live Monitor"
echo "Press Ctrl+C to exit"
echo ""

while true; do
    clear
    echo "╔══════════════════════════════════════════════════════════╗"
    echo "║           VolGuard System Monitor                        ║"
    echo "║           $(date '+%Y-%m-%d %H:%M:%S')                              ║"
    echo "╚══════════════════════════════════════════════════════════╝"
    echo ""

    # System Status
    echo "📊 System Status"
    echo "────────────────"
    STATUS=$(curl -s http://localhost:8000/api/v1/supervisor/status 2>/dev/null)
    if [ $? -eq 0 ]; then
        echo "$STATUS" | jq -r '
            "Status:        \(.status)",
            "Environment:   \(.environment)",
            "Database:      \(.database)",
            "Kill Switch:   \(if .kill_switch_active then "🔴 ACTIVE" else "🟢 Inactive" end)"
        ' 2>/dev/null || echo "Status: API Responding"
    else
        echo "❌ API Not Responding"
    fi

    echo ""

    # Key Metrics
    echo "📈 Key Metrics (Last 5 sec)"
    echo "────────────────────────────"
    METRICS=$(curl -s http://localhost:8000/metrics 2>/dev/null)
    if [ $? -eq 0 ]; then
        echo "$METRICS" | grep -E 'volguard_(active_positions|net_delta|daily_pnl|available_margin|system_state)' | \
        awk '{
            if ($1 ~ /active_positions/) print "Active Positions:  " $2
            if ($1 ~ /net_delta/) print "Net Delta:         " $2
            if ($1 ~ /daily_pnl/) print "Daily PnL:         ₹" $2
            if ($1 ~ /available_margin/) print "Available Margin:  ₹" $2
            if ($1 ~ /system_state/) {
                state = $2
                if (state == 0) state_name = "NORMAL"
                else if (state == 1) state_name = "DEGRADED"
                else if (state == 2) state_name = "HALTED"
                else state_name = "EMERGENCY"
                print "System State:      " state_name
            }
        }'
    else
        echo "❌ Metrics Not Available"
    fi

    echo ""

    # Recent Errors
    echo "🚨 Recent Errors (Last 10)"
    echo "──────────────────────────"
    if [ -f "logs/volguard_errors_$(date +%Y%m%d).log" ]; then
        tail -10 logs/volguard_errors_$(date +%Y%m%d).log | \
        jq -r '.timestamp + " " + .level + " " + .message' 2>/dev/null || \
        tail -10 logs/volguard_errors_$(date +%Y%m%d).log
    else
        echo "No errors today ✅"
    fi

    echo ""
    echo "────────────────────────────────────────────────────────────"
    echo "Refreshing in 5 seconds... (Ctrl+C to exit)"

    sleep 5
done
