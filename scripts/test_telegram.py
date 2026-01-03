#!/usr/bin/env python3
"""
Test Telegram alerts independently
"""
import asyncio
import sys
import os
from dotenv import load_dotenv

# Load environment
load_dotenv()

async def test_telegram():
    """Test Telegram alerts"""
    from app.services.telegram_alerts import telegram_alerts
    
    print("🔍 Testing Telegram alerts...")
    print(f"Bot Token: {'SET' if os.getenv('TELEGRAM_BOT_TOKEN') else 'NOT SET'}")
    print(f"Chat ID: {'SET' if os.getenv('TELEGRAM_CHAT_ID') else 'NOT SET'}")
    
    if not telegram_alerts.enabled:
        print("❌ Telegram alerts are DISABLED")
        print("Please set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env")
        return False
    
    print("✅ Telegram alerts are ENABLED")
    print("Sending test alert...")
    
    # Test 1: Basic alert
    print("1. Testing basic alert...")
    success1 = await telegram_alerts.send_test_alert()
    
    if success1:
        print("   ✅ Basic alert sent successfully")
    else:
        print("   ❌ Failed to send basic alert")
    
    # Test 2: Trade alert
    print("2. Testing trade alert...")
    success2 = await telegram_alerts.send_trade_alert(
        action="EXECUTED",
        instrument="NIFTY23DEC21500CE",
        quantity=50,
        side="SELL",
        strategy="STRANGLE",
        reason="Delta hedge"
    )
    
    if success2:
        print("   ✅ Trade alert sent successfully")
    else:
        print("   ❌ Failed to send trade alert")
    
    # Test 3: Emergency alert
    print("3. Testing emergency alert...")
    success3 = await telegram_alerts.send_emergency_stop_alert(
        reason="Test emergency",
        triggered_by="TEST_SCRIPT"
    )
    
    if success3:
        print("   ✅ Emergency alert sent successfully")
    else:
        print("   ❌ Failed to send emergency alert")
    
    all_success = success1 and success2 and success3
    
    if all_success:
        print("\n🎉 ALL TELEGRAM TESTS PASSED!")
        print("Your alerts will be sent for:")
        print("  • Critical errors")
        print("  • Emergency stops")
        print("  • Trade executions")
        print("  • Capital breaches")
        print("  • Data quality issues")
    else:
        print("\n⚠️ Some Telegram tests failed")
        print("Check your bot token and chat ID")
    
    return all_success

if __name__ == "__main__":
    result = asyncio.run(test_telegram())
    sys.exit(0 if result else 1)
