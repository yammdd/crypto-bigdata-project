#!/usr/bin/env python3
"""
Cache clearing utility for crypto dashboard
"""

import subprocess
import time

def clear_streamlit_cache():
    """Clear Streamlit container cache"""
    print("🔄 Clearing Streamlit cache...")
    
    try:
        # Restart Streamlit container
        result = subprocess.run([
            "docker", "compose", "restart", "streamlit"
        ], capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            print("✅ Streamlit cache cleared (container restarted)")
            return True
        else:
            print(f"❌ Failed to restart Streamlit: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error clearing Streamlit cache: {e}")
        return False

def clear_mongodb_cache():
    """Clear MongoDB cache by updating timestamps"""
    print("🔄 Clearing MongoDB cache...")
    
    try:
        # Run write_to_mongo to refresh data
        result = subprocess.run([
            "docker", "compose", "exec", "-T", "spark-master",
            "spark-submit", "/opt/spark/work-dir/batch/write_to_mongo.py"
        ], capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            print("✅ MongoDB cache cleared (data refreshed)")
            return True
        else:
            print(f"❌ Failed to refresh MongoDB: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error clearing MongoDB cache: {e}")
        return False

def main():
    print("🧹 Cache Clearing Utility")
    print("=" * 40)
    
    # Clear Streamlit cache
    if clear_streamlit_cache():
        print("⏳ Waiting 10 seconds for Streamlit to restart...")
        time.sleep(10)
    
    print("\n" + "=" * 40)
    
    # Clear MongoDB cache
    clear_mongodb_cache()
    
    print("\n" + "=" * 40)
    print("✅ Cache clearing complete!")
    print("\n🌐 Now visit http://localhost:8501")
    print("💡 Browser cache clearing tips:")
    print("   • Hard refresh: Ctrl+Shift+R (Windows) or Cmd+Shift+R (Mac)")
    print("   • Or try incognito/private mode")
    print("   • Or clear browser cache manually")
    print("🔄 Click 'Refresh Data' button on the dashboard")

if __name__ == "__main__":
    main()
