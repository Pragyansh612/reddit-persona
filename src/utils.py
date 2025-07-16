"""
Utility functions for the Reddit Persona Generator.
"""

import logging
import os
import time
from typing import Dict, Any, Optional, List
from datetime import datetime
import json
import re
from urllib.parse import urlparse


def setup_logging(level: int = logging.INFO) -> None:
    """Set up logging configuration."""
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('persona_generator.log')
        ]
    )


def create_directories(directories: List[str]) -> None:
    """Create multiple directories if they don't exist."""
    for directory in directories:
        ensure_directory_exists(directory)


def ensure_directory_exists(directory: str) -> None:
    """Ensure that a directory exists, create it if it doesn't."""
    if not os.path.exists(directory):
        os.makedirs(directory)
        logging.info(f"Created directory: {directory}")


def validate_url(url: str) -> bool:
    """Validate if the URL is a valid Reddit user profile URL."""
    return validate_reddit_url(url)


def extract_username_from_url(url: str) -> Optional[str]:
    """Extract username from Reddit profile URL."""
    patterns = [
        r'reddit\.com/user/([^/]+)',
        r'reddit\.com/u/([^/]+)',
        r'old\.reddit\.com/user/([^/]+)',
        r'old\.reddit\.com/u/([^/]+)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    
    return None


def validate_reddit_url(url: str) -> bool:
    """Validate if the URL is a valid Reddit user profile URL."""
    try:
        parsed = urlparse(url)
        if not parsed.netloc:
            return False
            
        # Check if it's a Reddit domain
        reddit_domains = ['reddit.com', 'www.reddit.com', 'old.reddit.com']
        if parsed.netloc not in reddit_domains:
            return False
            
        # Check if it's a user profile URL
        path_patterns = [
            r'^/user/[^/]+/?$',
            r'^/u/[^/]+/?$'
        ]
        
        for pattern in path_patterns:
            if re.match(pattern, parsed.path):
                return True
                
        return False
        
    except Exception:
        return False


def format_timestamp(timestamp: float) -> str:
    """Format Unix timestamp to readable date string."""
    try:
        dt = datetime.fromtimestamp(timestamp)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, OSError):
        return 'Unknown'


def calculate_account_age(created_utc: float) -> str:
    """Calculate account age from creation timestamp."""
    try:
        created_date = datetime.fromtimestamp(created_utc)
        current_date = datetime.now()
        age_delta = current_date - created_date
        
        days = age_delta.days
        years = days // 365
        months = (days % 365) // 30
        
        if years > 0:
            return f"{years} year{'s' if years > 1 else ''}, {months} month{'s' if months > 1 else ''}"
        elif months > 0:
            return f"{months} month{'s' if months > 1 else ''}"
        else:
            return f"{days} day{'s' if days > 1 else ''}"
            
    except (ValueError, OSError):
        return 'Unknown'


def sanitize_filename(filename: str) -> str:
    """Sanitize filename by removing invalid characters."""
    # Remove invalid characters for Windows/Linux filenames
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    
    # Remove extra spaces and dots
    filename = re.sub(r'\s+', '_', filename)
    filename = re.sub(r'\.+', '.', filename)
    
    # Ensure filename is not empty
    if not filename or filename == '.':
        filename = 'unknown_user'
    
    return filename


def save_json(data: Dict[str, Any], filepath: str) -> None:
    """Save data to JSON file."""
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        logging.info(f"Data saved to {filepath}")
    except Exception as e:
        logging.error(f"Failed to save JSON to {filepath}: {e}")


def load_json(filepath: str) -> Optional[Dict[str, Any]]:
    """Load data from JSON file."""
    try:
        if os.path.exists(filepath):
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        else:
            logging.warning(f"File {filepath} does not exist")
            return None
    except Exception as e:
        logging.error(f"Failed to load JSON from {filepath}: {e}")
        return None


def rate_limit_handler(func):
    """Decorator to handle rate limiting with exponential backoff."""
    def wrapper(*args, **kwargs):
        max_retries = 3
        base_delay = 1
        
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if "rate limit" in str(e).lower() and attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    logging.warning(f"Rate limit hit, waiting {delay} seconds...")
                    time.sleep(delay)
                else:
                    raise e
    return wrapper


def progress_bar(current: int, total: int, prefix: str = "", suffix: str = "", length: int = 50) -> None:
    """Display a progress bar in the console."""
    if total == 0:
        return
        
    percent = (current / total) * 100
    filled_length = int(length * current // total)
    bar = '█' * filled_length + '-' * (length - filled_length)
    
    print(f'\r{prefix} |{bar}| {percent:.1f}% {suffix}', end='', flush=True)
    
    if current == total:
        print()  # New line when complete


def truncate_text(text: str, max_length: int = 100) -> str:
    """Truncate text to specified length with ellipsis."""
    if len(text) <= max_length:
        return text
    return text[:max_length - 3] + "..."


def clean_reddit_content(content: str) -> str:
    """Clean Reddit content by removing markdown and formatting."""
    if not content:
        return ""
    
    # Remove common Reddit markdown
    content = re.sub(r'\*\*(.*?)\*\*', r'\1', content)  # Bold
    content = re.sub(r'\*(.*?)\*', r'\1', content)  # Italic
    content = re.sub(r'~~(.*?)~~', r'\1', content)  # Strikethrough
    content = re.sub(r'`(.*?)`', r'\1', content)  # Code
    content = re.sub(r'\^(.*?)\^', r'\1', content)  # Superscript
    
    # Remove Reddit-specific formatting
    content = re.sub(r'&gt;', '>', content)  # Quote markers
    content = re.sub(r'&lt;', '<', content)
    content = re.sub(r'&amp;', '&', content)
    
    # Remove excessive whitespace
    content = re.sub(r'\s+', ' ', content)
    
    return content.strip()


def format_score(score: int) -> str:
    """Format score numbers for display."""
    if score >= 1000:
        return f"{score/1000:.1f}k"
    return str(score)


def get_file_size(filepath: str) -> str:
    """Get human-readable file size."""
    try:
        size = os.path.getsize(filepath)
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} TB"
    except OSError:
        return "Unknown"


def validate_config() -> bool:
    """Validate configuration settings."""
    try:
        from config import REDDIT_CONFIG, OPENAI_CONFIG
        
        required_reddit_keys = ['client_id', 'client_secret', 'user_agent']
        required_openai_keys = ['api_key']
        
        # Check Reddit config
        for key in required_reddit_keys:
            if not REDDIT_CONFIG.get(key) or REDDIT_CONFIG[key] == f'your_{key}_here':
                logging.error(f"Missing or invalid Reddit configuration: {key}")
                return False
        
        # Check OpenAI config
        for key in required_openai_keys:
            if not OPENAI_CONFIG.get(key) or OPENAI_CONFIG[key] == f'your_{key}_here':
                logging.error(f"Missing or invalid OpenAI configuration: {key}")
                return False
        
        return True
    except ImportError:
        logging.error("Configuration file not found or invalid")
        return False


def estimate_processing_time(num_posts: int, num_comments: int) -> str:
    """Estimate processing time based on content amount."""
    # Rough estimates based on API calls and processing
    post_time = num_posts * 0.1  # 0.1 seconds per post
    comment_time = num_comments * 0.05  # 0.05 seconds per comment
    llm_time = 10  # Fixed time for LLM processing
    
    total_seconds = post_time + comment_time + llm_time
    
    if total_seconds < 60:
        return f"{total_seconds:.0f} seconds"
    elif total_seconds < 3600:
        return f"{total_seconds/60:.1f} minutes"
    else:
        return f"{total_seconds/3600:.1f} hours"


def create_citation_url(post_id: str = None, comment_id: str = None, 
                       subreddit: str = None, username: str = None) -> str:
    """Create a proper Reddit URL for citations."""
    base_url = "https://www.reddit.com"
    
    if post_id:
        if subreddit:
            return f"{base_url}/r/{subreddit}/comments/{post_id}/"
        else:
            return f"{base_url}/comments/{post_id}/"
    elif comment_id:
        # For comments, we need the post ID too, but we'll use a simplified URL
        return f"{base_url}/comments/{comment_id}/"
    elif username:
        return f"{base_url}/user/{username}/"
    else:
        return base_url


def print_banner():
    """Print application banner."""
    banner = """
    ██████╗ ███████╗██████╗ ██████╗ ██╗████████╗    ██████╗ ███████╗██████╗ ███████╗ ██████╗ ███╗   ██╗ █████╗ 
    ██╔══██╗██╔════╝██╔══██╗██╔══██╗██║╚══██╔══╝    ██╔══██╗██╔════╝██╔══██╗██╔════╝██╔═══██╗████╗  ██║██╔══██╗
    ██████╔╝█████╗  ██║  ██║██║  ██║██║   ██║       ██████╔╝█████╗  ██████╔╝███████╗██║   ██║██╔██╗ ██║███████║
    ██╔══██╗██╔══╝  ██║  ██║██║  ██║██║   ██║       ██╔═══╝ ██╔══╝  ██╔══██╗╚════██║██║   ██║██║╚██╗██║██╔══██║
    ██║  ██║███████╗██████╔╝██████╔╝██║   ██║       ██║     ███████╗██║  ██║███████║╚██████╔╝██║ ╚████║██║  ██║
    ╚═╝  ╚═╝╚══════╝╚═════╝ ╚═════╝ ╚═╝   ╚═╝       ╚═╝     ╚══════╝╚═╝  ╚═╝╚══════╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝  ╚═╝
    
    ██████╗ ███████╗███╗   ██╗███████╗██████╗  █████╗ ████████╗ ██████╗ ██████╗ 
    ██╔════╝ ██╔════╝████╗  ██║██╔════╝██╔══██╗██╔══██╗╚══██╔══╝██╔═══██╗██╔══██╗
    ██║  ███╗█████╗  ██╔██╗ ██║█████╗  ██████╔╝███████║   ██║   ██║   ██║██████╔╝
    ██║   ██║██╔══╝  ██║╚██╗██║██╔══╝  ██╔══██╗██╔══██║   ██║   ██║   ██║██╔══██╗
    ╚██████╔╝███████╗██║ ╚████║███████╗██║  ██║██║  ██║   ██║   ╚██████╔╝██║  ██║
     ╚═════╝ ╚══════╝╚═╝  ╚═══╝╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝
    """
    print(banner)
    print("    Generate comprehensive user personas from Reddit profiles")
    print("    Using AI-powered analysis with proper citations")
    print("    " + "="*60)
    print()


def print_summary(username: str, total_posts: int, total_comments: int, 
                 processing_time: float, output_file: str):
    """Print processing summary."""
    print("\n" + "="*60)
    print("PROCESSING COMPLETE")
    print("="*60)
    print(f"Username: {username}")
    print(f"Posts analyzed: {total_posts}")
    print(f"Comments analyzed: {total_comments}")
    print(f"Processing time: {processing_time:.1f} seconds")
    print(f"Output file: {output_file}")
    print(f"File size: {get_file_size(output_file)}")
    print("="*60)


def handle_keyboard_interrupt():
    """Handle Ctrl+C gracefully."""
    print("\n\nOperation cancelled by user.")
    print("Exiting gracefully...")
    exit(0)


def display_help():
    """Display detailed help information."""
    help_text = """
Reddit Persona Generator - Help

USAGE:
    python main.py --url "https://www.reddit.com/user/username/" [OPTIONS]

REQUIRED ARGUMENTS:
    --url           Reddit user profile URL
                    Examples: 
                    https://www.reddit.com/user/kojied/
                    https://www.reddit.com/user/Hungry-Move-6603/

OPTIONAL ARGUMENTS:
    --max-posts     Maximum number of posts to scrape (default: 10)
    --max-comments  Maximum number of comments to scrape (default: 15)
    --output-dir    Output directory for persona files (default: ./output)
    --verbose       Enable verbose logging
    --debug         Enable debug mode
    --help          Show this help message

EXAMPLES:
    # Basic usage
    python main.py --url "https://www.reddit.com/user/kojied/"
    
    # With custom limits
    python main.py --url "https://www.reddit.com/user/kojied/" --max-posts 50 --max-comments 100
    
    # With verbose output
    python main.py --url "https://www.reddit.com/user/kojied/" --verbose
    
    # Custom output directory
    python main.py --url "https://www.reddit.com/user/kojied/" --output-dir "./my_personas"

CONFIGURATION:
    Before running, ensure you have configured:
    1. Reddit API credentials in config.py
    2. OpenAI API key in config.py
    
    See README.md for detailed setup instructions.

OUTPUT:
    The script generates:
    - A comprehensive user persona text file
    - Citations for each persona characteristic
    - Processing logs
    
For more information, visit: https://github.com/yourusername/reddit-persona-generator
"""
    print(help_text)