"""
Data Processor Module

Handles cleaning, filtering, and preprocessing of Reddit data for persona generation.
"""

import logging
import re
from typing import Dict, List, Any, Optional
from datetime import datetime
import json

from config import PERSONA_CONFIG


class DataProcessor:
    """Processes and cleans Reddit data for persona analysis."""
    
    def __init__(self, verbose: bool = False):
        """Initialize the data processor."""
        self.verbose = verbose
        self.logger = logging.getLogger(__name__)
        
    def process_user_data(self, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process and clean user data for persona generation.
        
        Args:
            user_data: Raw user data from Reddit scraper
            
        Returns:
            Processed and cleaned user data
        """
        self.logger.info("Processing user data...")
        
        processed_data = {
            'username': user_data.get('username'),
            'account_age_days': user_data.get('account_age_days', 0),
            'karma': user_data.get('karma', {}),
            'posts': [],
            'comments': [],
            'subreddits': [],
            'statistics': {}
        }
        
        # Process posts
        processed_data['posts'] = self._process_posts(user_data.get('posts', []))
        
        # Process comments
        processed_data['comments'] = self._process_comments(user_data.get('comments', []))
        
        # Process subreddits
        processed_data['subreddits'] = self._process_subreddits(user_data.get('subreddits', []))
        
        # Generate statistics
        processed_data['statistics'] = self._generate_statistics(processed_data)
        
        self.logger.info(f"Processed {len(processed_data['posts'])} posts and "
                        f"{len(processed_data['comments'])} comments")
        
        return processed_data
    
    def _process_posts(self, posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process and clean posts data."""
        processed_posts = []
        
        for post in posts:
            # Clean and validate post
            cleaned_post = self._clean_post(post)
            
            if cleaned_post and self._is_valid_content(cleaned_post.get('content', '')):
                processed_posts.append(cleaned_post)
        
        # Sort by creation time (newest first)
        processed_posts.sort(key=lambda x: x.get('created_utc', 0), reverse=True)
        
        return processed_posts
    
    def _process_comments(self, comments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process and clean comments data."""
        processed_comments = []
        
        for comment in comments:
            # Clean and validate comment
            cleaned_comment = self._clean_comment(comment)
            
            if cleaned_comment and self._is_valid_content(cleaned_comment.get('content', '')):
                processed_comments.append(cleaned_comment)
        
        # Sort by creation time (newest first)
        processed_comments.sort(key=lambda x: x.get('created_utc', 0), reverse=True)
        
        return processed_comments
    
    def _process_subreddits(self, subreddits: List[str]) -> List[Dict[str, Any]]:
        """Process subreddit data and add metadata."""
        processed_subreddits = []
        
        for subreddit in subreddits:
            if subreddit:
                subreddit_data = {
                    'name': subreddit,
                    'category': self._categorize_subreddit(subreddit),
                    'description': self._get_subreddit_description(subreddit)
                }
                processed_subreddits.append(subreddit_data)
        
        return processed_subreddits
    
    def _clean_post(self, post: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Clean and validate a single post."""
        try:
            cleaned_post = {
                'id': post.get('id'),
                'title': self._clean_text(post.get('title', '')),
                'content': self._clean_text(post.get('content', '')),
                'subreddit': post.get('subreddit', ''),
                'created_utc': post.get('created_utc', 0),
                'score': post.get('score', 0),
                'num_comments': post.get('num_comments', 0),
                'url': post.get('url', ''),
                'upvote_ratio': post.get('upvote_ratio', 0),
                'type': 'post'
            }
            
            # Add sentiment analysis if configured
            if PERSONA_CONFIG['analyze_sentiment']:
                cleaned_post['sentiment'] = self._analyze_sentiment(
                    cleaned_post['title'] + ' ' + cleaned_post['content']
                )
            
            return cleaned_post
            
        except Exception as e:
            self.logger.error(f"Error cleaning post: {e}")
            return None
    
    def _clean_comment(self, comment: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Clean and validate a single comment."""
        try:
            cleaned_comment = {
                'id': comment.get('id'),
                'content': self._clean_text(comment.get('content', '')),
                'subreddit': comment.get('subreddit', ''),
                'created_utc': comment.get('created_utc', 0),
                'score': comment.get('score', 0),
                'url': comment.get('url', ''),
                'parent_post_title': comment.get('parent_post_title', ''),
                'type': 'comment'
            }
            
            # Add sentiment analysis if configured
            if PERSONA_CONFIG['analyze_sentiment']:
                cleaned_comment['sentiment'] = self._analyze_sentiment(
                    cleaned_comment['content']
                )
            
            return cleaned_comment
            
        except Exception as e:
            self.logger.error(f"Error cleaning comment: {e}")
            return None
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text content."""
        if not text:
            return ''
        
        # Remove URLs
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
        
        # Remove Reddit markup
        text = re.sub(r'/u/\w+', '', text)  # Remove username mentions
        text = re.sub(r'/r/\w+', '', text)  # Remove subreddit mentions
        text = re.sub(r'\*\*(.*?)\*\*', r'\1', text)  # Bold text
        text = re.sub(r'\*(.*?)\*', r'\1', text)  # Italic text
        text = re.sub(r'~~(.*?)~~', r'\1', text)  # Strikethrough
        text = re.sub(r'&gt;', '>', text)  # HTML entities
        text = re.sub(r'&lt;', '<', text)
        text = re.sub(r'&amp;', '&', text)
        
        # Clean up whitespace
        text = ' '.join(text.split())
        
        return text.strip()
    
    def _is_valid_content(self, content: str) -> bool:
        """Check if content is valid for analysis."""
        if not content:
            return False
        
        # Check minimum length
        if len(content) < PERSONA_CONFIG['min_content_length']:
            return False
        
        # Skip deleted/removed content
        if content.lower() in ['[deleted]', '[removed]', 'deleted', 'removed']:
            return False
        
        # Skip very short or spam-like content
        if len(content.split()) < 3:
            return False
        
        return True
    
    def _categorize_subreddit(self, subreddit: str) -> str:
        """Categorize subreddit by type."""
        # Simple categorization - in a real implementation, you'd use a more comprehensive mapping
        categories = {
            'technology': ['programming', 'technology', 'coding', 'python', 'javascript', 'webdev'],
            'gaming': ['gaming', 'games', 'nintendo', 'xbox', 'playstation', 'steam'],
            'lifestyle': ['food', 'cooking', 'fitness', 'fashion', 'relationships'],
            'entertainment': ['movies', 'music', 'television', 'books', 'netflix'],
            'news': ['news', 'worldnews', 'politics', 'coronavirus'],
            'educational': ['askreddit', 'explainlikeimfive', 'todayilearned', 'science'],
            'hobby': ['diy', 'crafts', 'gardening', 'photography', 'art'],
            'sports': ['sports', 'soccer', 'basketball', 'football', 'baseball'],
            'finance': ['personalfinance', 'investing', 'cryptocurrency', 'stocks'],
            'career': ['jobs', 'career', 'cscareerquestions', 'resumes']
        }
        
        subreddit_lower = subreddit.lower()
        
        for category, keywords in categories.items():
            if any(keyword in subreddit_lower for keyword in keywords):
                return category
        
        return 'other'
    
    def _get_subreddit_description(self, subreddit: str) -> str:
        """Get a brief description of what the subreddit is about."""
        # Simple descriptions - in a real implementation, you'd fetch from Reddit API
        descriptions = {
            'programming': 'Programming and software development discussions',
            'askreddit': 'Ask and answer thought-provoking questions',
            'gaming': 'General gaming discussions and news',
            'technology': 'Technology news and discussions',
            'news': 'Current news and events',
            'food': 'Food, cooking, and recipes',
            'fitness': 'Health, fitness, and exercise'
        }
        
        return descriptions.get(subreddit.lower(), f'Discussions about {subreddit}')
    
    def _analyze_sentiment(self, text: str) -> str:
        """Simple sentiment analysis."""
        # This is a simplified implementation
        # In a real application, you'd use a proper sentiment analysis library
        
        positive_words = ['good', 'great', 'amazing', 'awesome', 'love', 'like', 'happy', 'excellent']
        negative_words = ['bad', 'terrible', 'awful', 'hate', 'dislike', 'sad', 'angry', 'frustrated']
        
        text_lower = text.lower()
        
        positive_count = sum(1 for word in positive_words if word in text_lower)
        negative_count = sum(1 for word in negative_words if word in text_lower)
        
        if positive_count > negative_count:
            return 'positive'
        elif negative_count > positive_count:
            return 'negative'
        else:
            return 'neutral'
    
    def _generate_statistics(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate statistical summary of the data."""
        posts = data.get('posts', [])
        comments = data.get('comments', [])
        
        # Calculate posting patterns
        post_scores = [post.get('score', 0) for post in posts]
        comment_scores = [comment.get('score', 0) for comment in comments]
        
        # Calculate activity by subreddit
        subreddit_activity = {}
        for post in posts:
            subreddit = post.get('subreddit', '')
            subreddit_activity[subreddit] = subreddit_activity.get(subreddit, 0) + 1
        
        for comment in comments:
            subreddit = comment.get('subreddit', '')
            subreddit_activity[subreddit] = subreddit_activity.get(subreddit, 0) + 1
        
        # Sort subreddits by activity
        top_subreddits = sorted(subreddit_activity.items(), key=lambda x: x[1], reverse=True)[:10]
        
        statistics = {
            'total_posts': len(posts),
            'total_comments': len(comments),
            'average_post_score': sum(post_scores) / len(post_scores) if post_scores else 0,
            'average_comment_score': sum(comment_scores) / len(comment_scores) if comment_scores else 0,
            'top_subreddits': top_subreddits,
            'most_active_subreddit': top_subreddits[0][0] if top_subreddits else None,
            'subreddit_diversity': len(subreddit_activity),
            'content_length_avg': self._calculate_average_content_length(posts + comments)
        }
        
        return statistics
    
    def _calculate_average_content_length(self, items: List[Dict[str, Any]]) -> float:
        """Calculate average content length."""
        total_length = 0
        count = 0
        
        for item in items:
            content = item.get('content', '')
            if content:
                total_length += len(content)
                count += 1
        
        return total_length / count if count > 0 else 0