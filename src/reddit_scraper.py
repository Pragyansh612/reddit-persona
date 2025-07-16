"""
Reddit Scraper Module - Fixed Authentication Version

Handles scraping of Reddit user profiles, posts, and comments using PRAW with user authentication.
"""

import logging
import time
from typing import Dict, List, Optional, Any
import praw
from prawcore.exceptions import NotFound, Forbidden, ServerError, ResponseException
from datetime import datetime

from config import REDDIT_CONFIG, SCRAPING_CONFIG, ERROR_CONFIG


class RedditScraper:
    """Scrapes Reddit user profiles and extracts posts and comments."""
    
    def __init__(self, max_posts: int = 10, max_comments: int = 15, verbose: bool = False):
        """Initialize the Reddit scraper."""
        self.max_posts = max_posts
        self.max_comments = max_comments
        self.verbose = verbose
        self.logger = logging.getLogger(__name__)
        
        # Initialize Reddit API client
        self.reddit = self._initialize_reddit_client()
        
    def _initialize_reddit_client(self) -> praw.Reddit:
        """Initialize and return Reddit API client with authentication."""
        try:
            # Use authenticated access with username/password
            reddit = praw.Reddit(
                client_id=REDDIT_CONFIG['client_id'],
                client_secret=REDDIT_CONFIG['client_secret'],
                user_agent=REDDIT_CONFIG['user_agent'],
                username=REDDIT_CONFIG['username'],
                password=REDDIT_CONFIG['password']
            )
            
            # Test connection with authenticated access
            try:
                authenticated_user = reddit.user.me()
                if authenticated_user:
                    self.logger.info(f"Successfully connected to Reddit API as user: {authenticated_user.name}")
                else:
                    raise Exception("Authentication returned None")
            except Exception as e:
                self.logger.error(f"Authentication failed: {e}")
                # Don't continue with fallback - authentication is required
                raise Exception(f"Reddit authentication failed: {e}")
            
            return reddit
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Reddit client: {e}")
            raise
    
    def scrape_user_profile(self, username: str) -> Optional[Dict[str, Any]]:
        """
        Scrape a Reddit user's profile and return structured data.
        
        Args:
            username: Reddit username (without u/ prefix)
            
        Returns:
            Dictionary containing user data or None if failed
        """
        try:
            # Verify authentication first
            try:
                me = self.reddit.user.me()
                if not me:
                    self.logger.error("Not authenticated to Reddit API")
                    return None
            except Exception as e:
                self.logger.error(f"Authentication verification failed: {e}")
                return None
            
            # Get user object
            user = self.reddit.redditor(username)
            
            # Check if user exists and is accessible
            try:
                user_created = user.created_utc
                link_karma = user.link_karma
                comment_karma = user.comment_karma
                
                self.logger.info(f"Found user {username} - Link Karma: {link_karma}, Comment Karma: {comment_karma}")
                
            except (NotFound, Forbidden) as e:
                self.logger.error(f"User {username} not found or not accessible: {e}")
                return None
            except Exception as e:
                self.logger.error(f"Error accessing user {username}: {e}")
                return None
            
            self.logger.info(f"Scraping profile for user: {username}")
            
            # Collect user data
            user_data = {
                'username': username,
                'created_utc': user_created,
                'account_age_days': (datetime.now().timestamp() - user_created) / 86400,
                'posts': [],
                'comments': [],
                'subreddits': [],  # Changed from set to list
                'karma': {
                    'post': link_karma,
                    'comment': comment_karma
                }
            }
            
            # Track subreddits as a set first, then convert to list
            subreddits_set = set()
            
            # Scrape posts
            posts = self._scrape_user_posts(user)
            user_data['posts'] = posts
            
            # Scrape comments
            comments = self._scrape_user_comments(user)
            user_data['comments'] = comments
            
            # Extract subreddits from posts and comments
            for post in posts:
                if post.get('subreddit'):
                    subreddits_set.add(post.get('subreddit'))
            for comment in comments:
                if comment.get('subreddit'):
                    subreddits_set.add(comment.get('subreddit'))
            
            # Convert set to sorted list
            user_data['subreddits'] = sorted(list(subreddits_set))
            
            self.logger.info(f"Successfully scraped {len(posts)} posts and {len(comments)} comments")
            
            return user_data
            
        except Exception as e:
            self.logger.error(f"Error scraping user profile: {e}")
            return None
    
    def _scrape_user_posts(self, user) -> List[Dict[str, Any]]:
        """Scrape user's posts."""
        posts = []
        try:
            self.logger.info(f"Scraping up to {self.max_posts} posts...")
            
            for i, post in enumerate(user.submissions.new(limit=self.max_posts)):
                try:
                    post_data = {
                        'id': post.id,
                        'title': post.title,
                        'content': post.selftext,
                        'subreddit': str(post.subreddit),
                        'created_utc': post.created_utc,
                        'score': post.score,
                        'upvote_ratio': getattr(post, 'upvote_ratio', 0),
                        'num_comments': post.num_comments,
                        'url': f"https://reddit.com{post.permalink}",
                        'is_self': post.is_self,
                        'link_flair_text': post.link_flair_text,
                        'type': 'post'
                    }
                    posts.append(post_data)
                    
                    # Rate limiting
                    time.sleep(SCRAPING_CONFIG['rate_limit_delay'])
                    
                    if self.verbose:
                        self.logger.info(f"Scraped post {i+1}/{self.max_posts}: {post.title[:50]}...")
                        
                except Exception as e:
                    self.logger.warning(f"Error scraping individual post: {e}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Error scraping posts: {e}")
            
        return posts
    
    def _scrape_user_comments(self, user) -> List[Dict[str, Any]]:
        """Scrape user's comments."""
        comments = []
        try:
            self.logger.info(f"Scraping up to {self.max_comments} comments...")
            
            for i, comment in enumerate(user.comments.new(limit=self.max_comments)):
                try:
                    # Skip deleted comments
                    if comment.body in ['[deleted]', '[removed]']:
                        continue
                    
                    comment_data = {
                        'id': comment.id,
                        'content': comment.body,
                        'subreddit': str(comment.subreddit),
                        'created_utc': comment.created_utc,
                        'score': comment.score,
                        'url': f"https://reddit.com{comment.permalink}",
                        'parent_id': comment.parent_id,
                        'is_submitter': getattr(comment, 'is_submitter', False),
                        'type': 'comment'
                    }
                    
                    # Try to get parent post title if available
                    try:
                        if hasattr(comment, 'submission'):
                            comment_data['parent_post_title'] = comment.submission.title
                    except:
                        comment_data['parent_post_title'] = None
                    
                    comments.append(comment_data)
                    
                    # Rate limiting
                    time.sleep(SCRAPING_CONFIG['rate_limit_delay'])
                    
                    if self.verbose:
                        self.logger.info(f"Scraped comment {i+1}/{self.max_comments}: {comment.body[:50]}...")
                        
                except Exception as e:
                    self.logger.warning(f"Error scraping individual comment: {e}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Error scraping comments: {e}")
            
        return comments
    
    def get_user_statistics(self, username: str) -> Dict[str, Any]:
        """Get basic user statistics."""
        try:
            # Verify authentication first
            try:
                me = self.reddit.user.me()
                if not me:
                    self.logger.error("Not authenticated to Reddit API")
                    return {}
            except Exception as e:
                self.logger.error(f"Authentication verification failed: {e}")
                return {}
            
            user = self.reddit.redditor(username)
            
            stats = {
                'username': username,
                'link_karma': getattr(user, 'link_karma', 0),
                'comment_karma': getattr(user, 'comment_karma', 0),
                'created_utc': user.created_utc,
                'account_age_days': (datetime.now().timestamp() - user.created_utc) / 86400,
                'has_verified_email': getattr(user, 'has_verified_email', False),
                'is_employee': getattr(user, 'is_employee', False),
                'is_mod': getattr(user, 'is_mod', False),
                'is_gold': getattr(user, 'is_gold', False),
            }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Error getting user statistics: {e}")
            return {}
    
    def validate_user_exists(self, username: str) -> bool:
        """Check if a Reddit user exists and is accessible."""
        try:
            # Verify authentication first
            try:
                me = self.reddit.user.me()
                if not me:
                    self.logger.error("Not authenticated to Reddit API")
                    return False
            except Exception as e:
                self.logger.error(f"Authentication verification failed: {e}")
                return False
            
            user = self.reddit.redditor(username)
            user.created_utc  # This will raise an exception if user doesn't exist
            return True
        except (NotFound, Forbidden):
            return False
        except Exception as e:
            self.logger.error(f"Error validating user: {e}")
            return False