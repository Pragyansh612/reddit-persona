"""
Persona Generator Module

Uses LLM analysis to generate comprehensive user personas from Reddit data.
"""

import logging
from typing import Dict, List, Any, Optional
import openai
from datetime import datetime
import json
import re

from config import OPENAI_CONFIG, PERSONA_PROMPTS, PERSONA_CONFIG


class PersonaGenerator:
    """Generates user personas using LLM analysis of Reddit data."""
    
    def __init__(self, include_citations: bool = True, verbose: bool = False):
        """Initialize the persona generator."""
        self.include_citations = include_citations
        self.verbose = verbose
        self.logger = logging.getLogger(__name__)
        
        # Initialize OpenAI client
        openai.api_key = OPENAI_CONFIG['api_key']
        self.model = OPENAI_CONFIG['model']
        
    def generate_persona(self, user_data: Dict[str, Any]) -> str:
        """
        Generate a comprehensive user persona from Reddit data.
        
        Args:
            user_data: Processed Reddit user data
            
        Returns:
            Formatted persona string
        """
        self.logger.info("Starting persona generation...")
        
        # Extract content for analysis
        content = self._extract_content_for_analysis(user_data)
        
        if not content:
            self.logger.error("No content available for analysis")
            return "Unable to generate persona: No content available"
        
        # Generate different aspects of persona
        persona_sections = {}
        
        # Basic information
        persona_sections['basic_info'] = self._generate_basic_info(user_data)
        
        # Demographic analysis
        persona_sections['demographics'] = self._analyze_demographics(content, user_data)
        
        # Personality analysis
        persona_sections['personality'] = self._analyze_personality(content, user_data)
        
        # Interests and hobbies
        persona_sections['interests'] = self._analyze_interests(content, user_data)
        
        # Behaviors and habits
        persona_sections['behaviors'] = self._analyze_behaviors(content, user_data)
        
        # Motivations and goals
        persona_sections['motivations'] = self._analyze_motivations(content, user_data)
        
        # Frustrations and pain points
        persona_sections['frustrations'] = self._analyze_frustrations(content, user_data)
        
        # Format final persona
        formatted_persona = self._format_persona(persona_sections, user_data)
        
        self.logger.info("Persona generation completed")
        return formatted_persona
    
    def _extract_content_for_analysis(self, user_data: Dict[str, Any]) -> str:
        """Extract and combine content for LLM analysis."""
        content_parts = []
        
        # Add posts
        for post in user_data.get('posts', []):
            if post.get('title'):
                content_parts.append(f"POST TITLE: {post['title']}")
            if post.get('content') and len(post['content']) > PERSONA_CONFIG['min_content_length']:
                content_parts.append(f"POST CONTENT: {post['content']}")
        
        # Add comments
        for comment in user_data.get('comments', []):
            if comment.get('content') and len(comment['content']) > PERSONA_CONFIG['min_content_length']:
                content_parts.append(f"COMMENT: {comment['content']}")
        
        return "\n\n".join(content_parts)
    
    def _generate_basic_info(self, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate basic user information."""
        # Fix: Ensure subreddits is a list of strings
        subreddits = user_data.get('subreddits', [])
        if subreddits and isinstance(subreddits[0], dict):
            # If subreddits contains dictionaries, extract the name/string representation
            subreddits = [str(sub) if isinstance(sub, dict) else sub for sub in subreddits]
        
        return {
            'username': user_data.get('username', 'Unknown'),
            'account_age_days': user_data.get('account_age_days', 0),
            'total_posts': len(user_data.get('posts', [])),
            'total_comments': len(user_data.get('comments', [])),
            'post_karma': user_data.get('karma', {}).get('post', 0),
            'comment_karma': user_data.get('karma', {}).get('comment', 0),
            'active_subreddits': subreddits
        }
    
    def _analyze_demographics(self, content: str, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze demographic information using LLM."""
        try:
            prompt = PERSONA_PROMPTS['demographic_analysis'].format(content=content[:3000])
            
            response = openai.ChatCompletion.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an expert at analyzing text to identify demographic information. Be conservative in your estimates and only make claims based on clear evidence."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=OPENAI_CONFIG['max_tokens'],
                temperature=OPENAI_CONFIG['temperature']
            )
            
            analysis = response.choices[0].message.content
            
            # Extract citations if enabled
            citations = []
            if self.include_citations:
                citations = self._extract_citations(analysis, user_data)
            
            return {
                'analysis': analysis,
                'citations': citations
            }
            
        except Exception as e:
            self.logger.error(f"Error in demographic analysis: {e}")
            return {'analysis': 'Unable to analyze demographics', 'citations': []}
    
    def _analyze_personality(self, content: str, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze personality traits using LLM."""
        try:
            prompt = PERSONA_PROMPTS['personality_analysis'].format(content=content[:3000])
            
            response = openai.ChatCompletion.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an expert at analyzing writing style and communication patterns to identify personality traits. Focus on observable behaviors and communication patterns."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=OPENAI_CONFIG['max_tokens'],
                temperature=OPENAI_CONFIG['temperature']
            )
            
            analysis = response.choices[0].message.content
            
            citations = []
            if self.include_citations:
                citations = self._extract_citations(analysis, user_data)
            
            return {
                'analysis': analysis,
                'citations': citations
            }
            
        except Exception as e:
            self.logger.error(f"Error in personality analysis: {e}")
            return {'analysis': 'Unable to analyze personality', 'citations': []}
    
    def _analyze_interests(self, content: str, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze interests and hobbies using LLM."""
        try:
            # Fix: Ensure subreddits is a list of strings before joining
            subreddits_raw = user_data.get('subreddits', [])
            if subreddits_raw:
                # Convert any non-string items to strings
                subreddits_list = []
                for sub in subreddits_raw:
                    if isinstance(sub, dict):
                        # If it's a dict, try to get a meaningful string representation
                        subreddits_list.append(str(sub.get('name', sub)))
                    else:
                        subreddits_list.append(str(sub))
                subreddits = ", ".join(subreddits_list)
            else:
                subreddits = "None"
            
            prompt = PERSONA_PROMPTS['interests_analysis'].format(
                content=content[:3000],
                subreddits=subreddits
            )
            
            response = openai.ChatCompletion.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an expert at identifying interests and hobbies from online activity. Consider both explicit mentions and implicit interests shown through participation."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=OPENAI_CONFIG['max_tokens'],
                temperature=OPENAI_CONFIG['temperature']
            )
            
            analysis = response.choices[0].message.content
            
            citations = []
            if self.include_citations:
                citations = self._extract_citations(analysis, user_data)
            
            return {
                'analysis': analysis,
                'citations': citations
            }
            
        except Exception as e:
            self.logger.error(f"Error in interests analysis: {e}")
            return {'analysis': 'Unable to analyze interests', 'citations': []}
    
    def _analyze_behaviors(self, content: str, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze behavioral patterns using LLM."""
        try:
            prompt = PERSONA_PROMPTS['behavior_analysis'].format(content=content[:3000])
            
            response = openai.ChatCompletion.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an expert at analyzing behavioral patterns from online activity. Focus on observable patterns in posting, engagement, and interaction styles."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=OPENAI_CONFIG['max_tokens'],
                temperature=OPENAI_CONFIG['temperature']
            )
            
            analysis = response.choices[0].message.content
            
            citations = []
            if self.include_citations:
                citations = self._extract_citations(analysis, user_data)
            
            return {
                'analysis': analysis,
                'citations': citations
            }
            
        except Exception as e:
            self.logger.error(f"Error in behavior analysis: {e}")
            return {'analysis': 'Unable to analyze behaviors', 'citations': []}
    
    def _analyze_motivations(self, content: str, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze motivations and goals using LLM."""
        try:
            prompt = PERSONA_PROMPTS['motivations_analysis'].format(content=content[:3000])
            
            response = openai.ChatCompletion.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an expert at identifying underlying motivations and goals from online behavior. Look for patterns that reveal what drives the person."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=OPENAI_CONFIG['max_tokens'],
                temperature=OPENAI_CONFIG['temperature']
            )
            
            analysis = response.choices[0].message.content
            
            citations = []
            if self.include_citations:
                citations = self._extract_citations(analysis, user_data)
            
            return {
                'analysis': analysis,
                'citations': citations
            }
            
        except Exception as e:
            self.logger.error(f"Error in motivations analysis: {e}")
            return {'analysis': 'Unable to analyze motivations', 'citations': []}
    
    def _analyze_frustrations(self, content: str, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze frustrations and pain points using LLM."""
        try:
            # Custom prompt for frustrations
            prompt = f"""
            Analyze the following Reddit posts and comments to identify frustrations, complaints, and pain points expressed by the user.
            Look for:
            - Explicit complaints or frustrations
            - Recurring problems they face
            - Things that annoy or bother them
            - Challenges they're trying to overcome
            - Negative experiences they share
            
            Content: {content[:3000]}
            """
            
            response = openai.ChatCompletion.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an expert at identifying frustrations and pain points from online communication. Focus on explicit complaints and recurring negative themes."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=OPENAI_CONFIG['max_tokens'],
                temperature=OPENAI_CONFIG['temperature']
            )
            
            analysis = response.choices[0].message.content
            
            citations = []
            if self.include_citations:
                citations = self._extract_citations(analysis, user_data)
            
            return {
                'analysis': analysis,
                'citations': citations
            }
            
        except Exception as e:
            self.logger.error(f"Error in frustrations analysis: {e}")
            return {'analysis': 'Unable to analyze frustrations', 'citations': []}
    
    def _extract_citations(self, analysis: str, user_data: Dict[str, Any]) -> List[Dict[str, str]]:
        """Extract citations for persona claims."""
        citations = []
        
        # Simple citation extraction - match content with posts/comments
        all_content = []
        
        # Add posts
        for post in user_data.get('posts', []):
            all_content.append({
                'type': 'post',
                'content': post.get('content', ''),
                'title': post.get('title', ''),
                'url': post.get('url', ''),
                'subreddit': post.get('subreddit', '')
            })
        
        # Add comments
        for comment in user_data.get('comments', []):
            all_content.append({
                'type': 'comment',
                'content': comment.get('content', ''),
                'url': comment.get('url', ''),
                'subreddit': comment.get('subreddit', '')
            })
        
        # For now, return the most relevant sources
        # In a real implementation, you'd use more sophisticated matching
        return all_content[:5]  # Return top 5 sources
    
    def _format_persona(self, sections: Dict[str, Any], user_data: Dict[str, Any]) -> str:
        """Format the complete persona document."""
        username = user_data.get('username', 'Unknown')
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Fix: Ensure subreddits are properly formatted as strings
        subreddits = sections['basic_info']['active_subreddits']
        if subreddits:
            subreddits_str = ', '.join(str(sub) for sub in subreddits[:10])
        else:
            subreddits_str = "None"
        
        persona_text = f"""
{'='*80}
USER PERSONA: {username}
{'='*80}

Generated on: {timestamp}
Analysis based on {len(user_data.get('posts', []))} posts and {len(user_data.get('comments', []))} comments

{'='*80}
BASIC INFORMATION
{'='*80}

Username: {sections['basic_info']['username']}
Account Age: {sections['basic_info']['account_age_days']:.0f} days
Total Posts: {sections['basic_info']['total_posts']}
Total Comments: {sections['basic_info']['total_comments']}
Post Karma: {sections['basic_info']['post_karma']}
Comment Karma: {sections['basic_info']['comment_karma']}
Active Subreddits: {subreddits_str}

{'='*80}
DEMOGRAPHICS
{'='*80}

{sections['demographics']['analysis']}

{'='*80}
PERSONALITY TRAITS
{'='*80}

{sections['personality']['analysis']}

{'='*80}
INTERESTS & HOBBIES
{'='*80}

{sections['interests']['analysis']}

{'='*80}
BEHAVIORS & HABITS
{'='*80}

{sections['behaviors']['analysis']}

{'='*80}
MOTIVATIONS & GOALS
{'='*80}

{sections['motivations']['analysis']}

{'='*80}
FRUSTRATIONS & PAIN POINTS
{'='*80}

{sections['frustrations']['analysis']}

"""

        # Add citations if enabled
        if self.include_citations:
            persona_text += f"""
{'='*80}
CITATIONS & SOURCES
{'='*80}

The following sources were used to generate this persona:

"""
            
            # Add sample citations
            for i, citation in enumerate(sections['demographics']['citations'][:5], 1):
                persona_text += f"{i}. {citation['type'].upper()}: {citation.get('title', citation['content'][:100])}...\n"
                persona_text += f"   URL: {citation.get('url', 'N/A')}\n"
                persona_text += f"   Subreddit: r/{citation.get('subreddit', 'N/A')}\n\n"
        
        persona_text += f"""
{'='*80}
DISCLAIMER
{'='*80}

This persona is generated based on publicly available Reddit activity and should be 
used for research and educational purposes only. The analysis represents patterns 
observed in the user's online behavior and may not reflect their complete personality 
or circumstances.

Generation completed at: {timestamp}
"""
        
        return persona_text