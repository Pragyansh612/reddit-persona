#!/usr/bin/env python3
"""
Reddit User Persona Generator - Main Entry Point

This script scrapes Reddit user profiles and generates comprehensive user personas
using LLM analysis with proper citations.

Usage:
    python main.py --url "https://www.reddit.com/user/username/"
    python main.py --url "https://www.reddit.com/user/username/" --max-posts 50 --verbose

Author: Your Name
Date: 2024
"""

import argparse
import logging
import os
import sys
from typing import Optional

from src.reddit_scraper import RedditScraper
from src.persona_generator import PersonaGenerator
from src.data_processor import DataProcessor
from src.utils import setup_logging, create_directories, validate_url
from config import OUTPUT_CONFIG, LOGGING_CONFIG


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Generate user personas from Reddit profiles',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python main.py --url "https://www.reddit.com/user/kojied/"
  python main.py --url "https://www.reddit.com/user/Hungry-Move-6603/" --max-posts 10
  python main.py --url "https://www.reddit.com/user/kojied/" --verbose --output-dir "./custom_output"
        '''
    )
    
    parser.add_argument(
        '--url',
        required=True,
        help='Reddit user profile URL (e.g., https://www.reddit.com/user/username/)'
    )
    
    parser.add_argument(
        '--max-posts',
        type=int,
        default=10,
        help='Maximum number of posts to scrape (default: 10)'
    )
    
    parser.add_argument(
        '--max-comments',
        type=int,
        default=15,
        help='Maximum number of comments to scrape (default: 15)'
    )
    
    parser.add_argument(
        '--output-dir',
        default=OUTPUT_CONFIG['output_dir'],
        help='Output directory for persona files (default: ./output)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug mode'
    )
    
    parser.add_argument(
        '--no-citations',
        action='store_true',
        help='Disable citations in output'
    )
    
    return parser.parse_args()


def extract_username(url: str) -> str:
    """Extract username from Reddit URL."""
    try:
        # Handle different URL formats
        if '/user/' in url:
            username = url.split('/user/')[1].rstrip('/')
        elif '/u/' in url:
            username = url.split('/u/')[1].rstrip('/')
        else:
            raise ValueError("Invalid Reddit URL format")
        
        # Clean username
        username = username.split('/')[0]  # Remove any trailing path
        return username
    except Exception as e:
        raise ValueError(f"Could not extract username from URL: {e}")


def main() -> None:
    """Main function to orchestrate the persona generation process."""
    # Parse command line arguments
    args = parse_arguments()
    
    # Set up logging
    log_level = logging.DEBUG if args.debug else (logging.INFO if args.verbose else logging.WARNING)
    setup_logging(level=log_level)
    logger = logging.getLogger(__name__)
    
    try:
        # Validate URL
        if not validate_url(args.url):
            logger.error(f"Invalid Reddit URL: {args.url}")
            sys.exit(1)
        
        # Extract username
        username = extract_username(args.url)
        logger.info(f"Processing user: {username}")
        
        # Create necessary directories
        create_directories([args.output_dir, './data', './logs'])
        
        # Initialize components
        scraper = RedditScraper(
            max_posts=args.max_posts,
            max_comments=args.max_comments,
            verbose=args.verbose
        )
        
        processor = DataProcessor(verbose=args.verbose)
        
        generator = PersonaGenerator(
            include_citations=not args.no_citations,
            verbose=args.verbose
        )
        
        # Step 1: Scrape Reddit data
        logger.info("Step 1: Scraping Reddit data...")
        user_data = scraper.scrape_user_profile(username)
        
        if not user_data:
            logger.error(f"Failed to scrape data for user: {username}")
            sys.exit(1)
        
        logger.info(f"Successfully scraped {len(user_data.get('posts', []))} posts and "
                   f"{len(user_data.get('comments', []))} comments")
        
        # Step 2: Process and clean data
        logger.info("Step 2: Processing and cleaning data...")
        processed_data = processor.process_user_data(user_data)
        
        # Step 3: Generate persona
        logger.info("Step 3: Generating user persona...")
        persona = generator.generate_persona(processed_data)
        
        # Step 4: Save results
        logger.info("Step 4: Saving results...")
        output_file = os.path.join(args.output_dir, f"{username}_persona.txt")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(persona)
        
        logger.info(f"Persona generated successfully: {output_file}")
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"PERSONA GENERATION COMPLETE")
        print(f"{'='*60}")
        print(f"User: {username}")
        print(f"Posts analyzed: {len(processed_data.get('posts', []))}")
        print(f"Comments analyzed: {len(processed_data.get('comments', []))}")
        print(f"Output file: {output_file}")
        print(f"{'='*60}")
        
        if args.verbose:
            print("\nPersona preview:")
            print("-" * 40)
            with open(output_file, 'r', encoding='utf-8') as f:
                preview = f.read()[:500]
                print(f"{preview}...")
        
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error occurred: {e}")
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()