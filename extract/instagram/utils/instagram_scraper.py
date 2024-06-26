import json
import time
from itertools import islice

import instaloader
from instaloader import Profile
import logging

from utils.logger import Logger


class InstagramScraper:
    def __init__(self, username, password):
        self.logger = Logger().get_logger()
        self.username = username
        self.password = password
        self.loader = instaloader.Instaloader()

    def login(self):
        try:
            self.loader.login(self.username, self.password)
            logging.info("Login successful")
        except instaloader.exceptions.BadCredentialsException:
            logging.error("Login failed: Bad credentials")
            raise
        except instaloader.exceptions.TwoFactorAuthRequiredException:
            logging.error("Login failed: Two-factor authentication required")
            raise
        except Exception as e:
            logging.error(f"An unexpected error occurred during login: {e}")
            raise

    def get_profile_info(self, target_username):
        try:
            profile = Profile.from_username(self.loader.context, target_username)
            profile_info = {
                'name': profile.full_name,
                'username': profile.username,
                'followers_count': str(profile.followers),
                'following_count': str(profile.followees),
                'business_category': profile.business_category_name,
                'category': profile.business_category_name,
                'is_business_account': str(profile.is_business_account),
                'business_email': None,
                'business_phone_number': None,
                'biography': profile.biography,
                'external_url': profile.external_url,
            }
            logging.info("Profile information retrieved successfully")
            return profile_info
        except instaloader.exceptions.ProfileNotExistsException:
            logging.error(f"Profile not found: {target_username}")
            raise
        except Exception as e:
            logging.error(f"An unexpected error occurred while fetching profile info: {e}")
            raise

    def get_post_info(self, target_username):
        post_data = []
        try:
            profile = Profile.from_username(self.loader.context, target_username)
            for post in islice(profile.get_posts(), 10):
                node_content_dict = {
                    'accessibility_caption': post.accessibility_caption,
                    'caption': post.caption,
                    'caption_hashtags': post.caption_hashtags,
                    'caption_mentions': post.caption_mentions,
                    'comments': post.comments,
                    'date': post.date.strftime('%Y-%m-%d %H:%M:%S'),
                    'date_local': post.date_local.strftime('%Y-%m-%d %H:%M:%S'),
                    'date_utc': post.date_utc.strftime('%Y-%m-%d %H:%M:%S'),
                    'is_pinned': post.is_pinned,
                    'is_sponsored': post.is_sponsored,
                    'is_video': post.is_video,
                    'likes': post.likes,
                    'location': post.location,
                    'mediacount': post.mediacount,
                    'mediaid': post.mediaid,
                    'owner_id': post.owner_id,
                    'owner_username': post.owner_username,
                    'pcaption': post.pcaption,
                    'profile': post.profile,
                    'shortcode': post.shortcode,
                    'sponsor_users': post.sponsor_users,
                    'tagged_users': post.tagged_users,
                    'title': post.title,
                    'typename': post.typename,
                    'url': post.url,
                    'video_duration': post.video_duration,
                    'video_url': post.video_url,
                    'video_view_count': post.video_view_count,
                    'viewer_has_liked': post.viewer_has_liked
                }

                # Convert dictionary to JSON string
                node_content_str = json.dumps(node_content_dict)

                post_info = {
                    'node_content': node_content_str,
                    'owner_username': post.owner_username,
                    'shortcode': post.shortcode,
                    'posted_at': post.date.strftime('%Y-%m-%d %H:%M:%S'),
                    'post_caption': post.caption,
                    'post_n_comments': str(post.comments),
                    'post_n_likes': str(post.likes)
                }
                post_data.append(post_info)
                time.sleep(1)
            logging.info(f"Post information retrieved successfully for {target_username}")
            return post_data
        except instaloader.exceptions.ProfileNotExistsException:
            logging.error(f"Profile not found: {target_username}")
            raise
        except Exception as e:
            logging.error(f"An unexpected error occurred while fetching post info for {target_username}: {e}")
            raise
