import os
import requests
import csv
import json
from datetime import datetime
import pytz
from dateutil import parser



# Base URL for the Bluesky PDS API
PDS_BASE_URL = "https://bsky.social/xrpc"

# Get users' time zone
UTC_TIMEZONE = pytz.utc
def get_user_timezone():
    """
    Prompts the user to enter a timezone and validates it using pytz.
    Returns a pytz timezone object.
    """
    while True:
        tz_input = input("Please enter your desired timezone (e.g., 'America/New_York', 'Europe/London', 'Asia/Tokyo', 'Pacific/Fiji'): ").strip()
        try:
            user_tz = pytz.timezone(tz_input)
            print(f"Timezone set to: {user_tz}")
            return user_tz
        except pytz.UnknownTimeZoneError:
            print(f"Error: '{tz_input}' is not a valid timezone. Please try again.")
        except Exception as e:
            print(f"An unexpected error occurred while setting timezone: {e}")


#login section

def bluesky_login(identifier: str, password: str):
    """
    Logs into Bluesky and returns the access JWT.
    """
    login_url = f"{PDS_BASE_URL}/com.atproto.server.createSession"
    headers = {"Content-Type": "application/json"}
    payload = {
        "identifier": identifier,
        "password": password
    }
    
    try:
        response = requests.post(login_url, headers=headers, json=payload)
        response.raise_for_status()
        session_data = response.json()
        return session_data.get("accessJwt")
    except requests.exceptions.RequestException as e:
        print(f"Error during BSKY login: {e}")
        return None
    
# Fetch Posts #
def get_my_posts(jwt: str, user_handle:str, limit: int = 100, include_replies: bool = False, exclude_reposts: bool = True, from_date: datetime = None, to_date: datetime = None):
    """
    Fetches a specified number of posts for a given Bluesky user handle
    using direct HTTP requests. Handles pagination and date filtering during fetching.

    Args:
        jwt (str): The access JWT token.
        user_handle (str): The Bluesky handle.
        limit (int, optional): The maximum number of posts to fetch (after filtering).
        include_replies (bool, optional): Whether to include replies.
        exclude_reposts (bool, optional): Whether to exclude reposts.
        from_date (datetime, optional): A UTC timezone-aware datetime to filter posts from (inclusive).
        to_date (datetime, optional): A UTC timezone-aware datetime to filter posts until (inclusive).

    Returns:
        list: A list of dictionaries, where each dictionary represents a Bluesky post
              that matches the criteria.
    """
    filtered_posts = [] # This will store our final filtered posts
    cursor = None
    
    print(f"Fetching posts for {user_handle} (Include replies: {include_replies}, Exclude reposts: {exclude_reposts})...")
    if from_date:
        print(f"Filtering posts from: {from_date.isoformat()}")
    if to_date:
        print(f"Filtering posts until: {to_date.isoformat()}") 

    while True:
        try:
            get_feed_url = f"{PDS_BASE_URL}/app.bsky.feed.getAuthorFeed"

            params = {
                "actor": user_handle,
                "limit": 100, # Fetch max per request
            }
            
            if include_replies:
                params["filter"] = "posts_with_replies"
            else:
                params["filter"] = "posts_no_replies"

            if cursor:
                params["cursor"] = cursor

            headers = {
                "Authorization": f"Bearer {jwt}",
                "Content-Type": "application/json"
            }

            response = requests.get(get_feed_url, headers=headers, params=params)
            response.raise_for_status()

            feed_data = response.json()

            if not feed_data.get("feed"):
                break # No More Posts

            current_batch_has_relevant_posts = False # Flag to check if current batch has posts within from_date

            for feed_item in feed_data["feed"]:
                # Only process actual post items
                if not feed_item.get("post"):
                    continue # Skip if it's not a post item
                if exclude_reposts and feed_item.get("reason"):
                    continue # Skip this item, it's a repost

                if feed_item["post"].get("record") and \
                   isinstance(feed_item["post"]["record"], dict) and \
                   feed_item["post"]["record"].get('$type') == 'app.bsky.feed.post':
                    
                    post = feed_item["post"]
                    created_at_iso_utc = post.get('record', {}).get('createdAt', '')

                    try:
                        post_datetime_utc = parser.isoparse(created_at_iso_utc).astimezone(pytz.utc)
                        
                        # Apply date filters immediately
                        # If we've gone past the 'from_date', and already collected enough posts, we can stop early
                        if from_date and post_datetime_utc < from_date:
                            # If posts are ordered chronologically (latest first), then once we pass the 'from_date'
                            # all subsequent posts will also be too old.
                            # We can break out of the inner loop and then the outer while loop.
                            print(f"Reached post older than 'from_date': {post_datetime_utc}. Stopping fetch.")
                            current_batch_has_relevant_posts = False # No more relevant posts in this batch or subsequent ones
                            break # Break out of the for loop, will then break while loop
                        
                        if to_date and post_datetime_utc > to_date:
                            continue  # Skip posts after the to_date, but keep fetching as older posts might be relevant

                        # If the post passes all filters, add it to our list
                        filtered_posts.append(post)
                        current_batch_has_relevant_posts = True # At least one relevant post found in this batch

                        if len(filtered_posts) >= limit:
                            print(f"Reached desired limit of {limit} filtered posts. Stopping fetch.")
                            break # Break out of the for loop, will then break while loop

                    except Exception as e:
                        print(f"Could not parse timestamp for post (URI: {post.get('uri', 'N/A')}): {e}. Skipping post for filtering.")
                        continue # If we can't parse, we skip this post

            cursor = feed_data.get("cursor")
            
            # If we reached the limit or no more posts from API, or if we passed the from_date and have enough posts, break
            if len(filtered_posts) >= limit or not cursor or (from_date and not current_batch_has_relevant_posts and len(filtered_posts) > 0):
                break # No more posts to fetch or limit reached, or we've gone past our 'from_date' in a chronologically ordered feed

        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching posts: {e}")
            break
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON response {e}")
            break
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            break
            
    print(f"Successfully fetched and filtered {len(filtered_posts)} posts.")
    return filtered_posts

      
# Function to analyse posts and prepare a CSV file 
def prepare_post_data_for_csv(posts: list, target_timezone):
    """
    Analyzes a list of Bluesky posts (dictionaries) and prepares data
    in a format suitable for CSV export, localized to the target_timezone.

    Args:
        posts (list): A list of dictionaries representing Bluesky posts.
        target_timezone (pytz.tzinfo.BaseTzInfo): A pytz timezone object for conversion.

    Returns:
        list: A list of dictionaries, each suitable for a CSV row.
    """
    if not posts:
        print("No posts to analyze or export.")
        return []

    csv_data = []
    
    print(f"\n--- Preparing Post Data for CSV (Localizing Time to {target_timezone.tzname(datetime.now())}) ---")
    
    problematic_post_count = 0
    max_problematic_posts_to_show = 5 

    for i, post in enumerate(posts):
        text = post.get('record', {}).get('text', '')
        created_at_iso_utc = post.get('record', {}).get('createdAt', '') 

        formatted_date = ''
        formatted_time = ''

        try:
            
            dt_utc_aware = parser.isoparse(created_at_iso_utc)
            # Defensive check: If for any reason it's still naive (shouldn't be with 'Z' and isoparse),
            # explicitly localize it as UTC.
            if dt_utc_aware.tzinfo is None:
                dt_utc_aware = UTC_TIMEZONE.localize(dt_utc_aware)
            
            # Now convert to the target timezone
            dt_target_aware = dt_utc_aware.astimezone(target_timezone)
            
            # Format the date and time strings
            formatted_date = dt_target_aware.strftime('%Y-%m-%d')
            formatted_time = dt_target_aware.strftime('%H:%M:%S')

        except Exception as e: # Keep broad exception for now, but `ValueError` is likely the primary one
            if problematic_post_count < max_problematic_posts_to_show:
                print(f"ERROR: Post {i+1} - Could not parse or localize timestamp '{created_at_iso_utc}'. Error: {e}")
                problematic_post_count += 1
            elif problematic_post_count == max_problematic_posts_to_show:
                print(f" (Suppressing further timestamp parsing errors for brevity.)")
                problematic_post_count += 1
            # formatted_date and formatted_time remain ''

        like_count = post.get('likeCount', 0)
        reply_count = post.get('replyCount', 0)

        csv_data.append({
            'Post': text,
            'Date': formatted_date,
            'Time': formatted_time,
            '#likes': like_count,
            '#comments': reply_count
        })

    print(f"Prepared {len(csv_data)} posts for CSV export with localized time.")
    return csv_data

# --- Function to save data to CSV ---
def save_posts_to_csv(posts_data: list, filename: str): # Your function signature
    print(f"DEBUG (save_posts_to_csv): Received filename parameter: {filename}")
    

    """
    Saves a list of dictionaries to a CSV file.
    """
    if not posts_data:
        print("No data to save to CSV.")
        return

    fieldnames = ['Post', 'Date', 'Time', '#likes', '#comments']

    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile: 
            pass
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            writer.writerows(posts_data)

        print(f"\nSuccessfully saved {len(posts_data)} posts to '{filename}'")
    except IOError as e:
        print(f"Error saving to CSV file '{filename}': {e}")

