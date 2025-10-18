import os
import csv
from datetime import datetime
from googleapiclient.discovery import build
from dotenv import load_dotenv
import time
from googleapiclient.errors import HttpError
import sys

# Load environment variables
load_dotenv()
API_KEY = os.getenv('YOUTUBE_API_KEY')

if not API_KEY:
    print("ERROR: No API key found in .env file")
    exit()

# Initialize YouTube API
youtube = build('youtube', 'v3', developerKey=API_KEY)


def safe_execute(request):
    """Execute a googleapiclient request and handle HttpError with clearer messages."""
    try:
        return request.execute()
    except HttpError as e:
        try:
            error_content = e.content.decode() if isinstance(e.content, (bytes, bytearray)) else str(e.content)
        except Exception:
            error_content = str(e)

        if 'API key expired' in error_content or 'api key' in error_content.lower():
            print("ERROR: The YouTube API key appears to be invalid or expired.")
            print("Please renew or replace the API key in your .env file (YOUTUBE_API_KEY).")
            print(f"Details: {error_content}")
            sys.exit(1)
        elif 'commentsDisabled' in error_content:
            print("WARNING: Comments are disabled for this video.")
            return None
        else:
            print("ERROR: YouTube API request failed.")
            print(f"Details: {error_content}")
            return None
    except Exception as e:
        print("ERROR: Unexpected error during API request:", e)
        return None


# Artist configurations - ADD YOUR 5 ARTISTS HERE
ARTISTS = {
    'Artist 1': 'CHANNEL_ID_HERE',
    'Artist 2': 'CHANNEL_ID_HERE',
    'Artist 3': 'CHANNEL_ID_HERE',
    'Artist 4': 'CHANNEL_ID_HERE',
    'Artist 5': 'CHANNEL_ID_HERE'
}


def get_channel_info(channel_id):
    """Get channel statistics for benchmarking (DV3)"""
    request = youtube.channels().list(
        part='statistics,snippet',
        id=channel_id
    )
    response = safe_execute(request)
    
    if not response or 'items' not in response:
        return None
    
    channel = response['items'][0]
    return {
        'channel_id': channel_id,
        'channel_name': channel['snippet']['title'],
        'subscriber_count': channel['statistics'].get('subscriberCount', 0),
        'total_view_count': channel['statistics'].get('viewCount', 0),
        'total_video_count': channel['statistics'].get('videoCount', 0),
        'country': channel['snippet'].get('country', ''),
        'custom_url': channel['snippet'].get('customUrl', '')
    }


def get_channel_videos(channel_id, max_results=100):
    """Get recent videos from a channel"""
    videos = []
    
    request = youtube.channels().list(
        part='contentDetails',
        id=channel_id
    )
    response = safe_execute(request)

    if not response or 'items' not in response:
        print("WARNING: Unexpected response from YouTube API when fetching channel contentDetails:")
        print(response)
        return videos
    
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    next_page_token = None
    
    while len(videos) < max_results:
        request = youtube.playlistItems().list(
            part='snippet,contentDetails',
            playlistId=playlist_id,
            maxResults=min(100, max_results - len(videos)),
            pageToken=next_page_token
        )
        response = safe_execute(request)

        if not response or 'items' not in response:
            print("WARNING: Unexpected response from playlistItems.list:")
            print(response)
            break

        for item in response['items']:
            video_id = item['contentDetails']['videoId']
            videos.append(video_id)
        
        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break
    
    return videos


def get_video_details(video_ids):
    """Get detailed information for videos"""
    video_data = []
    
    # YouTube API allows max 50 IDs per request
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        
        request = youtube.videos().list(
            part='snippet,statistics,contentDetails',
            id=','.join(batch)
        )
        response = safe_execute(request)
        
        if not response or 'items' not in response:
            continue
        
        for video in response['items']:
            snippet = video['snippet']
            title = snippet['title']
            title_lower = title.lower()
            
            # Content type classification (for DV1)
            content_type = 'other'
            if 'official video' in title_lower or 'music video' in title_lower:
                content_type = 'music_video'
            elif 'live' in title_lower or 'konzert' in title_lower:
                content_type = 'live'
            elif 'acoustic' in title_lower or 'akustik' in title_lower:
                content_type = 'acoustic'
            elif 'lyric' in title_lower or 'lyrics' in title_lower:
                content_type = 'lyric_video'
            elif 'vlog' in title_lower or 'behind' in title_lower:
                content_type = 'vlog'
            
            # Collaboration detection (for DV1)
            has_collab = bool('feat' in title_lower or 'ft.' in title_lower or 
                             'featuring' in title_lower or 'mit' in title_lower or
                             '&' in title or 'x' in title_lower)
            
            video_data.append({
                'video_id': video['id'],
                'channel_id': snippet['channelId'],
                'channel_name': snippet['channelTitle'],
                'title': title,
                'title_length': len(title),
                'description': snippet.get('description', ''),
                'published_at': snippet['publishedAt'],
                'duration': video['contentDetails']['duration'],
                'view_count': video['statistics'].get('viewCount', 0),
                'like_count': video['statistics'].get('likeCount', 0),
                'comment_count': video['statistics'].get('commentCount', 0),
                'tags': '|'.join(snippet.get('tags', [])),
                'tag_count': len(snippet.get('tags', [])),
                'category_id': snippet.get('categoryId', ''),
                'thumbnail_url': snippet['thumbnails']['high']['url'],
                'content_type': content_type,
                'has_collab': has_collab
            })
    
    return video_data


def get_video_comments(video_id, max_comments=500):
    """Get comments for a specific video"""
    comments = []
    next_page_token = None
    
    print(f"      Fetching comments for video {video_id}...", end='')
    
    while len(comments) < max_comments:
        try:
            request = youtube.commentThreads().list(
                part='snippet,replies',
                videoId=video_id,
                maxResults=min(100, max_comments - len(comments)),
                pageToken=next_page_token,
                textFormat='plainText'
            )
            response = safe_execute(request)
            
            if not response:
                print(f" Comments disabled or unavailable")
                break
            
            if 'items' not in response:
                break
            
            for item in response['items']:
                comment = item['snippet']['topLevelComment']['snippet']
                comment_text = comment['textDisplay']
                
                # Extract metadata for analysis
                reply_count = item['snippet']['totalReplyCount']
                
                # Check if comment has emojis (basic detection)
                has_emoji = any(ord(char) > 127 for char in comment_text)
                
                # Get comment length
                comment_length = len(comment_text)
                
                comments.append({
                    'comment_id': item['snippet']['topLevelComment']['id'],
                    'video_id': video_id,
                    'author': comment['authorDisplayName'],
                    'text': comment_text,
                    'like_count': comment['likeCount'],
                    'reply_count': reply_count,
                    'published_at': comment['publishedAt'],
                    'updated_at': comment.get('updatedAt', comment['publishedAt']),
                    'comment_length': comment_length,
                    'has_emoji': has_emoji
                })
            
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
                
            time.sleep(0.1)  # Small delay between comment pages
            
        except Exception as e:
            print(f" Error fetching comments: {str(e)[:50]}")
            break
    
    print(f" ✓ Got {len(comments)} comments")
    return comments


def save_to_csv(data, filename):
    """Save data to CSV file"""
    os.makedirs('data/raw', exist_ok=True)
    filepath = f'data/raw/{filename}'
    
    if data:
        keys = data[0].keys()
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(data)
        print(f"✅ Saved {len(data)} records to {filepath}")
    else:
        print(f"⚠️  No data to save for {filename}")


def main():
    print("🎬 Starting YouTube Data Collection (Videos + Comments)...")
    print(f"📅 Date: {datetime.now()}\n")
    
    all_channels = []
    all_videos = []
    all_comments = []
    
    # Fetch data for each artist
    for artist_name, channel_id in ARTISTS.items():
        print(f"\n{'='*60}")
        print(f"👤 Fetching data for: {artist_name}")
        print(f"{'='*60}")
        
        # Get channel info (for DV3 benchmarking)
        print("\n0️⃣ Fetching channel statistics...")
        channel_info = get_channel_info(channel_id)
        if channel_info:
            all_channels.append(channel_info)
            print(f"   ✓ Subscribers: {channel_info['subscriber_count']}")
        
        # Get video IDs
        print("\n1️⃣ Fetching video list...")
        video_ids = get_channel_videos(channel_id, max_results=100)
        print(f"   ✓ Found {len(video_ids)} videos")
        
        # Get video details
        print("\n2️⃣ Fetching video details...")
        videos = get_video_details(video_ids)
        all_videos.extend(videos)
        print(f"   ✓ Retrieved details for {len(videos)} videos")
        
        # Get comments for each video
        print("\n3️⃣ Fetching comments (500 per video)...")
        for video_id in video_ids:
            comments = get_video_comments(video_id, max_comments=500)
            all_comments.extend(comments)
            time.sleep(0.5)  # Rate limiting between videos
        
        print(f"   ✓ Total comments collected so far: {len(all_comments)}")
        time.sleep(1)
    
    # Save all data
    print(f"\n{'='*60}")
    print("💾 Saving data to CSV files...")
    print(f"{'='*60}\n")
    
    save_to_csv(all_channels, 'channels.csv')
    save_to_csv(all_videos, 'videos.csv')
    save_to_csv(all_comments, 'comments.csv')
    
    # Summary
    print(f"\n{'='*60}")
    print("📊 SUMMARY")
    print(f"{'='*60}")
    print(f"✅ Channels: {len(all_channels)}")
    print(f"✅ Videos: {len(all_videos)}")
    print(f"✅ Comments: {len(all_comments)}")
    if len(all_videos) > 0:
        print(f"📊 Average comments per video: {len(all_comments)/len(all_videos):.1f}")
    print(f"\n🎉 Data collection complete!")


if __name__ == "__main__":
    main()
