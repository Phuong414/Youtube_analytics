import os
import csv
from datetime import datetime
from googleapiclient.discovery import build
from dotenv import load_dotenv
from tqdm import tqdm
import time

# Load environment variables
load_dotenv()
API_KEY = os.getenv('YOUTUBE_API_KEY')

if not API_KEY:
    print("ERROR: No API key found in .env file")
    exit()

# Initialize YouTube API
youtube = build('youtube', 'v3', developerKey=API_KEY)

# Artist configurations - YOU NEED TO UPDATE THESE CHANNEL IDs
ARTISTS = {
    'Max Giesinger': 'UCxxxxxxxxxxxxxxxxxx',  # Replace with actual channel ID
    'Wincent Weiss': 'UCyyyyyyyyyyyyyyyyyy'   # Replace with actual channel ID
}

def get_channel_videos(channel_id, max_results=50):
    """Get recent videos from a channel"""
    videos = []
    
    # Get uploads playlist ID
    request = youtube.channels().list(
        part='contentDetails',
        id=channel_id
    )
    response = request.execute()
    
    if not response['items']:
        return videos
    
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    # Get videos from playlist
    next_page_token = None
    
    while len(videos) < max_results:
        request = youtube.playlistItems().list(
            part='snippet,contentDetails',
            playlistId=playlist_id,
            maxResults=min(50, max_results - len(videos)),
            pageToken=next_page_token
        )
        response = request.execute()
        
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
        response = request.execute()
        
        for video in response['items']:
            video_data.append({
                'video_id': video['id'],
                'channel_id': video['snippet']['channelId'],
                'channel_name': video['snippet']['channelTitle'],
                'title': video['snippet']['title'],
                'description': video['snippet'].get('description', ''),
                'published_at': video['snippet']['publishedAt'],
                'duration': video['contentDetails']['duration'],
                'view_count': video['statistics'].get('viewCount', 0),
                'like_count': video['statistics'].get('likeCount', 0),
                'comment_count': video['statistics'].get('commentCount', 0),
                'tags': '|'.join(video['snippet'].get('tags', [])),
                'category_id': video['snippet'].get('categoryId', ''),
                'thumbnail_url': video['snippet']['thumbnails']['high']['url']
            })
    
    return video_data

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
    print("🎬 Starting YouTube Data Collection...")
    print(f"📅 Date: {datetime.now()}\n")
    
    all_videos = []
    
    # Fetch data for each artist
    for artist_name, channel_id in ARTISTS.items():
        print(f"\n{'='*60}")
        print(f"👤 Fetching data for: {artist_name}")
        print(f"{'='*60}")
        
        # Get video IDs
        print("\n1️⃣ Fetching video list...")
        video_ids = get_channel_videos(channel_id, max_results=50)
        print(f"   ✓ Found {len(video_ids)} videos")
        
        # Get video details
        print("\n2️⃣ Fetching video details...")
        videos = get_video_details(video_ids)
        all_videos.extend(videos)
        print(f"   ✓ Retrieved details for {len(videos)} videos")
        
        time.sleep(1)  # Rate limiting
    
    # Save all data
    print(f"\n{'='*60}")
    print("💾 Saving data to CSV files...")
    print(f"{'='*60}\n")
    
    save_to_csv(all_videos, 'videos.csv')
    
    # Summary
    print(f"\n{'='*60}")
    print("📊 SUMMARY")
    print(f"{'='*60}")
    print(f"✅ Videos: {len(all_videos)}")
    print(f"\n🎉 Data collection complete!")

if __name__ == "__main__":
    main()