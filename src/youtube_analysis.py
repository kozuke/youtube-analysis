import os
from datetime import datetime

from click import option, command, argument
from googleapiclient.discovery import build
import pandas as pd

DEVELOPER_KEY = os.environ['YOUTUBE_API_KEY']
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

VIDEO_URL = 'https://www.youtube.com/watch?v='
CHANNEL_URL = 'https://www.youtube.com/channel/'

_HEADER_DICT = {
    'title': 'タイトル',
    'channelTitle': 'チャンネル名',
    'publishedAt': '公開日付',
    'video_url': '動画URL',
    'channel_url': 'チャンネルURL',
    'viewCount': '視聴数',
    'likeCount': 'GOOD数',
    'dislikeCount': 'BAD数',
    'commentCount': 'コメント数',
    'good_rate': 'GOOD率',
    'bad_rate': 'BAD率',
    'comment_rate': 'コメント率',
    'description': '説明',
}

_SEARCH_COLS = {
    'id.videoId': 'videoId',
    'snippet.publishedAt': 'publishedAt',
    'snippet.channelId': 'channelId',
    'snippet.title': 'title',
    'snippet.description': 'description',
    'snippet.channelTitle': 'channelTitle',
}

_VIDEO_COLS = {
    'id': 'videoId',
    'statistics.viewCount': 'viewCount',
    'statistics.likeCount': 'likeCount',
    'statistics.dislikeCount': 'dislikeCount',
    'statistics.commentCount': 'commentCount'
}

COUNT_COLS = ['viewCount',
              'likeCount',
              'dislikeCount',
              'commentCount']

_OUTFILE = 'video_analysis_{time}_{keyword}.csv'


@argument('keyword')
@option('--max-counts', '-mc', default=50)
@command()
def main(keyword, max_counts):
    time = datetime.today().strftime('%y%m%d_%H%M')
    outfile = _OUTFILE.format(time=time, keyword=keyword)

    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=DEVELOPER_KEY)

    search_response = youtube.search().list(
        q=keyword,
        part='snippet',
        order='viewCount',
        type='video',
        maxResults=max_counts,
    ).execute()
    df_search_response = pd.json_normalize(search_response, record_path='items')[_SEARCH_COLS] \
        .rename(columns=_SEARCH_COLS)
    video_ids = ','.join(df_search_response.videoId.to_list())

    video_info = youtube.videos().list(
        part='id,statistics',
        id=video_ids,
        maxResults=max_counts
    ).execute()
    df_video_info = pd.json_normalize(video_info, record_path='items')[_VIDEO_COLS].rename(columns=_VIDEO_COLS)
    df_merged_response = df_search_response.merge(df_video_info, how='inner', on='videoId')
    df_merged_response[COUNT_COLS] = df_merged_response[COUNT_COLS].fillna(0).astype(
        {
            'viewCount': int,
            'likeCount': int,
            'dislikeCount': int,
            'commentCount': int,
        })

    df_merged_response['video_url'] = VIDEO_URL + df_merged_response.videoId
    df_merged_response['channel_url'] = CHANNEL_URL + df_merged_response.channelId
    df_merged_response['good_rate'] = round((df_merged_response.likeCount / df_merged_response.viewCount) * 100, 3)
    df_merged_response['bad_rate'] = round((df_merged_response.dislikeCount / df_merged_response.viewCount) * 100, 3)
    df_merged_response['comment_rate'] = round((df_merged_response.commentCount / df_merged_response.viewCount) * 100,
                                               3)
    df_merged_response.publishedAt = df_merged_response.publishedAt.astype('datetime64[ns]').dt.strftime('%Y-%m-%d')

    df_merged_response = df_merged_response[_HEADER_DICT.keys()].rename(columns=_HEADER_DICT)

    df_merged_response.to_csv(outfile, encoding='utf-8', index=None)


if __name__ == '__main__':
    main()
