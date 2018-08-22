"""Generate RSS feed file and place in bucket when file uploaded to S3"""
import logging
import operator
import os
import pprint
import boto3
from chalice import Chalice
from feedgen.feed import FeedGenerator

BUCKET = os.getenv('S3_BUCKET')
app = Chalice(app_name='lesscast')
app.log.setLevel(logging.DEBUG)

@app.on_s3_event(bucket=BUCKET, events=['s3:ObjectCreated:*', 's3:ObjectRemoved:*'])
def handle(event):
    (_, ext) = os.path.splitext(event.key)
    mapper = {
            '.mp3': handle_audio,
            '.m4a': handle_audio,
            '.mkv': handle_video_to_audio,
            '.webm': handle_video_to_audio,
            '.mp4': handle_video_to_audio,
            }
    if ext not in mapper.keys():
        return
    app.log.debug('Got event: %s', pprint.pformat(event.to_dict()))
    mapper[ext](event)

def _encode(job_input, outputs):
    app.log.debug('job_input: %s', pprint.pformat(job_input))
    app.log.debug('outputs: %s', pprint.pformat(outputs))
    app.log.info('Encoding %s', job_input['Key'])
    transcoder = boto3.client('elastictranscoder')
    pipeline = os.getenv('PIPELINE_ID')
    response = transcoder.create_job(
            PipelineId=pipeline,
            Input=job_input,
            Outputs=outputs)
    app.log.info('Started ElasticTranscoder Job: %s', response['Job']['Id'])

def _get_valid_files(objectsum):
    return objectsum.key.endswith(('.mp3', '.m4a'))

def handle_video_to_audio(event):
    if 'ObjectRemoved' in event.to_dict()['Records'][0]['eventName']:
        app.log.info('Video deleted. Nothing to do')
        return
    app.log.info('Processing video %s', event.key)
    (fn, _) = os.path.splitext(event.key)
    fn = os.path.basename(fn)
    # 160K AAC
    preset = '1351620000001-100120'
    job_input = dict(Key=event.key, FrameRate='auto')
    outputs = list()
    outputs.append(dict(
        Key='audio/' + fn + '.m4a',
        PresetId=preset,
        ))
    _encode(job_input, outputs)

def handle_audio(event):
    _gen_rss(event)

def _gen_rss(event):
    """Generate RSS feed file and place in bucket when file uploaded to S3"""
    s3 = boto3.resource('s3')
    sbucket = s3.Bucket(event.bucket)
    dbucket = s3.Bucket('lesscast-web')
    fg = FeedGenerator()
    fg.title('Lesscast Uploads')
    fg.description('Created by lesscast')
    fg.link(href='https://s3.amazonaws.com/{}/rss.xml'.format(dbucket.name))
    fg.load_extension('podcast')
    fg.podcast.itunes_category('Technology', 'Podcasting')
    keyfunc = operator.attrgetter('last_modified')
    iterator = filter(_get_valid_files, sbucket.objects.all())
    for objsum in sorted(iterator, key=keyfunc):
        app.log.info('Adding %s to feed', objsum.key)
        pub_url = 'https://s3.amazonaws.com/{}/{}'.format(
            sbucket.name, objsum.key)
        obj = objsum.Object()
        acl = obj.Acl()
        acl.put(ACL='public-read')
        fe = fg.add_entry()
        fe.id(pub_url)
        fe.link(href=pub_url)
        fe.title(os.path.basename(obj.key.rstrip('.mp3')))
        fe.description('added by lesscast')
        fe.enclosure(pub_url, 0, 'audio/mpeg')
    rss_content = fg.rss_str(pretty=True)
    dbucket.put_object(ACL='public-read', Key='rss.xml', Body=rss_content)
    app.log.info('Complete')
