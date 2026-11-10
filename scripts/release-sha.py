#!/usr/bin/env python3
"""Print path-scoped commits visible on main. No mutation; requires read_api PAT."""
import os,json,urllib.parse,urllib.request,sys
ip=os.environ['HOST_IP']; token=os.environ['GITLAB_TOKEN']
project=urllib.parse.quote(os.environ.get('GITLAB_PROJECT','data360/flows-nifi'),safe='')
path=sys.argv[1] if len(sys.argv)>1 else 'flows/shared/kafka-to-postgres.json'
query=urllib.parse.urlencode({'ref_name':'main','path':path,'per_page':10})
url=f'http://{ip}:18080/api/v4/projects/{project}/repository/commits?{query}'
request=urllib.request.Request(url,headers={'PRIVATE-TOKEN':token})
with urllib.request.urlopen(request,timeout=20) as r: commits=json.load(r)
if not commits:raise SystemExit('No main commits for snapshot; check path and MR')
for c in commits:print(c['id'],c['title'])
