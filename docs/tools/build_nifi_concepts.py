#!/usr/bin/env python3
"""Build the illustrated NiFi primer (SVG, PNG and a Unicode Vietnamese PDF).

Dependencies: reportlab, markdown-it-py, pymupdf. Run from the repo root:
  python docs/tools/build_nifi_concepts.py
A Unicode TTF is required (Arial on macOS, DejaVu Sans on Linux), or set
NIFI_DOC_FONT and NIFI_DOC_BOLD_FONT. Monospace: Menlo/DejaVu Sans Mono
or NIFI_DOC_MONO_FONT. This script never connects to the lab.
"""
import copy
import html
import math
import os
import pathlib
import re

import pymupdf as fitz
from markdown_it import MarkdownIt
from reportlab import rl_config
from reportlab.graphics import renderPDF, renderSVG
from reportlab.graphics.shapes import Drawing, Line, Polygon, Rect, String
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.utils import simpleSplit
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    BaseDocTemplate, Flowable, Frame, KeepTogether, LongTable, PageBreak,
    PageTemplate, Paragraph, Preformatted, Spacer, TableStyle,
)
from reportlab.platypus.tableofcontents import TableOfContents

def RoundRect(x, y, width, height, radius, **kwargs):
    return Rect(x, y, width, height, rx=radius, ry=radius, **kwargs)


ROOT = pathlib.Path(__file__).resolve().parents[2]
ASSETS = ROOT / 'docs/assets/nifi-concepts'
PDF_DIR = ROOT / 'output/pdf'
QA = ROOT / 'tmp/pdfs/nifi-concepts'
SOURCE = ROOT / 'docs/NIFI_CONCEPTS_VI.md'
for directory in (ASSETS, PDF_DIR, QA):
    directory.mkdir(parents=True, exist_ok=True)


def font_path(env_name, candidates):
    selected = os.environ.get(env_name)
    if selected:
        return selected
    for candidate in candidates:
        if pathlib.Path(candidate).exists():
            return candidate
    raise RuntimeError(f'Set {env_name} to a Unicode TTF font')


pdfmetrics.registerFont(TTFont('NifiSans', font_path('NIFI_DOC_FONT', [
    '/System/Library/Fonts/Supplemental/Arial.ttf',
    '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',
])))
pdfmetrics.registerFont(TTFont('NifiSansBold', font_path('NIFI_DOC_BOLD_FONT', [
    '/System/Library/Fonts/Supplemental/Arial Bold.ttf',
    '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf',
])))
pdfmetrics.registerFont(TTFont('NifiMono', font_path('NIFI_DOC_MONO_FONT', [
    '/System/Library/Fonts/Menlo.ttc',
    '/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf',
]), subfontIndex=0))
pdfmetrics.registerFontFamily('NifiSans', normal='NifiSans', bold='NifiSansBold',
                            italic='NifiSans', boldItalic='NifiSansBold')
rl_config.warnOnMissingFontGlyphs = 1

C = {key: colors.HexColor(value) for key, value in {
    'ink': '#172C46', 'muted': '#52677E', 'border': '#D9E2EB', 'paper': '#FFFFFF',
    'soft': '#F3F6FA', 'blue': '#3264C8', 'bluebg': '#EFF4FF', 'teal': '#087F81',
    'tealbg': '#EAF7F5', 'purple': '#7950AD', 'purplebg': '#F3EEFA',
    'amber': '#AF681A', 'amberbg': '#FFF6E7', 'red': '#B64B50', 'redbg': '#FCEFF0',
}.items()}


class Figure:
    def __init__(self, height, title, subtitle):
        self.h = height
        self.d = Drawing(1200, height)
        self.d.add(Rect(0, 0, 1200, height, fillColor=C['paper'], strokeColor=None))
        self.text(40, 38, title, 30, bold=True)
        self.text(40, 73, subtitle, 21, color='muted')

    def text(self, x, y, text, size=22, color='ink', bold=False, anchor='start'):
        self.d.add(String(x, self.h - y, text, fontName='NifiSansBold' if bold else 'NifiSans',
                          fontSize=size, fillColor=C[color], textAnchor=anchor))

    def line(self, points, color='ink', dashed=False, arrow=True, width=3):
        for a, b in zip(points, points[1:]):
            self.d.add(Line(a[0], self.h-a[1], b[0], self.h-b[1], strokeColor=C[color],
                            strokeWidth=width, strokeDashArray=[8, 6] if dashed else None))
        if arrow:
            a, b = points[-2:]
            angle = math.atan2(b[1]-a[1], b[0]-a[0])
            coords = [b[0], self.h-b[1]]
            for sign in (-1, 1):
                coords += [b[0]-14*math.cos(angle)+sign*6*math.sin(angle),
                           self.h-(b[1]-14*math.sin(angle)-sign*6*math.cos(angle))]
            self.d.add(Polygon(coords, fillColor=C[color], strokeColor=None))

    def box(self, x, y, w, h, title, lines=(), tone='blue', title_size=25, body_size=21):
        self.d.add(RoundRect(x, self.h-y-h, w, h, 14, fillColor=C.get(tone+'bg', C['soft']),
                             strokeColor=C.get(tone, C['border']), strokeWidth=1.8))
        cursor = y+34
        for part in title.split('\n'):
            size = title_size
            while pdfmetrics.stringWidth(part, 'NifiSansBold', size) > w-34:
                size -= .5
            self.text(x+17, cursor, part, size, color=tone if tone in C else 'ink', bold=True)
            cursor += size+7
        cursor += 7
        for line in lines:
            for part in simpleSplit(line, 'NifiSans', body_size, w-34):
                self.text(x+17, cursor, part, body_size)
                cursor += body_size+7
        if cursor > y+h+body_size:
            raise RuntimeError(f'Figure text overflow: {title}')


def figures():
    out = {}
    f = Figure(760, 'BA LỚP ĐỂ HIỂU MỘT FLOW', 'Đường dữ liệu, nguồn cấu hình và bằng chứng vận hành')
    f.text(40, 115, 'CẤU HÌNH', 19, 'teal', True)
    f.box(235, 130, 330, 135, 'Parameter Context', ['db.schema, db.url, kafka.topic'], 'teal')
    f.box(650, 130, 510, 135, 'Controller Services', ['KafkaConnection · PostgresPool', 'TripJsonReader'], 'teal')
    f.line([(395,265),(395,305),(315,305),(315,365)], 'teal', True)
    f.line([(490,265),(490,320),(775,320),(775,365)], 'teal', True)
    f.line([(905,265),(905,295),(815,295),(815,365)], 'teal', True)
    f.text(40, 345, 'DỮ LIỆU', 19, 'blue', True)
    for x,w,title,lines in [(40,150,'Kafka',['nguồn']), (225,200,'ConsumeKafka',['đọc record']),
                              (460,150,'Queue',['chờ xử lý']), (650,235,'PutDatabaseRecord',['ghi dữ liệu']),
                              (925,235,'PostgreSQL',['row đích'])]:
        f.box(x,365,w,120,title,lines,'blue',title_size=23)
    for a,b in [(190,225),(425,460),(610,650),(885,925)]:
        f.line([(a+4,425),(b-7,425)],'blue')
    f.text(40, 560, 'QUAN SÁT', 19, 'purple', True)
    f.box(300,580,530,115,'Data Provenance / Bulletin / Log',['FlowFile đã qua đâu? Có lỗi gì?'],'purple')
    f.box(870,580,290,115,'Query database',['Row có đúng không?'],'purple')
    f.line([(550,485),(550,570)],'purple',True)
    f.line([(1042,485),(1042,570)],'purple',True)
    f.text(40,735,'Mũi tên liền: dữ liệu. Nét đứt: cấu hình hoặc thông tin quan sát.',20,'muted')
    out['01-three-layers']=f.d

    f=Figure(760,'FLOWFILE: CONTENT + ATTRIBUTES','Một đơn vị dữ liệu có payload và metadata đi cùng nhau')
    f.d.add(RoundRect(25,760-105-480,1150,480,18,fillColor=C['soft'],strokeColor=C['border'],strokeWidth=2))
    f.text(50,145,'FlowFile UUID: 3d2a6e70-...  (ID kỹ thuật)',23,bold=True)
    f.box(50,175,585,375,'CONTENT',[], 'blue')
    code=['{','  "event_id": "stag-v1-001",','  "customer_id": "C001",','  "amount": 125.50,','  "event_time": "2026-11-11T01:00:00Z"','}']
    for i,line in enumerate(code):
        f.d.add(String(72,760-(258+i*34),line,fontName='NifiSans',fontSize=20,fillColor=C['ink']))
    f.box(675,175,475,375,'ATTRIBUTES', ['uuid = 3d2a6e70-...', 'kafka.topic = trips-staging',
          'kafka.partition = 1', 'kafka.offset = 0', 'flow.release = v1', 'local.market = STAG'],'teal',body_size=22)
    f.box(50,615,1100,110,'PROPERTY NẰM TRÊN COMPONENT',
          ['Ví dụ trên PutDatabaseRecord: Schema Name = #{db.schema}'], 'amber', title_size=24,body_size=23)
    out['02-flowfile']=f.d

    f=Figure(810,'PHẠM VI CỦA NHÓM VÀ CẤU HÌNH','PG ngang hàng không tự chia sẻ mọi service và parameter')
    f.d.add(RoundRect(345,810-115-605,810,605,16,fillColor=C['soft'],strokeColor=C['border'],strokeWidth=2))
    f.text(372,152,'MARKET_RUNTIME  /  canvas cha',25,bold=True)
    f.box(385,190,730,270,'kafka-to-postgres  /  SHARED PG',
          ['Processors: ConsumeKafka, StampFlowVersion, PutDatabaseRecord',
           'Services: KafkaConnection, PostgresPool, TripJsonReader'],'blue',body_size=22)
    f.box(425,360,650,70,'PG con (nếu có): dùng được service của shared PG',[],'blue',title_size=21)
    f.box(385,510,730,170,'LOCAL_AUDIT  /  PG NGANG HÀNG',
          ['LocalMarketTag, AuditSuccess, input ports và queue giữ lỗi',
           'Không tự thấy PostgresPool nằm trong shared PG'],'purple',title_size=24,body_size=22)
    f.box(35,190,265,160,'shared-runtime',['db.schema = staging','context của instance'],'teal',body_size=22)
    f.line([(300,265),(375,265)],'teal',True)
    f.text(307,245,'bind',19,'teal')
    f.box(35,405,265,275,'CẦN PHÂN BIỆT',
          ['Service có phạm vi theo cây PG.', 'Parameter Context của PG con phải được gắn rõ ràng.',
           'Context Inheritance là cơ chế riêng.'],'amber',title_size=21,body_size=20)
    f.text(40,775,'Đặt đúng scope để component nhìn thấy đúng tài nguyên và snapshot có đúng ranh giới.',21,'muted')
    out['03-scope']=f.d

    f=Figure(815,'MỞ ĐÂU TRÊN GIAO DIỆN NIFI 2.6?','Bản đồ chức năng - không phải screenshot')
    f.box(40,140,285,110,'Controller Settings',['menu toàn cục'],'blue')
    f.box(410,115,750,200,'Các tab trong Controller Settings',
          ['General · Management Controller Services', 'Reporting Tasks · Flow Analysis Rules',
           'Registry Clients · Parameter Providers'],'blue',body_size=23)
    f.line([(325,195),(400,195)],'blue')
    f.box(40,365,285,110,'Parameter Contexts',['menu toàn cục'],'teal')
    f.box(410,365,750,110,'shared-runtime / vault-preview',
          ['Edit → Parameters → Apply'],'teal',body_size=23)
    f.line([(325,420),(400,420)],'teal')
    f.box(40,540,285,110,'Data Provenance',['menu toàn cục'],'purple')
    f.box(410,540,750,110,'Tìm lịch sử FlowFile', ['UUID → event details → attributes / lineage'],'purple',body_size=23)
    f.line([(325,595),(400,595)],'purple')
    f.box(40,705,1120,75,'TRONG CANVAS PG: chuột phải → Controller Services → service của flow',[], 'amber',title_size=23)
    out['04-ui-map']=f.d

    f=Figure(850,'ĐƯA CẤU HÌNH TỪ VAULT VÀO NIFI','Các mũi tên ở hình này là cấu hình; không có FlowFile đi qua Vault')
    f.text(40,128,'A. BRIDGE CỦA BÀI CHÍNH',22,'blue',True)
    for x,w,title,body in [(40,245,'Vault',['kv/staging hoặc kv/runtime']),
                            (330,250,'sync-params.py',['đọc và apply qua API']),
                            (625,290,'shared-runtime',['Parameter Context local']),
                            (960,200,'Shared PG',['#{parameter}'])]:
        f.box(x,155,w,150,title,body,'blue',body_size=21)
    for a,b in [(285,330),(580,625),(915,960)]:f.line([(a+3,230),(b-8,230)],'blue',True)
    f.text(40,400,'B. NATIVE PARAMETER PROVIDER TRÊN UI',22,'teal',True)
    for x,w,title,body in [(40,200,'Vault',['kv/preview']),
                            (285,245,'VaultPreviewClient',['kết nối + xác thực']),
                            (575,245,'VaultPreviewProvider',['Fetch Parameters']),
                            (865,295,'vault-preview',['context sau Apply'])]:
        f.box(x,430,w,155,title,body,'teal',title_size=23,body_size=21)
    for a,b in [(240,285),(530,575),(820,865)]:f.line([(a+3,507),(b-8,507)],'teal',True)
    f.box(40,665,1120,135,'FETCH ≠ APPLY ≠ BIND ≠ START',
          ['Fetch lấy giá trị; Apply cập nhật context; Bind gắn context vào PG; Start chạy flow.',
           'vault-preview chưa bind shared PG trong bài mở rộng.'],'amber',title_size=25,body_size=22)
    out['05-parameters']=f.d

    f=Figure(850,'PROVENANCE KỂ LẠI HÀNH TRÌNH','Đọc cùng UUID, component và Details; đối chiếu thêm kết quả hệ đích')
    items=[('RECEIVE','ConsumeKafka','nhận bản tin'),('ATTRIBUTES\nMODIFIED','StampFlowVersion','thêm flow.release'),
           ('SEND','PutDatabaseRecord','gửi tới DB'),('ATTRIBUTES\nMODIFIED','LocalMarketTag','thêm local.market'),
           ('DROP','AuditSuccess','auto-terminate')]
    for i,(title,name,note) in enumerate(items):
        x=40+i*225
        f.box(x,150,210,205,title,[name,note],'purple',title_size=24,body_size=20)
        if i<4:f.line([(x+212,252),(x+222,252)],'purple',width=2)
    f.box(45,450,525,215,'UPDATE + ID chưa tồn tại',
          ['Có thể hoàn tất với 0 row.', 'FlowFile vẫn có thể đi nhánh success.', 'SEND / DROP chưa đủ kết luận DB có row.'],
          'red',title_size=25,body_size=23)
    f.box(630,450,525,215,'UPSERT + ID chưa tồn tại',
          ['Tạo row nếu hợp đồng/schema hợp lệ.', 'Query theo event_id để xác nhận.', 'Kiểm tra cả giá trị các cột mong đợi.'],
          'teal',title_size=25,body_size=23)
    f.text(45,755,'Bằng chứng mạnh trong lab: đường đi FlowFile + query PostgreSQL đúng event_id.',23,'ink',True)
    f.text(45,798,'DROP ở AuditSuccess với Details auto-terminate không phát lệnh DELETE row trong DB.',21,'muted')
    out['06-provenance']=f.d

    f=Figure(800,'CÙNG FLOW, TRIỂN KHAI RIÊNG TỪNG THỊ TRƯỜNG','Version cấu trúc flow và cấu hình/dữ liệu môi trường là các lớp khác nhau')
    f.box(40,140,255,155,'NiFi staging',['Save / Commit Local Changes'],'blue',title_size=26,body_size=22)
    f.box(355,140,300,155,'GitLab: staging',['S1 - snapshot JSON', 'writer có quyền ghi'],'blue',title_size=26,body_size=22)
    f.box(805,140,350,155,'GitLab: main',['M1 - SHA đã duyệt', 'reader đọc để rollout'],'teal',title_size=27,body_size=22)
    f.line([(300,220),(345,220)],'blue')
    f.line([(663,220),(795,220)],'teal')
    f.text(665,180,'Review + MR',20,'teal',True)
    f.line([(980,295),(980,370),(525,370),(525,455)],'teal')
    f.line([(980,370),(980,455)],'teal')
    f.text(360,418,'Import / Change Version riêng',21,'teal')
    f.text(800,418,'Import / Change Version riêng',21,'teal')
    f.box(340,470,370,215,'NiFi VN: cùng M1',
          ['Context: VN', 'Kafka/DB: 19092 / 15432', 'LOCAL_AUDIT: VN'],'blue',title_size=27,body_size=23)
    f.box(785,470,370,215,'NiFi INDO: cùng M1',
          ['Context: INDO', 'Kafka/DB: 29092 / 25432', 'LOCAL_AUDIT: INDO'],'purple',title_size=27,body_size=23)
    f.text(40,750,'Merge cập nhật Git. Mỗi market rollout riêng; queue, offsets và row DB vẫn là state local.',21,'muted')
    out['07-promotion']=f.d
    return out


FIGURES = figures()
for stem, drawing in FIGURES.items():
    vector = QA / (stem+'.pdf')
    renderPDF.drawToFile(drawing, str(vector))
    renderSVG.drawToFile(drawing, str(ASSETS / (stem+'.svg')))
    svg = (ASSETS / (stem+'.svg')).read_text()
    svg = svg.replace('font-family: NifiSansBold;', 'font-family: Arial; font-weight: bold;').replace('NifiSansBold','Arial').replace('NifiSans','Arial')
    (ASSETS / (stem+'.svg')).write_text(svg)
    with fitz.open(vector) as document:
        document[0].get_pixmap(matrix=fitz.Matrix(1.5,1.5), alpha=False).save(ASSETS / (stem+'.png'))

PAGE_W, PAGE_H = 595.276, 841.89
MARGIN=48
WIDTH=PAGE_W-2*MARGIN
styles=getSampleStyleSheet()
styles.add(ParagraphStyle('BodyVN',fontName='NifiSans',fontSize=10.3,leading=14.6,
                         textColor=C['ink'],spaceAfter=7,allowWidows=0,allowOrphans=0))
styles.add(ParagraphStyle('H1VN',parent=styles['BodyVN'],fontName='NifiSansBold',fontSize=21,
                         leading=26,spaceBefore=4,spaceAfter=13,keepWithNext=True))
styles.add(ParagraphStyle('H2VN',parent=styles['BodyVN'],fontName='NifiSansBold',fontSize=13,
                         leading=18,spaceBefore=13,spaceAfter=7,keepWithNext=True,textColor=C['teal']))
styles.add(ParagraphStyle('CaptionVN',parent=styles['BodyVN'],fontSize=9.1,leading=13,
                         textColor=C['muted'],spaceAfter=14))
styles.add(ParagraphStyle('BulletVN',parent=styles['BodyVN'],leftIndent=14,firstLineIndent=-10,spaceAfter=6))
styles.add(ParagraphStyle('NumberVN',parent=styles['BodyVN'],leftIndent=18,firstLineIndent=-17,spaceAfter=7))
styles.add(ParagraphStyle('CellVN',parent=styles['BodyVN'],fontSize=9.1,leading=12.8,spaceAfter=0,wordWrap='CJK'))
styles.add(ParagraphStyle('HeadCellVN',parent=styles['CellVN'],fontName='NifiSansBold',textColor=colors.white))
styles.add(ParagraphStyle('CodeVN',fontName='NifiMono',fontSize=8.6,leading=12,
                         leftIndent=9,rightIndent=7,spaceBefore=5,spaceAfter=11,
                         backColor=C['soft'],borderPadding=9,textColor=C['ink']))
styles.add(ParagraphStyle('TOCVN',parent=styles['BodyVN'],fontSize=10.5,leading=19,leftIndent=0,
                         firstLineIndent=0,spaceBefore=3,spaceAfter=3))
styles.add(ParagraphStyle('CoverVN',parent=styles['BodyVN'],fontName='NifiSansBold',fontSize=36,
                         leading=43,spaceAfter=20))
styles.add(ParagraphStyle('CoverSubVN',parent=styles['BodyVN'],fontSize=14.5,leading=22,spaceAfter=18))
styles.add(ParagraphStyle('KickerVN',parent=styles['BodyVN'],fontName='NifiSansBold',fontSize=10,
                         leading=14,textColor=C['teal'],spaceAfter=18))

md=MarkdownIt('commonmark').enable('table')


def inline(children):
    out=[]
    for item in children or []:
        kind=item.type
        if kind=='text':out.append(html.escape(item.content))
        elif kind=='code_inline':out.append('<font name="NifiSans" size="9">'+html.escape(item.content)+'</font>')
        elif kind=='strong_open':out.append('<b>')
        elif kind=='strong_close':out.append('</b>')
        elif kind=='em_open':out.append('<i>')
        elif kind=='em_close':out.append('</i>')
        elif kind=='softbreak':out.append(' ')
        elif kind=='hardbreak':out.append('<br/>')
        elif kind=='link_open':
            href=item.attrGet('href') or ''
            # Repo-relative references are useful in Markdown; PDF links use GitHub.
            if not re.match(r'https?://',href):
                href='https://github.com/ducpham4010/nifi-multi-market-lab/blob/main/docs/'+href
            out.append('<link href="'+html.escape(href,quote=True)+'" color="#3264C8">')
        elif kind=='link_close':out.append('</link>')
    return ''.join(out)


class DiagramFlowable(Flowable):
    def __init__(self, drawing, max_height=355):
        super().__init__()
        self.drawing=drawing
        self.scale=min(WIDTH/drawing.width,max_height/drawing.height)
        self.width=drawing.width*self.scale
        self.height=drawing.height*self.scale
        self.hAlign='CENTER'

    def draw(self):
        self.canv.saveState()
        self.canv.scale(self.scale,self.scale)
        renderPDF.draw(self.drawing,self.canv,0,0)
        self.canv.restoreState()


class GuideDoc(BaseDocTemplate):
    def __init__(self,path):
        super().__init__(str(path),pagesize=(PAGE_W,PAGE_H),leftMargin=MARGIN,rightMargin=MARGIN,
                         topMargin=53,bottomMargin=48,title='Hiểu NiFi từ những thành phần đầu tiên',
                         author='NiFi multi-market lab',subject='NiFi 2.6 - tài liệu nhập môn tiếng Việt')
        frame=Frame(MARGIN,48,WIDTH,PAGE_H-101,id='body',leftPadding=0,rightPadding=0,topPadding=0,bottomPadding=0)
        self.addPageTemplates(PageTemplate(id='standard',frames=[frame],onPage=self.page_furniture))
        self.heading_number=0

    def page_furniture(self,canvas,doc):
        canvas.saveState()
        if doc.page>1:
            canvas.setFont('NifiSans',8)
            canvas.setFillColor(C['muted'])
            canvas.drawString(MARGIN,PAGE_H-28,'HIỂU NIFI  /  NIFI 2.6.0  /  LAB VN - INDO')
            canvas.setStrokeColor(C['border']);canvas.setLineWidth(.5)
            canvas.line(MARGIN,PAGE_H-36,PAGE_W-MARGIN,PAGE_H-36)
        canvas.setFont('NifiSans',8)
        canvas.setFillColor(C['muted'])
        canvas.drawString(MARGIN,25,'Tài liệu khái niệm · 11/11/2026')
        canvas.drawRightString(PAGE_W-MARGIN,25,str(doc.page))
        canvas.restoreState()

    def afterFlowable(self,flowable):
        if isinstance(flowable,Paragraph) and flowable.style.name=='H1VN' and flowable.getPlainText() != 'Lộ trình đọc':
            title=flowable.getPlainText()
            key='chapter-'+title.split('.',1)[0]
            self.canv.bookmarkPage(key)
            self.canv.addOutlineEntry(title,key,0,False)
            self.notify('TOCEntry',(0,title,self.page,key))


def table_from_tokens(tokens,start):
    rows=[];row=[];i=start
    while i<len(tokens):
        item=tokens[i]
        if item.type=='tr_open':row=[]
        elif item.type in ('th_open','td_open'):
            value=inline(tokens[i+1].children)
            row.append(value)
        elif item.type=='tr_close':rows.append(row)
        elif item.type=='table_close':break
        i+=1
    n=len(rows[0])
    ratios={2:[.29,.71],3:[.25,.34,.41],4:[.28,.24,.24,.24]}.get(n,[1/n]*n)
    cells=[[Paragraph(value,styles['HeadCellVN'] if j==0 else styles['CellVN'])
            for value in row] for j,row in enumerate(rows)]
    table=LongTable(cells,colWidths=[WIDTH*x for x in ratios],repeatRows=1,hAlign='LEFT')
    table.setStyle(TableStyle([
        ('BACKGROUND',(0,0),(-1,0),C['ink']),('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white,C['soft']]),
        ('VALIGN',(0,0),(-1,-1),'TOP'),('LEFTPADDING',(0,0),(-1,-1),8),
        ('RIGHTPADDING',(0,0),(-1,-1),8),('TOPPADDING',(0,0),(-1,-1),5),
        ('BOTTOMPADDING',(0,0),(-1,-1),5),('LINEBELOW',(0,0),(-1,0),.7,C['ink']),
        ('LINEBELOW',(0,1),(-1,-1),.35,C['border']),
    ]))
    if len(rows) <= 9:
        table = KeepTogether([table])
    return [Spacer(1,4),table,Spacer(1,12)],i


def build_pdf():
    tokens=md.parse(SOURCE.read_text())
    story=[Spacer(1,34),Paragraph('TỪ KHÁI NIỆM ĐẾN DÒNG DỮ LIỆU',styles['KickerVN']),
           Paragraph('Hiểu NiFi<br/>từ những thành phần<br/>đầu tiên',styles['CoverVN']),
           Paragraph('Parameter Contexts, Controller Services, Controller Settings, Data Provenance và cách chúng phối hợp.',styles['CoverSubVN']),
           Spacer(1,14),DiagramFlowable(FIGURES['01-three-layers'],max_height=280),Spacer(1,18),
           Paragraph('Dành cho người mới · Ví dụ xuyên suốt Kafka → NiFi → PostgreSQL<br/>Áp dụng cho NiFi 2.6.0 trong lab VN / INDO',styles['CoverSubVN']),PageBreak(),
           Paragraph('Lộ trình đọc',styles['H1VN']),
           Paragraph('Chương 1-6: nhìn đường dữ liệu. Chương 7-9: hiểu cấu hình và phạm vi. Chương 10-14: theo dõi, vận hành và phát hành. Chương 15-17: tự kiểm tra và tra cứu.',styles['BodyVN'])]
    toc=TableOfContents();toc.levelStyles=[styles['TOCVN']];toc.dotsMinLevel=0
    story.extend([toc,PageBreak()])
    list_stack=[];list_item=False;first_chapter=True;i=0
    while i<len(tokens):
        item=tokens[i]
        if item.type=='heading_open':
            level=int(item.tag[1]);title=tokens[i+1].content
            if level==1:
                i+=3;continue
            if level==2:
                if not first_chapter:story.append(PageBreak())
                first_chapter=False
                story.append(Paragraph(inline(tokens[i+1].children),styles['H1VN']))
            else:story.append(Paragraph(inline(tokens[i+1].children),styles['H2VN']))
            i+=3;continue
        if first_chapter:
            i+=1;continue
        if item.type in ('bullet_list_open','ordered_list_open'):
            list_stack.append({'type':item.type,'number':int(item.attrGet('start') or 1)})
        elif item.type in ('bullet_list_close','ordered_list_close'):
            if list_stack:list_stack.pop()
        elif item.type=='list_item_open':list_item=True
        elif item.type=='paragraph_open':
            tok=tokens[i+1]
            images=[c for c in tok.children or [] if c.type=='image']
            if images:
                for image in images:
                    stem=pathlib.Path(image.attrGet('src')).stem
                    story.append(DiagramFlowable(FIGURES[stem]))
                    story.append(Spacer(1,7))
            else:
                text=inline(tok.children)
                if list_stack and list_item:
                    state=list_stack[-1]
                    if state['type']=='ordered_list_open':
                        text=str(state['number'])+'. '+text;state['number']+=1;style=styles['NumberVN']
                    else:text='• '+text;style=styles['BulletVN']
                    list_item=False
                else:style=styles['CaptionVN'] if tok.children and tok.children[0].type=='em_open' else styles['BodyVN']
                if text.rstrip().endswith(':'):
                    style = ParagraphStyle('LeadInVN', parent=style, keepWithNext=True)
                story.append(Paragraph(text,style))
            i+=3;continue
        elif item.type=='fence':
            code_style = styles['CodeVN']
            glyphs = pdfmetrics.getFont('NifiMono').face.charToGlyph
            if any(ord(char) not in glyphs for char in item.content if not char.isspace()):
                code_style = ParagraphStyle('CodeFallbackVN', parent=code_style, fontName='NifiSans')
            story.append(Preformatted(item.content.rstrip(),code_style,maxLineLength=91))
        elif item.type=='table_open':
            elements,i=table_from_tokens(tokens,i);story.extend(elements)
        i+=1
    path=PDF_DIR/'NIFI_CONCEPTS_VI.pdf'
    GuideDoc(path).multiBuild(story)
    return path


if __name__=='__main__':
    output=build_pdf()
    for preview in QA.glob('page-*.png'):
        preview.unlink()
    with fitz.open(output) as document:
        for index,page in enumerate(document):
            page.get_pixmap(matrix=fitz.Matrix(1.2,1.2),alpha=False).save(QA/f'page-{index+1:02d}.png')
        print(f'Built {len(FIGURES)} figures and {len(document)} PDF pages: {output}')
