import pymysql
import aiohttp
from aiohttp import ClientSession
import asyncio
import csv
import requests
import re
import logging
import json

logging.basicConfig(filename='/home/teh/my.log', level=logging.INFO)
logger = logging.getLogger(__name__)
COUNTRY_CODE_MAPPING = {'4': 'AF', '8': 'AL', '12': 'DZ', '16': 'AS', '20': 'AD', '24': 'AO', '660': 'AI', '10': 'AQ',
                        '28': 'AG', '32': 'AR', '51': 'AM', '533': 'AW', '36': 'AU', '40': 'AT', '31': 'AZ', '44': 'BS',
                        '48': 'BH', '50': 'BD', '52': 'BB', '112': 'BY', '56': 'BE', '84': 'BZ', '204': 'BJ', '60': 'BM',
                        '64': 'BT', '68': 'BO', '535': 'BQ', '70': 'BA', '72': 'BW', '74': 'BV', '76': 'BR', '86': 'IO',
                        '96': 'BN', '100': 'BG', '854': 'BF', '108': 'BI', '116': 'KH', '120': 'CM', '124': 'CA',
                        '132': 'CV', '136': 'KY', '140': 'CF', '148': 'TD', '152': 'CL', '156': 'CN', '162': 'CX',
                        '166': 'CC', '170': 'CO', '174': 'KM', '178': 'CG', '180': 'CD', '184': 'CK', '188': 'CR',
                        '191': 'HR', '192': 'CU', '531': 'CW', '196': 'CY', '203': 'CZ', '384': 'CI', '208': 'DK',
                        '262': 'DJ', '212': 'DM', '214': 'DO', '218': 'EC', '818': 'EG', '222': 'SV', '226': 'GQ',
                        '232': 'ER', '233': 'EE', '231': 'ET', '238': 'FK', '234': 'FO', '242': 'FJ', '246': 'FI',
                        '250': 'FR', '254': 'GF', '258': 'PF', '260': 'TF', '266': 'GA', '270': 'GM', '268': 'GE',
                        '276': 'DE', '288': 'GH', '292': 'GI', '300': 'GR', '304': 'GL', '308': 'GD', '312': 'GP',
                        '316': 'GU', '320': 'GT', '831': 'GG', '324': 'GN', '624': 'GW', '328': 'GY', '332': 'HT',
                        '334': 'HM', '336': 'VA', '340': 'HN', '344': 'HK', '348': 'HU', '352': 'IS', '356': 'IN',
                        '360': 'ID', '364': 'IR', '368': 'IQ', '372': 'IE', '833': 'IM', '376': 'IL', '380': 'IT',
                        '388': 'JM', '392': 'JP', '832': 'JE', '400': 'JO', '398': 'KZ', '404': 'KE', '296': 'KI',
                        '408': 'KP', '410': 'KR', '414': 'KW', '417': 'KG', '418': 'LA', '428': 'LV', '422': 'LB',
                        '426': 'LS', '430': 'LR', '434': 'LY', '438': 'LI', '440': 'LT', '442': 'LU', '446': 'MO',
                        '807': 'MK', '450': 'MG', '454': 'MW', '458': 'MY', '462': 'MV', '466': 'ML', '470': 'MT',
                        '584': 'MH', '474': 'MQ', '478': 'MR', '480': 'MU', '175': 'YT', '484': 'MX', '583': 'FM',
                        '498': 'MD', '492': 'MC', '496': 'MN', '499': 'ME', '500': 'MS', '504': 'MA', '508': 'MZ',
                        '104': 'MM', '516': 'NA', '520': 'NR', '524': 'NP', '528': 'NL', '540': 'NC', '554': 'NZ',
                        '558': 'NI', '562': 'NE', '566': 'NG', '570': 'NU', '574': 'NF', '580': 'MP', '578': 'NO',
                        '512': 'OM', '586': 'PK', '585': 'PW', '275': 'PS', '591': 'PA', '598': 'PG', '600': 'PY',
                        '604': 'PE', '608': 'PH', '612': 'PN', '616': 'PL', '620': 'PT', '630': 'PR', '634': 'QA',
                        '642': 'RO', '643': 'RU', '646': 'RW', '638': 'RE', '652': 'BL', '654': 'SH', '659': 'KN',
                        '662': 'LC', '663': 'MF', '666': 'PM', '670': 'VC', '882': 'WS', '674': 'SM', '678': 'ST',
                        '682': 'SA', '686': 'SN', '688': 'RS', '690': 'SC', '694': 'SL', '702': 'SG', '534': 'SX',
                        '703': 'SK', '705': 'SI', '90': 'SB', '706': 'SO', '710': 'ZA', '239': 'GS', '728': 'SS',
                        '724': 'ES', '144': 'LK', '729': 'SD', '740': 'SR', '744': 'SJ', '748': 'SZ', '752': 'SE',
                        '756': 'CH', '760': 'SY', '158': 'TW', '762': 'TJ', '834': 'TZ', '764': 'TH', '626': 'TL',
                        '768': 'TG', '772': 'TK', '776': 'TO', '780': 'TT', '788': 'TN', '792': 'TR', '795': 'TM',
                        '796': 'TC', '798': 'TV', '800': 'UG', '804': 'UA', '784': 'AE', '826': 'GB', '840': 'US',
                        '581': 'UM', '858': 'UY', '860': 'UZ', '548': 'VU', '862': 'VE', '704': 'VN', '92': 'VG',
                        '850': 'VI', '876': 'WF', '732': 'EH', '887': 'YE', '894': 'ZM', '716': 'ZW', '248': 'AX'}


class Fetcher:
    def __init__(self, host, user, password, db):
        self.connect = pymysql.connect(host=host, user=user, password=password, db=db)
        self.cur = self.connect.cursor(pymysql.cursors.DictCursor)
        self.domains = []
        self.reg = re.compile(
            '^((?:http(?:s)?\:\/\/)?[a-zA-Z0-9_-]+(?:.[a-zA-Z0-9_-]+)*\.[a-zA-Z]{2,4}(?:\/[a-zA-Z0-9_]+)*(?:\/[a-zA-Z0-9_]+.[a-zA-Z]{2,4}(?:\?[a-zA-Z0-9_]+\=[a-zA-Z0-9_]+)?)?(?:\&[a-zA-Z0-9_]+\=[a-zA-Z0-9_]+)*)$')
        self.data = []
        self.responses = []

    def get_domains(self):
        self.cur.execute('SELECT title, id FROM websites')
        for x in self.cur.fetchall():
            if re.match(self.reg, x['title']):
                self.domains.append({'id': x['id'], 'title': x['title'].split('/').pop().split('www.').pop()})

    async def fetch(self, session, url):
        async with session.get(url) as response:
            return await response.text()

    async def run(self):
        tasks = []
        url = 'https://api.similarweb.com/SimilarWebAddon/{}/all?format=json'
        async with ClientSession() as session:
            for domain in self.domains[:10]:
                task = asyncio.ensure_future(self.fetch(session, url.format(domain['title'])))
                print(url.format(domain['title']))
                tasks.append(task)
            self.responses = await asyncio.gather(*tasks)
            self.responses = list(filter(lambda x: x != 'null', self.responses))
            self.responses = list(map(json.loads, self.responses))

    def process_responses(self):
        for res, domain in zip(self.responses, self.domains):
            geos = []
        for _ in range(3):
            try:
                geo = str(res['TopCountryShares'][_]['Country'])
                geos.append(COUNTRY_CODE_MAPPING.get(geo, None))
            except (KeyError, IndexError, TypeError):
                logging.error('Error handing website\'s geo\'s {}'.format(domain['title']))
                break
        try:
            self.data.append({'id': domain['id'],
                              'domain': domain['title'],
                              'category': res['Category'],
                              'geos': [geo for geo in geos if geo],
                              'language': 'qeq',
                              'traffic': 'organic' if res['TrafficSources']['Paid Referrals'] < res['TrafficSources'][
                                  'Referrals'] else 'Non-organic'})
            logging.info('Fetched website {}.'.format(domain['title']))
        except TypeError:
            logging.error('Exeception handling {} website.'.format(domain['title']))

    def fetch_domains(self):
        eta = len(self.domains)
        for i, domain in enumerate(self.domains):
            print('Fetching {} of {}.'.format(i+1, eta))
            url = 'https://api.similarweb.com/SimilarWebAddon/{}/all?format=json'.format(domain['title'])
            try:
                res = requests.get(url)
                if res.status_code == 200:
                    res = res.json()
                    geos = []
                    for _ in range(3):
                        try:
                            geo = str(res['TopCountryShares'][_]['Country'])
                            geos.append(COUNTRY_CODE_MAPPING.get(geo, None))
                        except (KeyError, IndexError, TypeError):
                            logging.error('Error handing website\'s geo\'s {} and its url {}.'.format(domain['title'], url))
                            break
                    try:
                        self.data.append({'id': domain['id'],
                                          'domain': domain['title'],
                                      'category': res['Category'],
                                      'geos': [geo for geo in geos if geo],
                                      'language': 'qeq',
                                      'traffic': 'organic' if res['TrafficSources']['Paid Referrals'] < res['TrafficSources']['Referrals'] else 'Non-organic'})
                        logging.info('Fetched website {}.'.format(domain['title']))
                    except TypeError:
                        logging.error('Exeception handling {} website.'.format(domain['title']))
                else:
                    logging.error('Error handling Similar Web handling website {}'.format(domain['title']))
            except Exception as e:
                logging.error('Error in requests.')

    def make_csv(self):
        with open('/home/teh/domains.csv', 'w+') as handler:
            csv_handler = csv.DictWriter(handler, fieldnames=['Website ID', 'Website Domain', 'Language', 'Main Geos', 'Traffic Source',
                                                       'Category'])
            csv_handler.writeheader()
            logging.info('CREATING CSV FILE.')
            for domain in self.data:
                logging.info('Inserting domain {} into csv file.'.format(domain['domain']))
                csv_handler.writerow({'Website ID': domain['id'], 'Website Domain': domain['domain'],
                                      'Language': domain['language'],
                                      'Main Geos': ','.join(geo for geo in domain['geos']),
                                      'Traffic Source': domain['traffic'], 'Category': domain['category']})


def main():
    a = Fetcher(host='localhost', user='root', password='imonomy', db='mydashboard')
    a.get_domains()
    a.fetch_domains()
    # loop = asyncio.get_event_loop()
    # future = asyncio.ensure_future(a.run())
    # loop.run_until_complete(future=future)
    # loop.close()
    # a.process_responses()
    a.make_csv()
    a.cur.close()
    a.connect.close()

main()
