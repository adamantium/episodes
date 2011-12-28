from BeautifulSoup import BeautifulSoup
from pymongo import Connection
from datetime import datetime
from datetime import timedelta
from utils import TimeSlot
from utils import DatetimeConverter
import logging
import re
import urllib2
import sys
import getopt

DATE_FORMAT_A = '%a, %d %b %Y %H:%M %Z Standard Time'
DATE_FORMAT_B = '%a, %d %b %Y %H:%M'
DAYS_OF_WEEK = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

class Collector(object):
    """This class is ancestor of all collector classes, which provides common database and logger settings,
    consistency of interface for convient automation."""
    def __init__(self, dbname, collection_prefix):
        # Connect to DB
        self.db = Connection('localhost', 27017)[dbname]
        self.collection_prefix = collection_prefix
        self.shows = self.db[collection_prefix + '_shows']
        self.timetable = self.db[collection_prefix + '_timetable']
        self.next = self.db[collection_prefix + '_next']
        self.index = self.db[collection_prefix + '_index']
        self.timestamps = self.db[collection_prefix + '_timestamps']

        # Set logger
        self.logger = logging.getLogger("EPISODES COLLECTOR")
        self.logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

        # Set Datetime Converter
        self.dc = DatetimeConverter('est', date_format='%d/%b/%y')

        # Temporary store
        self.temp_index = [];
    
    def element_is_tag(self, elem):
        return (elem.__class__.__name__ == 'Tag')
    
    def is_latest(self, target, update_time):
        latest = self.timestamps.find_one({'target': target})
        if latest:
            latest_ts = latest['timestamp']
            return latest_ts <= update_time
        else:
            return None
    
    def update_timestamp(self, target, update_time):
        self.timestamps.update({'target': target}, {'$set': {'timestamp': update_time}}, upsert=True)
    
    def update(self):
        raise NotImplementedError

class EpgCollector(Collector):
    """This collector collects information of american shows from http://epguides.com.
    """
    def __init__(self, dbname='episode', collection_prefix='us'):
        super(EpgCollector, self).__init__(dbname, collection_prefix)

    def is_valid_slot(self, td):
        if td.a:
            return (td.a.nextSibling == None)
        return False
    
    def find_csv_link(self, soup):
        list_as_csv_link = soup.findAll(text='list as')
        if len(list_as_csv_link) > 0:
            return list_as_csv_link[0].parent['href']
        else:
            tvrage_show_summary = soup.findAll(href=re.compile(r'http://www.tvrage.com/shows/id-[0-9]*$'))
            if tvrage_show_summary:
                return 'http://epguides.com/common/exportToCSV.asp?rage=' + tvrage_show_summary[0]['href'][31:]
    
    def update_one(self, show_ccn, link, start_time, duration):
        duration = timedelta(minutes = duration)
        
        # Load detail show page and check if the latest data is updated    
        page = urllib2.urlopen(link)
        soup = BeautifulSoup(page)
        update_time = datetime.strptime(soup.em.string.replace('-', ''), DATE_FORMAT_B)
        if not self.is_latest(show_ccn, update_time):
            # Get H1 tag that includes title data and imdb link data
            h1 = soup.h1.a
            title = h1.string
            # Convert normal imbd link to mobile version
            imdb_mobilelink = "http://m.imdb.com/title/tt"
            imdb_tt_number = h1['href'][27:]
            imdb_link = imdb_mobilelink + ('0' * (7 - len(imdb_tt_number))) + imdb_tt_number

            # Get CSV representation of episode data from TVRage
            csv_link = self.find_csv_link(soup)
            csv_page = urllib2.urlopen(csv_link)

            # Convert CSV string to list of episode
            csv_string = BeautifulSoup(csv_page).pre.string.replace('\r\n', '')
            csv_ep_list = csv_string.split('\n')[1:]
            ep_list = []

            # Examine each episode to classify
            for csv_ep in csv_ep_list:
                ep_info_split = csv_ep.split(',')
                ep_info = []
                for ep_info_item in ep_info_split:
                    if len(ep_info_item) > 0:
                        if (ep_info_item[0] == '"') and (ep_info_item[-1] == '"'):
                            ep_info_item = ep_info_item[1:-1]
                    ep_info.append(ep_info_item)
                idx, season, num, pcode, airdate = ep_info[:5]
                ep_title = ','.join(ep_info[5:-2])
                if ep_info[-1] == 'n':
                    is_special = False
                else:
                    is_special = True

                if idx:
                    if idx.isdigit():
                        idx = int(idx)
                    else:
                        self.logger.error('Index "%s" must be a number - %s for %s' % (idx, str(csv_ep), title))
                else:
                    if is_special:
                        idx = 0
                    else:
                        self.logger.error('Only special episodes can have a empty index - %s for %s' % (str(csv_ep), title))
                        idx = -1
                
                if season.isdigit():
                    season = int(season)
                else:
                    self.logger.error('Season "%s" must be a number - %s for %s' % (season, str(csv_ep), title)) 
                    season = -1

                if num.isdigit():
                    num = int(num)
                else:
                    if is_special != True:
                        self.logger.error('Episode number "%s" must be a number - %s for %s' % (num, str(csv_ep), title))
                        num = -1
                    else:
                        num = 0

                airdate = str(airdate.replace('"', ''))
                date_fmt = re.compile('\d\d/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/\d\d')
                if airdate == 'UNKNOWN':
                    air_dt_tuple = [9999, 12, 31, 23, 59]
                elif date_fmt.match(airdate):
                    # Combine timeslot and date to make datetime object and convert timezone to kst
                    kst_air_dt = self.dc.convert_tz('kst', (str(airdate), start_time))
                    kst_end_dt = kst_air_dt + duration
                    air_dt_tuple = self.dc.datetime_to_tuple(kst_air_dt)
                    
                    ep_ccn = show_ccn + ep_title + str(idx)
                    if kst_air_dt > self.now:
                        status = 'yet'
                        self.next.update({'ccn': ep_ccn}, {'$set': {'title': title, 'ad': air_dt_tuple}}) 
                    elif kst_end_dt < self.now:
                        status = 'aired'
                    else:
                        status = 'airing'
                elif airdate == 'UNAIRED':
                    air_dt_tuple = [0000, 1, 1, 0, 0]
                else:
                    air_dt_tuple = [-1, -1, -1, -1, -1]

                # Build up episode information list. 
                ep = [idx, season, num, pcode, air_dt_tuple, ep_title, is_special]
                ep_list.append(ep)

            # Update detail information about the show
            self.shows.update({'ccn': show_ccn}, {'$set': {'episodes': ep_list}})
            self.update_timestamp(show_ccn, update_time)

            # Upsert show into index
            self.index.update({'ccn': show_ccn}, {'$set': {'title': title, 'imdb_m': imdb_link}}, upsert=True)
            self.temp_index.append([show_ccn, title, imdb_link])
            self.logger.info('Episodes Updated for ' + show_ccn + ' titled with ' + title)  
        else:
            # Data is the latest, so no need to update
            self.logger.info('Episodes for ' + show_ccn + ' is latest.')

    def update_index(self):
        if not self.temp_index:
            # TODO:Fetch indexes to memory(temporary store) from database
            pass
        self.index.update({'ccn': 'totalindexlist'}, {'$set': {'list': self.temp_index}}, upsert=True)

    def update(self):
        self.logger.info('Start updating grid')
        # Load the grid page from www.epguides.com
        page = urllib2.urlopen('http://epguides.com/grid/')
        page_data = page.read()

        # Modify the grid page for smooth parsing
        page_data = re.sub(r'<HTML>(.|\s)*?</HEAD>', '', page_data)
        page_data = page_data.replace('<a name="hiatus">', '')
        page_data = page_data.replace('</HTML>','')

        # Parse HTML structure of the grid page with BeautifulSoup
        soup = BeautifulSoup(page_data)

        # Get the latest update time of the loaded grid page
        update_time = datetime.strptime(soup.em.string, DATE_FORMAT_A)
        self.logger.info('FINISH LOADING GRID PAGE')

        # Set current datetime for determining aired or not-yet
        self.now = self.dc.set_tz('kst', datetime.now())

        if not self.is_latest('grid', update_time):
            # Data is not latest, so update the grid data. Prepare the table list witch contains valid 
            self.logger.debug('DATA IS NOT LATEST [' + update_time.isoformat() + ']. Start updating grid.')
            table_list = soup.findAll('table')[2:]
            day_of_week_idx = 0

            # Load shows on Prime-Time TV Schedule
            self.logger.info('Update Prime-Time TV Schedule')
            # Iterate over table list, examining a table for each day of week
            for table in table_list[:7]:
                self.logger.debug(DAYS_OF_WEEK[day_of_week_idx])

                # Get list of rows, each row for a channel
                tr_list = table.findAll('tr')[1:]

                # Initialize TimeSlot to start at 8:00pm with 30 minutes per slot. If d.o.w is sunday, start at 7:00pm
                timeslot = TimeSlot([20, 0], 30)
                if day_of_week_idx == 6:
                    timeslot.set_to_start(new_start=[19, 0])
                day_of_week = DAYS_OF_WEEK[day_of_week_idx]

                # Iterate over channel rows.
                for tr in tr_list:
                    # Find all columns, representing each slot.
                    td_list = tr.findAll('td')

                    # Get channel information from first column.
                    channel = td_list[0].a
                    if channel:
                        channel_name = channel.font.string
                        channel_link = channel['href']
                    else:
                        continue
                    
                    # Start time slot for current channel row.
                    timeslot.set_to_start()

                    # Start digging into slots.
                    for td in td_list[1:]:
                        num_of_slots = int(td['colspan'])
                        start_time, end_time = timeslot.next(int(num_of_slots))
                        if self.is_valid_slot(td):
                            # Gather information about the show in current slot.                         
                            link = 'http://www.epguides.com' + td.a['href'][2:]
                            ccn = link[24:-1] # Show identifier in camelcase
                            status = 'air'
                            duration = num_of_slots * 30
                            # Upsert current show. This job perform update of the list of episodes automatically.
                            self.shows.update({'ccn': ccn}, {'$set': {'l': link, 'c': channel_name, 'w': day_of_week, 't': start_time, 'd': duration, 's': status}}, upsert=True)
                            self.logger.debug('Show Upserted - ' + ccn)
                            self.update_one(ccn, link, start_time, duration)
                day_of_week_idx += 1

            # Load shows on hiatus
            self.logger.info('Update Hiatus Shows')
            table = table_list[7]
            li_list = table.findAll('li')
            for li in li_list:
                link = 'http://www.epguides.com' + li.a['href'][2:]
                ccn = link[24:-1]
                return_date = li.span.string[9:]
                channel_name = 'UNKNOWN'
                day_of_week = 'UNKNOWN'
                start_time = [0, 0]
                end_time = [0, 0]
                duration = 0
                status = 'hiatus'
                self.shows.update({'ccn': ccn}, {'$set': {'l': link, 'c': channel_name, 'w': day_of_week, 't': start_time, 'd': duration, 's': status}}, upsert=True)
                self.logger.debug('Show Upserted - ' + ccn)
                self.update_one(ccn, link, start_time, duration)
            
            # TODO: Load shows that are cancelled/ended
            self.update_timestamp('grid', update_time)
        else:
            self.logger.info('DATA IS LATEST [' + update_time.isoformat() + ']. Start updating episodes.')
            # If grid data is latest, update each show detail iterating grid.
            for show in self.shows.find():
                self.update_one(show['ccn'])
        
        #

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg

# This is test purpose execution
def main(argv=None):
    epgc = EpgCollector()
    if argv is None:
        argv = sys.argv
    if argv[1:]:
        if 'r' in argv[1]:
            Connection('localhost', 27017).drop_database('episode')
        if 't' in argv[1]:
            print 'TEST START'
    epgc.update()

if __name__ == "__main__":
    sys.exit(main())
