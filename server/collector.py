from BeautifulSoup import BeautifulSoup
from pymongo import Connection
from datetime import datetime
from utils import TimeSlot
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
        self.grid = self.db[collection_prefix + '_grid']
        self.index = self.db[collection_prefix + '_index']
        self.timestamps = self.db[collection_prefix + '_timestamps']

        # Set logger
        self.logger = logging.getLogger("EPISODES")
        self.logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

        # Temporary Store
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

    def update_one(self):
        raise NotImplementedError
    
    def update_grid(self):
        raise NotImplementedError
    
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
    
    def update_one(self, show_ccn):
        show = self.grid.find_one({'ccn': show_ccn})

        # FIXME: Add exception handler instead of simple print string
        if not show:
            print "Show must be inserted first"
            return None

        # Load detail show page and check if the latest data is updated    
        page = urllib2.urlopen(show['link'])
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
            csv_string = BeautifulSoup(csv_page).textarea.string.replace('\r\n', '')
            csv_ep_list = csv_string.split('\n')[1:]
            ep_list = [csv_ep.split(',') for csv_ep in csv_ep_list]

            # Update detail information about the show
            self.grid.update({'ccn': show_ccn}, {'$set': {'episodes': ep_list}})
            self.update_timestamp(show_ccn, update_time)

            # Upsert show into index
            self.index.update({'ccn': show_ccn}, {'$set': {'title': title, 'imdb_m': imdb_link}}, upsert=True)
            self.temp_index.append([show_ccn, title, imdb_link])
            self.logger.info('Episodes Updated for ' + show_ccn + ' titled with ' + title)
        else:
            self.logger.info('Episodes for ' + show_ccn + ' is latest.')

    def upsert_one(self, show_info, update=True):
        self.grid.update({'ccn': show_info[0]}, {'$set': {'link': show_info[1], 'channel': show_info[2], 'dow': show_info[3], 'stime': show_info[4], 'etime': show_info[5], 's': show_info[6], 'r': show_info[7]}}, upsert=True)
        self.logger.debug('Grid Upserted - ' + str(show_info))
        if update:
            self.update_one(show_info[0])

    def update_index(self):
        if not self.temp_index:
            # TODO:Fetch indexes to memory(temporary store) from database
            pass
        self.index.update({'ccn': 'totalindexlist'}, {'$set': {'list': self.temp_index}}, upsert=True)

    def update_grid(self):
        self.logger.info('START UPDATING GRID')
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
                        num_of_slots = td['colspan']
                        start_time, end_time = timeslot.next(int(num_of_slots))
                        if self.is_valid_slot(td):
                            # Gather information about the show in current slot.                         
                            link = 'http://www.epguides.com' + td.a['href'][2:]
                            camelcase_name = link[24:-1]
                            status = 'air'
                            self.upsert_one([camelcase_name, link, channel_name, day_of_week, start_time, end_time, status, ''])
                day_of_week_idx += 1

            # Load shows on hiatus
            self.logger.info('Update Hiatus Shows')
            table = table_list[7]
            li_list = table.findAll('li')
            for li in li_list:
                link = 'http://www.epguides.com' + li.a['href'][2:]
                camelcase_name = link[24:-1]
                return_date = li.span.string[9:]
                channel_name = 'UNKNOWN'
                day_of_week = 'UNKNOWN'
                start_time = [25, 61]
                end_time = [25, 61]
                status = 'hiatus'
                self.upsert_one([camelcase_name, link, channel_name, day_of_week, start_time, end_time, status, return_date])
            
            # TODO: Load shows that are cancelled/ended
            self.update_timestamp('grid', update_time)
        else:
            self.logger.info('DATA IS LATEST [' + update_time.isoformat() + ']. Start updating episodes.')
            # If grid data is latest, update each show detail iterating grid.
            for show in self.grid.find():
                self.update_one(show['ccn'])

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
    epgc.update_grid()
    epgc.update_index()

if __name__ == "__main__":
    sys.exit(main())
