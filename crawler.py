# -*- coding: utf-8 -*-
# Minimum required Pyhton 3.4

'''
Rodar: python3 crawler.py lista_de_url.txt
'''

__author__ = "Demian Andrade (demian@demianandrade.com)"
__version__ = "1.0.0"
__copyright__ = "Copyright (c) 2021 Demian Andrade"
__all__ = ['Email Crawler']


import sys
import sqlite3 
from datetime import datetime
from collections import deque
from bs4 import BeautifulSoup
import requests
import requests.exceptions
from urllib.parse import urlsplit
import re
from email_validator import validate_email, EmailNotValidError, caching_resolver

# Python version check
MIN_PYTHON = (3, 4, 0)
if sys.version_info < MIN_PYTHON:
    sys.exit("\n*** Python %s.%s.%s ou mais novo necessário. Atualize seu Python.\n" % MIN_PYTHON)


# Database related functions -> SQLite
class Database:
    def __init__(self):
        try:
            self.con = sqlite3.connect('database.sqlite')
            self.cur = self.con.cursor()
        except Exception as err:
            print('*** ERRO AO ABRIR OU CRIAR BANCO: \nErro: %s' % str(err))
            sys.exit("*** Abortado.")

        try:
            self.cur.execute('''CREATE TABLE IF NOT EXISTS emails (created text, email text, tested int, valid int, obs text)''')
            self.cur.execute('''CREATE TABLE IF NOT EXISTS domains (created text, domain text)''')
            self.cur.execute('''CREATE TABLE IF NOT EXISTS exclusions (created text, domain text)''')
        except Exception as err:
            print('*** ERRO AO CRIAR TABELAS NO BANCO: \nErro: %s' % str(err))
            sys.exit("*** Abortado.")
        finally:
            self.con.commit()

    def __enter__(self):
        return self

    def __exit__(self):
        self.con.close()

    # insert emails to emails table
    def insert_email_single(self, email, tested, valid, obs):
        try:
             self.cur.execute("INSERT INTO emails VALUES ('%s', '%s', %d, %d, '%s')" % (datetime.today(), email, tested, valid, obs))
        except Exception as err:
            print('*** ERRO AO INSERIR UM EMAIL: \nErro: %s' % str(err))
        finally:
            self.con.commit()

    # insert single domain to domains to be searched table
    def insert_domain_single(self, domain):
        try:
            self.cur.execute("INSERT INTO domains VALUES ('%s', '%s')" % (datetime.today(), domain))
        except Exception as err:
            print('*** ERRO AO INSERIR UM DOMÍNIO: \nErro: %s' % str(err))
        finally:
            self.con.commit()

    # bulk insert domains to domains to be searched table from txt file, one per line, single column
    def insert_domains_from_file(self, file):
        try:
            domains = open(file, 'r').readlines()
            for domain in domains:
                self.cur.execute("INSERT INTO domains VALUES ('%s', '%s')" % (datetime.today(), domain.rstrip('\n')))
        except Exception as err:
            print('*** ERRO AO INSERIR DOMÍNIOS: \nErro: %s' % str(err))
        finally:
            self.con.commit()

    # insert single domain to excluded domains table
    def insert_exclusion_single(self, domain):
        try:
            self.cur.execute("INSERT INTO exclusions VALUES ('%s', '%s')" % (datetime.today(), domain))
        except Exception as err:
            print('*** ERRO AO INSERIR UM DADO: \nErro: %s' % str(err))
        finally:
            self.con.commit()

    # bulk insert domains to excluded domains table from txt file, one per line, single column
    def insert_exclusions_from_file(self, file):
        try:
            exclusions = open(file, 'r').readlines()
            for exclusion in exclusions:
                self.cur.execute("INSERT INTO exclusions VALUES ('%s', '%s')" % (datetime.today(), exclusion.rstrip('\n')))
        except Exception as err:
            print('*** ERRO AO INSERIR EXCLUSÕES: \nErro: %s' % str(err))
        finally:
            self.con.commit()

    # fetch data from tables
    def fetch_data_from_table(self, table):
        try:
            data = self.cur.execute('SELECT * FROM %s' % table)
        except Exception as err:
            print('*** ERRO AO LER DADOS DO BANCO: \nErro: %s' % str(err))
        finally:
            return data

    # update email after test
    def update_email(self, email, new_email, new_tested, new_valid, new_obs):
        try:
            self.cur.execute('UPDATE emails SET email = ? WHERE email = ?', (new_email, email))
            self.cur.execute('UPDATE emails SET tested = ? WHERE email = ?', (new_tested, email))
            self.cur.execute('UPDATE emails SET valid = ? WHERE email = ?', (new_valid, email))
            self.cur.execute('UPDATE emails SET obs = ? WHERE email = ?', (new_obs, email))
        except Exception as err:
            print('*** ERRO AO atualizer DADOS DO BANCO: \nErro: %s' % str(err))
        finally:
            self.con.commit()

    # clear all rows from table
    def clear_exclusion(self, table):
        try:
            self.cur.execute("DELETE FROM %s" % table)
        except Exception as err:
            print('*** ERRO AO ELIMINAR LIHAS DE %s: \nErro: %s' % (table, str(err)))
        finally:
            self.con.commit()

    # remove duplicates from tables. 
    # Allowed tables: emails, domains, exclusions
    def remove_duplicates(self, table):

        #table column headers (table: header)
        headers = {
            'emails': 'email',
            'domains': 'domain',
            'exclusions': 'domain'
        }
        try:
            count_exclusions = self.cur.execute("SELECT COUNT() FROM %s" % table)
            #print("*** Quantidade atual na base de %s: %s" % (table, count_exclusions.fetchone()[0]))
            self.cur.execute("DELETE FROM %s WHERE rowid NOT IN (SELECT min(rowid) FROM %s GROUP BY %s)" % (table, table, headers[table]))
        except Exception as err:
            print('*** ERRO AO ELIMINAR DUPLICADOS: \nErro: %s' % str(err))
        finally:
            self.con.commit()
            count_exclusions = self.cur.execute("SELECT COUNT() FROM %s" % table)
            #print("*** Quantidade após limpeza de %s: %s" % (table, count_exclusions.fetchone()[0]))



if __name__ == '__main__':
    print("\n*** Bora começar essa porra!\n")
    
    '''
    Crawling phase
    Set list of emails in domains_to_crawl variable
    Emails must be one per line
    '''


    # list of domains to crawl this time
    domains_to_crawl = sys.argv[1] #"lista1.txt"

    # initialize db, update and sanitize tables
    db = Database()
    
    db.insert_exclusions_from_file("exclusions.txt")
    # db.insert_domains_from_file("domains.txt")
    db.remove_duplicates('exclusions')
    db.remove_duplicates('emails')
    db.remove_duplicates('domains')

    # a set of urls that we have already crawled
    processed_urls = set()

    # a set of crawled emails
    emails = set()

    # a set of skippable urls
    skip_domains = set()

    # a set of new urls
    new_urls = deque([])


    # add data to sets
    # move list of already processed urls in db to processed_urls
    try:
        for url in db.fetch_data_from_table('domains'):
            processed_urls.add(url[1])
    except Exception as err:
        print('*** ERRO AO LER DOMÍNIOS DO BANCO: \nErro: %s' % str(err))

    # move list of excluded urls in db to skip_domains
    try:
        for url in db.fetch_data_from_table('exclusions'):
            skip_domains.add(url[1])
    except Exception as err:
        print('*** ERRO AO LER DOMÍNIOS EXCLUÍDOS DO BANCO: \nErro: %s' % str(err))

    # add domains from file to new_domains
    try:
        urls = open(domains_to_crawl, 'r').readlines()
        for url in urls:
            new_urls.append(url.strip('\n'))
    except Exception as err:
        print('*** ERRO AO INSERIR URLs: \nErro: %s' % str(err))
    

    # process urls one by one until we exhaust the queue
    while len(new_urls):
        # move next url from the queue to the set of processed urls
        url = new_urls.popleft()
        processed_urls.add(url)

        # extract base url to resolve relative links
        parts = urlsplit(url)
        base_url = "{0.scheme}://{0.netloc}".format(parts)
        path = url[:url.rfind('/')+1] if '/' in parts.path else url

        # already visited
        if base_url in processed_urls: 
            print("X: Domínio já visitado: %s" % base_url)
            continue

        # skippable domain
        if base_url in skip_domains:
            print("X: Domínio excluído: %s" % base_url)
            continue

        # add domain to visited domains list
        db.insert_domain_single(base_url)
        db.remove_duplicates('domains')

        # get url's content
        try:
            response = requests.get(url)
            print("P: Processando %s" % url)
        except (requests.exceptions.MissingSchema, requests.exceptions.ConnectionError):
            # ignore pages with errors
            print("X: Ignorado %s - não foi possível acessar url." % url)
            continue

        # extract all email addresses and add them into the resulting set
        # Also, first part of regex removes picture files with @ like logoWhiteoutLockup@2x.png
        new_emails = set(re.findall(r"(?!\S*\.(?:jpg|png|gif|bmp)(?:[\s\n\r]|$))[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z]+", response.text, re.I))
        emails.update(new_emails)

        # save these emails to db and test again for images files that ressamble emails
        for email in new_emails:
            if not email[-3:] in ['jpg', 'png', 'gif', 'bmp']:
                print("@: *** Encontrado %s" % email)
                db.insert_email_single(str(email), 0, 0, "")
            else:
                print("@: *** Ignorado %s" % email)
        
        # create a beutiful soup for the html document
        soup = BeautifulSoup(response.text, features="html.parser")

        # find and process all the anchors in the document
        link = ""
        for anchor in soup.find_all("a"):
            # extract link url from the anchor
            link = anchor.attrs["href"] if "href" in anchor.attrs else ''
            
            # bad links
            if not link.find("#") == -1: continue
            if not link.find('javascript') == -1: continue
            if not link.find('whatsapp://') == -1: continue

            # resolve relative links
            if link.startswith('/'):
                link = base_url + link
            elif not link.startswith('http'):
                link = path + link

            # add the new url to the queue if it was not enqueued nor processed yet
            if not link in new_urls and not link in processed_urls:
                new_urls.append(link)
    
    '''
    Email validation phase
    Check DNS/MX for each email
    Not checking SMTP deliverability
    '''

    # Purge duplicates
    db.remove_duplicates('emails')
        
    # set of emails to validate
    emails_to_validate = deque([])

    # DNS resolver
    resolver = caching_resolver(timeout=10)

    # load emails
    try:
        for email in db.fetch_data_from_table('emails'):
            # email[1] = email, email[2] = tested, email[3] = valid, email[4] = obs
            emails_to_validate.append([email[1], email[2], email[3], email[4]])
    except Exception as err:
        print('*** ERRO AO LER DOMÍNIOS DO BANCO: \nErro: %s' % str(err))

    for email in emails_to_validate:
        # email[0] = email, email[1] = tested, email[2] = valid, email[3] = obs
        if email[1] == 0:
            try:
                # Validate.
                valid = validate_email(email[0], check_deliverability=True, dns_resolver=resolver)

                # Update db
                db.update_email(email[0], valid.email, 1, 1, "")
                print("OK: %s" % email)

            except EmailNotValidError as e:
                db.update_email(email[0], email[0], 1, 0, str(e))
                print("INVÁLIDO: %s" % email)
