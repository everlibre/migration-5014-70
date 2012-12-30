#!/usr/bin/python
# -*- encoding: utf-8 -*-
''' Migration multi base ''' 
import sys, traceback
import openerp_connection
import time             
from datetime import datetime
from optparse import OptionParser
from migration_lib import MigrationLib
import openerp_connectionv7

PARSER = OptionParser()
PARSER.add_option("-a", "--userdb", dest="userdb", default='postgres', help="User Postgres db")
PARSER.add_option("-C", "--createdb", dest="createdb", default='true', help="Create base cible ?")
PARSER.add_option("-b", "--passwordb", dest="passdb", default='password', help="Password User Postgres db")
PARSER.add_option("-d", "--dbs", dest="dbs", default='terp', help="Nom de la base source")
PARSER.add_option("-u", "--users", dest="users", default='terp', help="User Openerp source")
PARSER.add_option("-w", "--passwds", dest="passwds", default='terp', help="mot de passe Openerp  source")
PARSER.add_option("-s", "--serveurs", dest="hosts", default='127.0.0.1', help="Adresse  Serveur source")
PARSER.add_option("-o", "--ports", dest="ports", default='8069', help="port du serveur source")
PARSER.add_option("-p", "--protocoles", dest="protocoles", default='http', help="protocole http/https source")
PARSER.add_option("-D", "--dbc", dest="dbc", default='testmulti', help="Nom de la base cible ")
PARSER.add_option("-U", "--userc", dest="userc", default='terp', help="User Openerp cible")
PARSER.add_option("-W", "--passwdc", dest="passwdc", default='terp', help="mot de passe Openerp cible  ")
PARSER.add_option("-S", "--serveurc", dest="hostc", default='127.0.0.1', help="Adresse  Serveur cible")
PARSER.add_option("-m", "--module", dest="module", default='all', help="module")
PARSER.add_option("-n", "--dbhostc", dest="dbhostc", default='127.0.0.1', help="Adresse  Serveur db cible")
PARSER.add_option("-O", "--portc", dest="portc", default='8090', help="port du serveur cible")
PARSER.add_option("-P", "--protocolec", dest="protocolec", default='http', help="protocole http/https cible")
(OPTIONS, ARGUMENTS) = PARSER.parse_args()
MODULE_LIST = {}
MODULE_IDS = []
NEWTAB = {}

def recree_db():
    ''' Creation de la base cible '''
    print u"Creation base %s " % BASECIBLE    
    connect_db = openerp_connection.openerp_db(OPTIONS.protocolec + '://', OPTIONS.hostc, OPTIONS.portc)
    try:
        connect_db.sock.drop(PASS_ADMIN_OPENERP, BASECIBLE)
        print u"Base drope"
    except BaseException:
        pass
    idnewbase = connect_db.sock.create(PASS_ADMIN_OPENERP, BASECIBLE, False, 'fr_CH', PASS_ADMIN_NEW_BASE)
    temporisation = 0
    print "Creation en cours",
    while temporisation == 0:
        try:
            res = connect_db.sock.get_progress(PASS_ADMIN_OPENERP, idnewbase)
            if len(res[1]) >0:
                break
        except BaseException:
            temporisation = 1
        time.sleep(10)
        print ".",
    print 
    print u"Base %s cree" % BASECIBLE



if OPTIONS.userc == 'terp':
    OPTIONS.userc = "admin"
    OPTIONS.passwdc = "admin"
    
if OPTIONS.dbc == 'terp':
    OPTIONS.dbc = OPTIONS.dbs + "v70"

BASECIBLE = OPTIONS.dbc
BASESOURCE = OPTIONS.dbs
PASS_ADMIN_OPENERP = 'Uniforme'
PASS_ADMIN_NEW_BASE = 'Uniforme'

print 
print "-"*80
print 
    
START = datetime.now()
if OPTIONS.createdb == 'true':
    recree_db()
    print "end creation ", datetime.now()-START
    
CONNECTION_CIBLE = openerp_connectionv7.openerp(OPTIONS.protocolec + '://', \
        OPTIONS.hostc, OPTIONS.portc, BASECIBLE, 'admin', PASS_ADMIN_NEW_BASE)

CONNECTION_CIBLE.object.execute(CONNECTION_CIBLE.dbname, CONNECTION_CIBLE.uid,
        CONNECTION_CIBLE.pwd, 'base.module.update', 'create', {})
CONNECTION_CIBLE.object.execute(CONNECTION_CIBLE.dbname, CONNECTION_CIBLE.uid,\
        CONNECTION_CIBLE.pwd, 'base.module.update', 'update_module', [1])   

DB_MODULE = openerp_connectionv7.module(CONNECTION_CIBLE)

DB_MODULE.install('multi_company')

#BASE_MULTIS = ['fief', 'fiefdev', 'fiefmgt']
BASE_MULTIS = [OPTIONS.dbs]
X = 1
try:    
    for BASESOURCE in BASE_MULTIS:
        print
        print "-"*80
        print 
        print "Migration %s" % BASESOURCE
        print 

        try:
            connectionsource = openerp_connection.openerp(OPTIONS.protocoles +\
'://', OPTIONS.hosts, OPTIONS.ports, BASESOURCE, OPTIONS.users, OPTIONS.passwds)
            migration = MigrationLib(connectionsource, CONNECTION_CIBLE,\
                    X, OPTIONS)
            migration.pass_admin_new_base = 'uniforme'
            migration.load_fields()
            if OPTIONS.createdb == 'true':
                vals = migration.get_values(1, 'res.company',['name'])
                if vals.has_key('account_id'):
                    vals['account_id'] = 1
                vals['name'] = BASESOURCE
                if X == 1:
                    if vals.has_key('partner_id'):
                        vals['partner_id'] = 1
                    CONNECTION_CIBLE.write('res.company', 1, vals)
                    company_id = 1
                elif CONNECTION_CIBLE.search('res.company', [('name',\
                        '=', BASESOURCE)]) == []:
                    vals['partner_id'] = CONNECTION_CIBLE.create('res.partner',\
                            {'name':BASESOURCE})    
                    company_id = CONNECTION_CIBLE.create('res.company',  vals)

        except BaseException, e:
            EXC_TYPE, EXC_VALUE, EXC_TRACEBACK = sys.exc_info()
            print "*** print_exception:"
            traceback.print_exception(EXC_TYPE, EXC_VALUE, EXC_TRACEBACK,\
                    limit=2, file=sys.stdout)
            sys.exit()  
        if OPTIONS.createdb == 'true':
            source_module_ids = connectionsource.search('ir.module.module',\
                    [('state', '=', 'installed')])
            cible_module_ids = CONNECTION_CIBLE.search('ir.module.module', [])
            cible_modules = {}
            for cible_module_id in cible_module_ids: 
                cible_module = CONNECTION_CIBLE.read('ir.module.module',\
                        cible_module_id)
                cible_modules[cible_module['name']] = cible_module_id
            source_modules = {}
            for source_module_id in source_module_ids: 
                source_module = connectionsource.read('ir.module.module',\
                        source_module_id)
                source_modules[source_module['name']] = source_module_id
            for source_module in source_modules:
                if cible_modules.has_key(source_module):
                    cible_module_id = cible_modules[source_module]
                    cible_module = CONNECTION_CIBLE.read('ir.module.module',\
                            cible_module_id)
                    if cible_module['state'] != 'installed' :
                        DB_MODULE.install(module_id=[cible_module_id])
            print "Fin installation modules"
        actserver_ids = CONNECTION_CIBLE.search('ir.actions.server', [], 0, 1000)
        CONNECTION_CIBLE.unlink('ir.actions.server', actserver_ids)
        
        if OPTIONS.module == 'all' or OPTIONS.module == 'base' :
            migration.migre_base_data()
        if OPTIONS.module == 'all' or OPTIONS.module == 'partner' :
            migration.migre_partner()
        if OPTIONS.module == 'all' or OPTIONS.module == 'product' :
            migration.migre_product()
        if OPTIONS.module == 'all' or OPTIONS.module == 'compta' :
            migration.migre_compta()
        
        
        END = datetime.now()
        print "Fin Migration %s en %s" % (BASESOURCE, END-START)
        X = X+1
except Exception, e:
    EXC_TYPE, EXC_VALUE, EXC_TRACEBACK = sys.exc_info()
    print "*** print_exception:"
    print "BASE CIBKE ",BASECIBLE
    erreur_file = open(BASECIBLE+".err",'a')
    erreur_file.write(traceback.format_exc())
    traceback.print_exception(EXC_TYPE, EXC_VALUE, EXC_TRACEBACK, limit=2,\
            file=erreur_file)
    erreur_file.close()
    raise
finally:
    connectionsource.logout()


