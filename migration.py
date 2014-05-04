#!/usr/bin/python
# -*- encoding: utf-8 -*-
""" Migration multi base """
import sys
import traceback
import openerp_connection
import time
import os
from datetime import datetime
from optparse import OptionParser
from migration_lib import MigrationLib
import openerp_connectionv7
import sys
import codecs
from smtplib import SMTP
from email.MIMEText import MIMEText

def recree_db(OPTIONS):
    """ Creation de la base cible """
    print u"Création base %s " % OPTIONS.dbc
    connect_db = openerp_connection.openerp_db(OPTIONS.protocolec + '://', OPTIONS.hostc, OPTIONS.portc)
    try:
        connect_db.sock.drop(OPTIONS.passwdadmin, OPTIONS.dbc)
        #print u"Base drope"
    except BaseException:
        pass
    #print "PASS_ADMIN_NEW_BASE ",PASS_ADMIN_NEW_BASE
    idnewbase = connect_db.sock.create(OPTIONS.passwdadmin, OPTIONS.dbc, False, 'fr_CH', OPTIONS.passwdadmin)
    temporisation = 0
    #print "Creation en cours",
    while temporisation == 0:
        try:
            res = connect_db.sock.get_progress(OPTIONS.passwdadmin, idnewbase)
            if len(res[1]) > 0:
                break
        except BaseException:
            temporisation = 1
        time.sleep(10)
        print ".",

def migration(OPTIONS):
    BASECIBLE = OPTIONS.dbc
    BASESOURCE = OPTIONS.dbs



    if OPTIONS.userc == 'terp':
        OPTIONS.userc = "admin"
        OPTIONS.passwdc = "admin"

    if OPTIONS.dbc == 'terp':
        OPTIONS.dbc = OPTIONS.dbs
    print
    print "-" * 80
    print
    X = 1
    START = datetime.now()
    if OPTIONS.createdb:
        try:
            os.remove("/media/mint/migration-%s.sqlite" % BASESOURCE)
        except:
            pass
        fname = "/tmp/%s_model.sql" % BASECIBLE
        if os.path.exists(fname):

            os.system('dropdb %s ' % BASECIBLE)

            os.system("createdb --encoding='unicode' %s " % BASECIBLE)
            load_cmd = 'psql -f %s %s >/tmp/load.log' % (fname, BASECIBLE)
            retval = os.system(load_cmd)
            if retval != 0:
                sys.exit(1)

            CONNECTION_CIBLE = openerp_connectionv7.openerp(OPTIONS.protocolec + '://',
                                                            OPTIONS.hostc, OPTIONS.portc, BASECIBLE, 'admin',
                                                            OPTIONS.passwdadmin)
            connectionsource = openerp_connection.openerp(OPTIONS.protocoles +
                                                          '://', OPTIONS.hosts, OPTIONS.ports,
                                                          OPTIONS.dbs, OPTIONS.users,
                                                          OPTIONS.passwds)
            migration = MigrationLib(connectionsource, CONNECTION_CIBLE, X, OPTIONS)
            migration.connect_pg()
            migration.passwordsource = OPTIONS.passwds
            migration.controle_data()
            migration.clean_source_db()
        else:
            recree_db(OPTIONS)
            CONNECTION_CIBLE = openerp_connectionv7.openerp(OPTIONS.protocolec + '://',
                                                            OPTIONS.hostc, OPTIONS.portc, BASECIBLE, 'admin',
                                                            OPTIONS.passwdadmin)
            connectionsource = openerp_connection.openerp(OPTIONS.protocoles +
                                                          '://', OPTIONS.hosts, OPTIONS.ports, OPTIONS.dbs,
                                                          OPTIONS.users,
                                                          OPTIONS.passwds)

            migration = MigrationLib(connectionsource, CONNECTION_CIBLE,
                                     X, OPTIONS)
            migration.passwordsource = OPTIONS.passwds
            migration.connect_pg()
            migration.controle_data()
            migration.clean_source_db()
            print u"base créé ", datetime.now() - START
            CONNECTION_CIBLE.object.execute(CONNECTION_CIBLE.dbname, CONNECTION_CIBLE.uid,
                                            CONNECTION_CIBLE.pwd, 'base.module.update', 'create', {})
            CONNECTION_CIBLE.object.execute(CONNECTION_CIBLE.dbname, CONNECTION_CIBLE.uid,
                                            CONNECTION_CIBLE.pwd, 'base.module.update', 'update_module', [1])
            DB_MODULE = openerp_connectionv7.module(CONNECTION_CIBLE)
            DB_MODULE.install('multi_company')
            DB_MODULE.install('auth_crypt')
            DB_MODULE.install('uni_otp')
            DB_MODULE.install('uni_account')
            DB_MODULE.install('base_vat')
            DB_MODULE.install('uni_features')
            source_module_ids = connectionsource.search('ir.module.module',
                                                        [('state', '=', 'installed')])
            cible_module_ids = CONNECTION_CIBLE.search('ir.module.module', [])
            cible_modules = {}
            for cible_module_id in cible_module_ids:
                cible_module = CONNECTION_CIBLE.read('ir.module.module',
                                                     cible_module_id)
                cible_modules[cible_module['name']] = cible_module_id
            source_modules = {}
            for source_module_id in source_module_ids:
                source_module = connectionsource.read('ir.module.module',
                                                      source_module_id)
                if "hr_timesheet_invoice" in source_module['name']:
                    migration.hr = True
                    print "HR OK"
                if source_module['name'] == 'school':
                    source_module['name'] = 'uni_school'
                source_modules[source_module['name']] = source_module_id
            for source_module in source_modules:
                if source_module in cible_modules:
                    cible_module_id = cible_modules[source_module]
                    cible_module = CONNECTION_CIBLE.read('ir.module.module',
                                                         cible_module_id)
                    if cible_module['state'] != 'installed':
                        DB_MODULE.install(module_id=[cible_module_id])
            print "Fin installation modules"
            actserver_ids = CONNECTION_CIBLE.search('ir.actions.server', [], 0, 1000)
            CONNECTION_CIBLE.unlink('ir.actions.server', actserver_ids)
    else:
        CONNECTION_CIBLE = openerp_connectionv7.openerp(OPTIONS.protocolec + '://',
                                                        OPTIONS.hostc, OPTIONS.portc, BASECIBLE, 'admin',
                                                        OPTIONS.passwdadmin)
        connectionsource = openerp_connection.openerp(OPTIONS.protocoles +
                                                      '://', OPTIONS.hosts, OPTIONS.ports, OPTIONS.dbs, OPTIONS.users,
                                                      OPTIONS.passwds)
        migration = MigrationLib(connectionsource, CONNECTION_CIBLE, X, OPTIONS)
        migration.connect_pg()

        migration.controle_data()
        migration.clean_source_db()
    BASESOURCE = OPTIONS.dbs

    try:
        print
        print "-" * 80
        print
        print "Migration %s" % BASESOURCE
        print
        try:
            migration.pass_admin_new_base = 'uniforme'
            migration.load_fields()
            #if OPTIONS.createdb == 'true':
            vals = migration.get_values(1, 'res.company', ['name'])
            if 'account_id' in vals:
                vals['account_id'] = 1
            vals['name'] = BASESOURCE
            if X == 1:
                if 'partner_id' in vals:
                    vals['partner_id'] = 1
                CONNECTION_CIBLE.write('res.company', 1, vals)
                company_id = 1
            elif not CONNECTION_CIBLE.search('res.company', [('name', '=', BASESOURCE)]):
                vals['partner_id'] = CONNECTION_CIBLE.create('res.partner', {'name': BASESOURCE})
                company_id = CONNECTION_CIBLE.create('res.company', vals)

        except BaseException, e:
            EXC_TYPE, EXC_VALUE, EXC_TRACEBACK = sys.exc_info()
            print "*** print_exception:"
            traceback.print_exception(EXC_TYPE, EXC_VALUE, EXC_TRACEBACK,
                                      limit=2, file=sys.stdout)
            sys.exit()

        if OPTIONS.module == 'all' or OPTIONS.module == 'base':
            migration.migre_base_data()
        if OPTIONS.module == 'all' or OPTIONS.module == 'partner':
            migration.migre_partner()
        if OPTIONS.module == 'all' or OPTIONS.module == 'product':
            migration.migre_product()
        if OPTIONS.module == 'all' or OPTIONS.module == 'compta':
            migration.init()
            migration.migre_compta()
        migration.controle_tables()
        migration.deconnect_pg()
        END = datetime.now()
        print "Fin Migration %s en %s" % (BASESOURCE, END - START)
        mailmsg = "Fin Migration %s en %s" % (BASESOURCE, END - START)
        mail_envoye = MIMEText(str(mailmsg))
        mail_envoye['From'] = "eric@vernichon.fr"
        mail_envoye['Subject'] = "Migration %s " % BASESOURCE
        mail_envoye['To'] = 'eric@vernichon.fr'
        #envoi = SMTP('192.168.12.15')
        envoi = SMTP('smtp.free.fr')
        envoi.sendmail(mail_envoye['From'], ['eric@vernichon.fr'], mail_envoye.as_string())
        X += 1
    except Exception, e:
        EXC_TYPE, EXC_VALUE, EXC_TRACEBACK = sys.exc_info()
        print "*** print_exception:"
        print "BASE CIBKE ", BASECIBLE
        erreur_file = open(BASECIBLE + ".err", 'a')
        erreur_file.write(traceback.format_exc())
        traceback.print_exception(EXC_TYPE, EXC_VALUE, EXC_TRACEBACK, limit=2,
                                  file=erreur_file)
        erreur_file.close()
        raise


def main():
    sys.stdout = codecs.getwriter('utf8')(sys.stdout)

    PARSER = OptionParser()

    PARSER.add_option("-C", "--createdb", dest="createdb", default=True, help="Create base cible ?")
    PARSER.add_option("-d", "--dbs", dest="dbs", default='terp', help="Nom de la base source")
    PARSER.add_option("-u", "--users", dest="users", default='terp', help="User Openerp source")
    PARSER.add_option("-w", "--passwds", dest="passwds", default='terp', help="mot de passe Openerp  source")
    PARSER.add_option("-s", "--serveurs", dest="hosts", default='127.0.0.1', help="Adresse  Serveur source")

    PARSER.add_option("-p", "--protocoles", dest="protocoles", default='http', help="protocole http/https source")
    PARSER.add_option("-D", "--dbc", dest="dbc", default='testmulti', help="Nom de la base cible ")
    PARSER.add_option("-U", "--userc", dest="userc", default='terp', help="User Openerp cible")
    PARSER.add_option("-W", "--passwdc", dest="passwdc", default='terp', help="mot de passe Openerp cible  ")
    PARSER.add_option("-S", "--serveurc", dest="hostc", default='127.0.0.1', help="Adresse  Serveur cible")
    PARSER.add_option("-m", "--module", dest="module", default='all', help="module")
    PARSER.add_option("-l", "--lines", dest="lines", default=True, help="With move line")

    PARSER.add_option("-a", "--userdbc", dest="userdbc", default='postgres', help="User Postgres db Cible")
    PARSER.add_option("-b", "--passwordbc", dest="passdbc", default='password', help="Password User Postgres db Cible")

    PARSER.add_option("-f", "--userdbs", dest="userdbs", default='postgres', help="User Postgres db Source")
    PARSER.add_option("-F", "--passwordbs", dest="passdbs", default='password',
                      help="Password User Postgres db Source")

    PARSER.add_option("-g", "--dbportc", dest="dbportc", default='5433', help="Port db cible")
    PARSER.add_option("-G", "--dbports", dest="dbports", default='5434', help="Port db source")

    PARSER.add_option("-n", "--dbhostc", dest="dbhostc", default='127.0.0.1', help="Adresse  Serveur db cible")
    PARSER.add_option("-N", "--dbhosts", dest="dbhosts", default='127.0.0.1', help="Adresse  Serveur db source")

    PARSER.add_option("-o", "--ports", dest="ports", default='8069', help="port du serveur OpenErp source")
    PARSER.add_option("-O", "--portc", dest="portc", default='8090', help="port du serveur OpenErp cible")

    PARSER.add_option("-P", "--protocolec", dest="protocolec", default='http', help="protocole http/https cible")
    PARSER.add_option("-A", "--passwdadmin", dest="passwdadmin", default='admin', help="mot de passe Admin")

    (OPTIONS, ARGUMENTS) = PARSER.parse_args()
    MODULE_LIST = {}
    MODULE_IDS = []
    NEWTAB = {}

    migration(OPTIONS)


if __name__ == '__main__':
    main()
