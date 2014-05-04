#!/usr/bin/python
# -*- encoding: utf-8 -*-
""" Module de migration openerp """

import os
import psycopg2
import sys
import sqlite3
import xmlrpclib
import uuid
from decimal import Decimal
from datetime import datetime

FIELDS_TAB = {}
FIELDS_TAB_SOURCE = {}
LIMITE = 100000000


class Memoize:
    def __init__(self, fonction):
        self.fonction = fonction
        self.memoized = {}

    def __call__(self, *args):
        try:
            return self.memoized[args]
        except KeyError:
            self.memoized[args] = self.fonction(*args)
            return self.memoized[args]


def utf(vals):
    """ converti une valeur en unicode"""
    if isinstance(vals, bool):
        str_utf8 = ""
    elif isinstance(vals, str):
        str_utf8 = vals
    elif isinstance(vals, unicode):
        str_utf8 = vals.encode('utf-8')
    else:
        str_utf8 = str(vals)
    res = str_utf8.replace(";", ", ").replace('\n', '')
    return res


class MigrationLib(object):
    """ Librairie de migration openerp v5.014 -> V7.0 """
    connectionsource = None
    connectioncible = None
    passwordsource = ''
    company_id = None
    newtab = {}
    conn_sqlite = None
    hr = False
    cr_sqlite = None
    options = None
    recursion_level = 0
    pass_admin_new_base = None
    current_account_period_ids = []
    account_processed = {}
    company_currency = None
    company_currency_source = None
    conn = None
    curseur = None
    conn_source = None
    reconciliation = None
    curseur_source = None
    cibleconnectstr = None
    sourceconnectstr = None
    tables_processed = []
    global_start = 0
    move_already_process = []

    def __init__(self, connectionsource, connectioncible, company_id, options):
        if not connectionsource or not connectioncible:
            raise
        self.connectionsource = connectionsource
        self.connectioncible = connectioncible
        self.company_id = company_id
        self.options = options
        file_name = str(uuid.uuid4())
        if os.path.exists("/tmp/migration-%s.sqlite" % connectionsource.dbname):
            os.remove("/tmp/migration-%s.sqlite" % connectionsource.dbname)
        if not os.path.exists("/tmp/migration-%s.sqlite" % connectionsource.dbname):
            try:
                self.conn_sqlite = sqlite3.connect("/tmp/migration-%s.sqlite" % connectionsource.dbname)
                self.cr_sqlite = self.conn_sqlite.cursor()
                self.cr_sqlite.execute("delete from table_old_id where base = '%s'" % connectionsource.dbname)
                self.conn_sqlite.commit()
            except sqlite3.DatabaseError:  # La base n'existe pas
                self.cr_sqlite.execute(
                    "CREATE TABLE table_old_id(base TEXT,objet TEXT,old_id INTEGER, new_id INTEGER);")
        else:
            self.conn_sqlite = sqlite3.connect("/tmp/migration-%s.sqlite" % connectionsource.dbname)
            self.cr_sqlite = self.conn_sqlite.cursor()

    def __affiche__erreur(self, erreur, sid, vals, function=None):
        """ fonction d'affichage des erreurs """
        fichier = open(self.connectioncible.dbname + ".err", "a")
        try:
            print "id d'origine ", sid
            for key in vals.keys():
                print "Error vals %s : %s \r\n" % (utf(key), utf(vals[key])),
                fichier.write("vals %s : %s \r\n" % (utf(key), utf(vals[key])))
            print
            if hasattr(erreur, 'faultCode'):
                print erreur.faultCode
                fichier.write(erreur.faultCode + "\r\n")
            if hasattr(erreur, 'faultString'):
                print erreur.faultString
                fichier.write(erreur.faultString + "\r\n")
            if hasattr(erreur, 'message'):
                print erreur.message
                fichier.write(erreur.message + "\r\n")
            if function:
                print function
                fichier.write(function + "\r\n" + "\r\n")
        except BaseException, except_erreur:
            if hasattr(except_erreur, 'faultCode'):
                print except_erreur.faultCode
                fichier.write(except_erreur.faultCode + "\r\n")
            if hasattr(except_erreur, 'faultString'):
                print except_erreur.faultString
                fichier.write(except_erreur.faultString + "\r\n")
            if hasattr(except_erreur, 'message'):
                print except_erreur.message
                fichier.write(except_erreur.message + "\r\n")
            print "SID ", sid
            fichier.write("SID " + str(sid) + "\r\n")
            print "Vals ", vals
            fichier.write("Vals " + str(vals) + "\r\n")

        fichier.close()
        sys.exit(1)


    def init(self):
        #self.load_fields()
        res = self.connectionsource.search('account.period', [('date_start', '>=', '2014-01-01')])
        if not res:
            fy_id = self.connectionsource.create('account.fiscalyear',
                                                 {'name': '2014', 'code': '2014', 'date_start': '2014-01-01',
                                                  'date_stop': '2014-12-31'})
            self.connectionsource.execute('account.fiscalyear', 'create_period', [fy_id])
        self.company_currency = \
            self.connectioncible.read('res.company', self.company_id, ['currency_id'])['currency_id'][0]
        self.company_currency_source = \
            self.connectionsource.read('res.company', self.company_id, ['currency_id'])['currency_id'][0]

    def load_fields(self):
        """ charge la definition des champs de tout les modeles """
        START = datetime.now()
        model_ids = self.connectioncible.search('ir.model', [], 0, 500000, 'model asc')
        for model_id in model_ids:
            model = self.connectioncible.read('ir.model', model_id)['model']
            try:
                FIELDS_TAB[model] = self.connectioncible.exec_act(model, 'fields_get')
            except BaseException, erreur:
                print "Load Fields ", erreur.faultString
                sys.exit()
        model_ids = self.connectionsource.search('ir.model', [], 0, 500000, 'model asc')
        for model_id in model_ids:
            model = self.connectionsource.read('ir.model', model_id)['model']
            try:
                FIELDS_TAB_SOURCE[model] = self.connectionsource.exec_act(model, 'fields_get')
            except BaseException, erreur:
                pass
        print "Find load fields", datetime.now() - START

    def __new(self, sid, model):
        """ recherche l'id dans la nouvelle base """
        if model == 'account.account':
            if sid in self.account_processed:
                return self.account_processed[sid]
        if model in self.newtab and sid in self.newtab[model]:
            return self.newtab[model][sid]
        self.cr_sqlite.execute("select new_id from table_old_id where old_id = %s and objet = '%s' and base = '%s'" % (
            sid, model, self.connectionsource.dbname))
        res = None
        try:
            res = self.cr_sqlite.fetchone()
            if res:
                res = res[0]
        except sqlite3.DataError:
            pass
        return res

    def get_values(self, record_id, model, champs=None):
        """ renvoie les valeurs de type model sur la base source """

        res = {}
        if model == 'account.account':
            champs = ['code', 'reconcile', 'user_type', 'currency_id', 'company_id', 'shortcut', 'note', 'parent_id',
                      'type', 'active', 'company_currency_id', 'name', 'currency_mode']
        elif model == 'res.users':
            champs = ['active', 'login', 'name', 'password', 'user_email', 'context_lang', 'groups_id', 'yubi_enable',
                      'yubi_prik', 'yubi_pubk', 'yubi_id']
        elif model == 'account.journal':
            champs = ['view_id', 'default_debit_account_id', 'update_posted', 'code', 'name', 'centralisation',
                      'group_invoice_lines', 'type_control_ids', 'company_id', 'currency', 'sequence_id',
                      'account_control_ids', 'refund_journal', 'invoice_sequence_id', 'active', 'analytic_journal_id',
                      'entry_posted', 'type', 'default_credit_account_id']
        elif model == 'account.journal.column':
            champs = ['name', 'sequence', 'required', 'field', 'readonly']
        elif model == "product.pricelist":
            champs = ['active', 'currency_id', 'type', 'name']
        elif model == "product.pricelist.version":
            champs = ['name', 'date_end', 'date_start', 'active', 'pricelist_id']
        elif model == "account.payment.term":
            champs = ['active', 'note', 'name']
        elif model == "account.move.line":
            champs = ['debit', 'credit', 'statement_id', 'currency_id', 'date_maturity', 'partner_id', 'blocked',
                      'analytic_account_id', 'centralisation', 'journal_id', 'tax_code_id', 'state', 'amount_taxed',
                      'ref', 'origin_link', 'account_id', 'period_id', 'amount_currency', 'date', 'move_id', 'name',
                      'tax_amount', 'product_id', 'account_tax_id', 'product_uom_id', 'followup_line_id', 'quantity']
        elif model == "account.analytic.account":
            champs = ['code', 'quantity_max', 'contact_id',
                      'company_currency_id', 'date', 'crossovered_budget_line',
                      'amount_max', 'partner_id', 'to_invoice', 'date_start',
                      'company_id', 'parent_id', 'state', 'complete_name', 'debit',
                      'pricelist_id', 'type', 'description', 'amount_invoiced',
                      'active', 'name', 'credit', 'balance', 'quantity']
        elif model == 'account.bank.statement':
            champs = ['name', 'currency', 'balance_end', 'balance_start', 'journal_id', 'import_bvr_id', 'state',
                      'period_id', 'date', 'balance_end_real']
        elif model == 'stock.location':
            champs = ['comment', 'address_id', 'stock_virtual_value', 'allocation_method', 'location_id',
                      'chained_location_id', 'complete_name', 'usage', 'stock_real_value', 'chained_location_type',
                      'account_id', 'chained_delay', 'stock_virtual', 'posz', 'posx', 'posy', 'active', 'icon',
                      'parent_right', 'name', 'chained_auto_packing', 'parent_left', 'stock_real']
        elif model == 'product.category':
            champs = ['name', 'sequence', 'type']
            #elif model == 'res.partner.address':

        if model in FIELDS_TAB:
            fields = FIELDS_TAB[model]
        else:
            if model in FIELDS_TAB_SOURCE:
                fields = FIELDS_TAB_SOURCE[model]
            else:
                fields = {}

        self.recursion_level += 1
        if not champs:
            champs = fields.keys()
        if self.recursion_level > 6:
            print ('\t' * self.recursion_level), "recursion level sup 6"
            print ('\t' * self.recursion_level), "Model ", model
            print ('\t' * self.recursion_level), "Champs ", champs
            print ('\t' * self.recursion_level), "record id", record_id
            print
            sys.exit(1)
        try:
            values = self.connectionsource.read(model, record_id, champs)
        except xmlrpclib.Fault as err:
            print "A fault occurred"
            print "Fault code: %d" % err.faultCode
            print "Fault string: %s" % err.faultString
            print "Model ", model
            print "record_id ", record_id
            print "Champs ", champs
            print "Erreur get values "

            sys.exit()

        for field in champs:
            if field in fields and field in values:
                if 'function' in fields[field] and fields[field]['function'] != '_fnct_read':
                    continue
                field_type = fields[field]['type']
                if field_type in ('text', 'char', 'selection'):
                    if values[field]:
                        if unicode(values[field]).strip():
                            try:
                                res[field] = str(values[field])
                            except BaseException:
                                res[field] = unicode(values[field])
                elif field_type in ('boolean', 'date', 'datetime', 'reference', 'float', 'integer'):
                    res[field] = values[field]

                elif field_type == 'many2one':
                    if values[field]:
                        try:
                            res[field] = self.__new(values[field][0], fields[field]['relation'])
                        except RuntimeError, e:
                            print dir(e)
                            print e.message
                            print "RuntimeError ", fields[field]['relation'], model
                            sys.exit()
                elif field_type == 'one2many':
                    continue

                elif field_type == 'many2many':
                    if values[field]:
                        new_val = []
                        for val in values[field]:
                            new_value = self.__new(val, fields[field]['relation'])
                            if new_value not in new_val:  # suppress duplicate value
                                new_val.append(new_value)
                        res[field] = [(6, 0, new_val)]

                elif field_type == 'binary':
                    if values[field]:
                        res[field] = values[field]
                else:
                    print "Erreur __get_values  ", model, field, fields[field]['type'], fields[field]
                    sys.exit()

            self.recursion_level -= 1
        return res

    def __bank_get(self, name):
        """ renvoi l'id de la banque en fonction du nom """
        if name and name.strip():
            bank_id = self.connectioncible.search('res.bank', [('name', 'ilike', name.strip())],
                                                  context={'lang': 'fr_FR'})
            if not bank_id:
                bank_id = None
            else:
                bank_id = bank_id[0]
        else:
            bank_id = None
        return bank_id

    def add_old_id(self, old_id, new_id, model):
        """ ajoute le nouvel id dans la base de correspondance newid <-> oldid """
        self.cr_sqlite.execute("insert into table_old_id(base ,objet ,old_id , new_id) values ('%s','%s',%s,%s)" % (
            self.connectionsource.dbname, model, old_id, new_id))
        self.conn_sqlite.commit()

    def __migre_ir_sequence_type(self):

        if self.company_id != 1:
            return
        self.tables_processed.append('ir.sequence.type')
        ir_sequence_ids = self.connectionsource.search('ir.sequence.type', [], 0, 2000)
        for old_sequence_id in ir_sequence_ids:
            sequence = self.connectionsource.read('ir.sequence.type', old_sequence_id)
            if sequence:
                sequence_id = self.connectioncible.search("ir.sequence.type", [('code', '=', sequence['code'])])
                if sequence_id:
                    sequence_id = sequence_id[0]
                else:
                    sequence.pop('id')
                    sequence_id = self.connectioncible.create('ir.sequence.type', sequence)
                self.add_old_id(old_sequence_id, sequence_id, 'ir.sequence.type')

    def __migre_ir_sequence(self):

        if self.company_id != 1:
            return
        self.tables_processed.append('ir.sequence')
        ir_sequence_ids = self.connectionsource.search('ir.sequence', [], 0, 2000)
        for old_sequence_id in ir_sequence_ids:
            sequence = self.get_values(old_sequence_id, 'ir.sequence')
            if sequence:
                sequence_id = self.connectioncible.search("ir.sequence", [('name', '=', sequence['name'])])
                if sequence_id:
                    sequence_id = sequence_id[0]
                else:
                    sequence_id = self.connectioncible.create('ir.sequence', sequence)

                self.add_old_id(old_sequence_id, sequence_id, 'ir.sequence')

    def __migre_res_country(self):
        """ migre les pays """
        if self.company_id != 1:
            return
        self.tables_processed.append('res.country')

        country_ids = self.connectionsource.search('res.country', [], 0, 2000)

        len_source = len(country_ids)

        for old_country_id in country_ids:
            vals = self.get_values(old_country_id, 'res.country', ['code', 'name'])
            if vals['code'] == 'UK':
                vals['code'] = 'GB'
            self.curseur.execute("select id from res_country where code = '%s' or name = '%s' " % (
                vals['code'], vals['name'].replace("'", "''")))
            country_id = self.curseur.fetchone()
            if not country_id:
                country_id = self.connectioncible.create('res.country', vals)
            else:
                country_id = country_id[0]
            self.add_old_id(old_country_id, country_id, 'res.country')

    def __migre_res_country_state(self):
        """ migration etat"""
        if self.company_id != 1:
            return
        self.tables_processed.append('res.country.state')
        country_state_ids = self.connectionsource.search('res.country.state', [], 0, 2000)
        for old_country_state_id in country_state_ids:

            vals = self.get_values(old_country_state_id, 'res.country.state')
            state = self.connectioncible.search('res.country.state', [('name', '=', vals['name'])], 0, 2)
            country_state_id = None
            if not state:
                try:
                    country_state_id = self.connectioncible.create('res.country.state', vals)
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, 0, vals, '__migre_res_country_state')

            else:
                try:

                    self.connectioncible.write('res.country.state', state[0], vals)
                    country_state_id = state[0]

                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, state, vals, '__migre_res_country_state write')
            self.add_old_id(old_country_state_id, country_state_id, 'res.country.state')

    def __migre_res_users(self):
        """ migration utilisateurs """
        vals = {}
        self.tables_processed.append('res.users')
        group_ids = self.connectioncible.search('res.groups',
                                                [('name', '!=', 'Survey / User'), ('name', '!=', 'Portal'),
                                                 ('name', '!=', 'Anonymous')])
        users_ids = self.connectionsource.search('res.users',
                                                 [('login', 'not like', '%admin%'), ('login', '!=', 'root'),
                                                  ('active', 'in', ['true', 'false'])], 0, 2000)
        try:
            vals = {'groups_id': [(6, 0, group_ids)]}  # self.get_values(1, 'res.users')
            #vals['password'] = self.passwordsource
            self.connectioncible.write('res.users', [1], vals)

            self.add_old_id(1, 1, 'res.users')

        except BaseException, erreur_base:
            self.__affiche__erreur(erreur_base, 0, vals, 'write migre_res_users')
        # group_ids.pop(self.connectioncible.search('res.groups', [('name', '=', 'Technical Features')])[0])
        # group_ids.pop(self.connectioncible.search('res.groups', [('name', '=', 'Settings')])[0])
        # group_ids.pop(self.connectioncible.search('res.groups', [('name', '=', 'Access Rights')])[0])
        group_ids = self.connectioncible.search('res.groups',
                                                [('name', '=', 'Employee')])
        for old_user_id in users_ids:
            vals = self.get_values(old_user_id, 'res.users',
                                   ['active', 'login', 'name', 'password', 'user_email', 'context_lang', 'groups_id'])
            vals['yubi_enable'] = False
            vals['menu_id'] = 1
            user = self.connectioncible.search('res.users',
                                               [('active', 'in', ['true', 'false']), ('login', '=', vals['login'])], 0,
                                               1)
            vals['password'] = "Uniforme10$"
            vals['company_ids'] = [self.company_id]
            vals['company_id'] = self.company_id
            vals['company_ids'] = [(6, 0, [self.company_id])]
            vals['groups_id'] = [(6, 0, group_ids)]
            if 'user_email' in vals:
                vals['email'] = vals['user_email']
                vals.pop('user_email')
            if 'context_lang' in vals and vals['context_lang'] == 'fr_FR':
                vals['context_lang'] = 'fr_CH'
            new_user_id = None
            if not user:
                try:
                    new_user_id = self.connectioncible.create('res.users', vals)
                    if self.hr:
                        new_employee_id = self.connectioncible.create('hr.employee',
                                                                      {'name': vals['name'], 'user_id': new_user_id})
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, 0, vals, 'create __migre_res_users')

            else:
                try:
                    user_val = self.connectioncible.read('res.users', user[0], ['company_ids'])
                    company_ids = user_val['company_ids']
                    company_ids.append(self.company_id)
                    vals['company_ids'] = company_ids
                    self.connectioncible.write('res.users', user, vals)
                    new_user_id = user[0]
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, 0, vals, 'write __migre_res_users')
            self.add_old_id(old_user_id, new_user_id, 'res.users')

    def __migre_res_currency_rate(self):
        """ Migration taux de devises """
        self.tables_processed.append('res.currency.rate')
        currency_rate_id = None
        currency_rate_ids = self.connectionsource.search('res.currency.rate', [], 0, 2000)
        cible_currency_rate_ids = self.connectioncible.search('res.currency.rate', [])
        self.connectioncible.unlink('res.currency.rate', cible_currency_rate_ids)
        for old_currency_rate_id in currency_rate_ids:
            vals = self.get_values(old_currency_rate_id, 'res.currency.rate',
                                   ['name', 'rate', 'currency_id', 'rate_admin', 'rate_coeff'])
            try:
                currency_rate_id = self.connectioncible.create('res.currency.rate', vals)
                self.add_old_id(old_currency_rate_id, currency_rate_id, 'res.currency.rate')
            except BaseException, erreur_base:
                self.__affiche__erreur(erreur_base, currency_rate_id, vals, "create __migre_res_currency")

    def __migre_res_currency(self):
        """ Migration devises """
        self.tables_processed.append('res.currency')
        new_currency_ids = self.connectioncible.search('res.currency', [('active', 'in', ['true', 'false'])], 0, 2000)
        self.connectioncible.write('res.currency', new_currency_ids, {'active': False})
        currency_ids = self.connectionsource.search('res.currency', [], 0, 2000)
        for old_currency_id in currency_ids:
            vals = self.get_values(old_currency_id, 'res.currency', ['name', 'rounding', 'rate', 'active', 'accuracy'])
            res = self.connectioncible.search('res.currency', [('name', '=', vals['name']), ('active', '=', False)])
            if not res:
                try:
                    currency_id = self.connectioncible.create('res.currency', vals)
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, currency_id, vals, "create __migre_res_currency")

            else:
                try:
                    self.connectioncible.write('res.currency', [res[0]], vals)
                    currency_id = res[0]
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, currency_id, vals, "write __migre_res_currency")
            self.add_old_id(old_currency_id, currency_id, 'res.currency')

    def migre_res_company(self):
        """ migration compagnie """
        self.tables_processed.append('res.company')
        res_company_ids = self.connectionsource.search('res.company', [], 0, 1, 'id asc')
        for res_company_id in res_company_ids:
            vals = self.get_values(res_company_id, 'res.company', ['name', 'logo', 'currency_id', 'rml_header'])
            for field in ['bvr_delta_vert', 'bvr_delta_horz', 'bvr_header', 'partner_id']:
                try:
                    vals.pop(field)
                except BaseException:
                    pass
            try:
                vals['partner_id'] = 1
                vals['name'] += ' v70'
                self.connectioncible.write('res.company', self.company_id, vals)
            except BaseException, erreur_base:
                self.__affiche__erreur(erreur_base, res_company_id, vals, "migre_res_company")
            self.add_old_id(self.company_id, 1, 'res.company')

    def __migre_account_type(self):
        """ migration des types de comptes """
        if self.company_id != 1:
            return
        self.tables_processed.append('account.account.type')
        account_account_ids = self.connectioncible.search('account.account.template', [], 0, 20000, 'id asc')
        if account_account_ids:
            self.connectioncible.unlink('account.account.template', account_account_ids)
        account_account_ids = self.connectioncible.search('account.account', [], 0, 20000, 'id asc')
        if account_account_ids:
            self.connectioncible.unlink('account.account', account_account_ids)
        account_account_type_ids = self.connectioncible.search('account.account.type', [], 0, 20000, 'id asc')
        if account_account_type_ids:
            self.connectioncible.unlink('account.account.type', account_account_type_ids)
        account_account_type_ids = self.connectionsource.search('account.account.type', [], 0, 20000, 'id asc')

        for account_account_type_id in account_account_type_ids:
            vals = self.get_values(account_account_type_id, 'account.account.type', ['code', 'name', 'close_method'])
            test_payable = self.connectionsource.search('account.account', [('user_type', '=', account_account_type_id),
                                                                            ('type', 'in', ('payable', 'receivable'))])
            if test_payable:
                vals['close_method'] = "unreconciled"
            res = self.connectioncible.create('account.account.type', vals)
            self.add_old_id(account_account_type_id, res, 'account.account.type')
        return True

    def __create_account(self, account_id, unreconciled_payable, unreconciled_receivable):
        """ migration d'un compte comptable """
        ac_id = None

        vals = self.get_values(account_id, 'account.account',
                               ['code', 'reconcile', 'user_type', 'currency_id', 'company_id', 'shortcut', 'note',
                                'parent_id', 'type', 'active', 'company_currency_id', 'name', 'currency_mode'])

        if 'code' in vals:
            exist_code = self.connectioncible.search('account.account', [('code', '=', vals['code'])], 0, 20000,
                                                     'id asc')
            if exist_code:
                return exist_code[0]
            if vals['code'] != '0' and ('parent_id' in vals and vals['parent_id'] is None):
                parent = self.connectionsource.read('account.account', account_id, ['parent_id'])
                vals['parent_id'] = self.__create_account(parent['parent_id'][0], unreconciled_payable,
                                                          unreconciled_receivable)
                self.add_old_id(parent['parent_id'][0], vals['parent_id'], 'account.account')
                self.account_processed[parent['parent_id'][0]] = vals['parent_id']
        else:
            print "Compte comptable sans code "
            sys.exit(1)
        vals['company_id'] = self.company_id
        if 'currency_id' in vals and vals['currency_id'] == self.company_currency:
            vals['currency_id'] = False
            self.connectionsource.write('account.account', [account_id], {'currency_id': False})
        if vals['type'] not in ('view', 'other', 'receivable', 'payable', 'liquidity', 'consolidation', 'closed'):
            vals['type'] = 'other'
        if vals['type'] == 'payable':
            vals['user_type'] = unreconciled_payable
        elif vals['type'] == 'receivable':
            vals['user_type'] = unreconciled_receivable
        try:
            exist_code = self.connectioncible.search('account.account', [('code', '=', vals['code'])], 0, 20000,
                                                     'id asc')
            if exist_code:
                vals['code'] = vals['code'] + "_" + str(len(exist_code) + 1)
            ac_id = self.connectioncible.create('account.account', vals)
        except BaseException, erreur_base:
            self.__affiche__erreur(erreur_base, account_id, vals, "__migre_account_account")
        return ac_id

    def __migre_account_account(self):
        """ Migration des comptes compables """
        self.tables_processed.append('account.account')
        unreconciled_payable = self.connectioncible.search('account.account.type', [('code', 'like', 'payable')], 0, 1,
                                                           'id asc')
        if not unreconciled_payable:
            print "pas de type payable"
            sys.exit()
        else:
            unreconciled_payable = unreconciled_payable[0]

        unreconciled_receivable = self.connectioncible.search('account.account.type', [('code', 'like', 'receivable')],
                                                              0, 1, 'id asc')
        if not unreconciled_receivable:
            print "pas de type receivable"
            sys.exit()
        else:
            unreconciled_receivable = unreconciled_receivable[0]
        account_ids = self.connectionsource.search('account.account', [('active', 'in', ['true', 'false'])], 0,
                                                   20000000, 'parent_id desc, id asc')
        len_account_ids = len(account_ids)
        x = 0
        for old_account_id in account_ids:
            x += 1
            if (x % 100) == 0:
                print "%s / %s " % (x, len_account_ids)
            if old_account_id not in self.account_processed.keys():
                new_account_id = self.__create_account(old_account_id, unreconciled_payable, unreconciled_receivable)
                self.add_old_id(old_account_id, new_account_id, 'account.account')
                self.account_processed[old_account_id] = new_account_id


    def __migre_account_journal(self):
        """ migre les journaux comptable """
        self.tables_processed.append('account.journal')
        account_journal_ids = self.connectionsource.search('account.journal', [('active', 'in', ['true', 'false'])], 0,
                                                           20000, 'id asc')
        #account_journal_cible_ids = self.connectioncible.search('account.journal', [], 0, 20000, 'id asc')
        #self.connectioncible.unlink('account.journal',account_journal_cible_ids)
        for account_journal_id in account_journal_ids:
            vals = self.get_values(account_journal_id, 'account.journal')
            vals['company_id'] = self.company_id
            if 'currency' in vals:
                if vals['currency'] == self.company_currency:
                    vals['currency'] = False
            else:
                vals['currency'] = False
            if 'default_debit_account_id' in vals and vals['default_debit_account_id']:
                try:
                    if vals['currency']:
                        self.connectioncible.write('account.account', vals['default_debit_account_id'],
                                                   {'currency_id': vals['currency']})
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, vals['default_debit_account_id'], vals,
                                           "write account default_debit_account_id __migre_account_journal")

            if vals['type'] == 'cash':
                vals['type'] = 'bank'
            if 'default_credit_account_id' in vals and vals['default_credit_account_id']:
                try:
                    if vals['currency']:
                        self.connectioncible.write('account.account', vals['default_credit_account_id'],
                                                   {'currency_id': vals['currency']})
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, account_journal_id, vals,
                                           "write account default_credit_account_id __migre_account_journal")

            if not 'company_id' in vals:
                vals['company_id'] = self.company_id

            exist_name = self.connectioncible.search('account.journal', [('name', '=', vals['name'])], 0, 20000,
                                                     'id asc')
            if exist_name:
                name = self.connectionsource.search('ir.translation', [('name', '=', 'account.journal, name'),
                                                                       ('res_id', '=', account_journal_id),
                                                                       ('src', '=', vals['name']),
                                                                       ('lang', '=', 'fr_FR')])
                if name:
                    vals['name'] = self.connectionsource.read('ir.translation', name[0], ['value'])['value']
                    exist_name = self.connectioncible.search('account.journal', [('name', '=', vals['name'])], 0, 20000,
                                                             'id asc')
                vals['name'] = vals['name'] + " - " + vals['code'] + " - " + str(account_journal_id)

            exist_code = self.connectioncible.search('account.journal', [('code', '=', vals['code'])], 0, 20000,
                                                     'id asc')

            if exist_name:
                try:

                    #self.connectioncible.write('account.journal', exist_name[0], vals)
                    self.add_old_id(account_journal_id, exist_name[0], 'account.journal')
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, account_journal_id, vals,
                                           "__migre_account_journal write exist name")
            elif exist_code:
                try:
                    #self.connectioncible.write('account.journal', exist_code[0], vals)
                    self.add_old_id(account_journal_id, exist_code[0], 'account.journal')
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, account_journal_id, vals,
                                           "__migre_account_journal write exist name")
            else:
                try:

                    res = self.connectioncible.create('account.journal', vals)
                    self.add_old_id(account_journal_id, res, 'account.journal')
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, account_journal_id, vals, "__migre_account_journal create")
        self.connectioncible.create('account.journal', {'code': 'cash', 'name': 'Cash Journal', 'type': 'cash',
                                                        'company_id': self.company_id})
        self.connectioncible.create('account.journal',
                                    {'code': 'av-vente', 'name': 'Sale Refund Journal', 'type': 'sale_refund',
                                     'company_id': self.company_id})
        self.connectioncible.create('account.journal',
                                    {'code': 'av-achat', 'name': 'Purchase Refund Journal', 'type': 'purchase_refund',
                                     'company_id': self.company_id})

    def __cree_account_tax_code(self, account_tax_code_id):
        """ creation des codes de taxes """

        vals = self.get_values(account_tax_code_id, 'account.tax.code',
                               ['info', 'name', 'sign', 'parent_id', 'notprintable', 'code'])
        vals['company_id'] = self.company_id
        try:
            account_tax_code_id = self.connectioncible.create('account.tax.code', vals)
        except BaseException, erreur_base:
            self.__affiche__erreur(erreur_base, account_tax_code_id, vals, "__migre_acc_tax_code")
        return account_tax_code_id

    def __migre_acc_tax_code(self):
        """ migre compte de taxe """
        self.tables_processed.append('account.tax.code')
        account_tax_code_ids = self.connectioncible.search('account.tax.code', [], 0, 1000000, 'id asc')
        self.connectioncible.unlink('account.tax.code', account_tax_code_ids)
        account_tax_code_ids = self.connectionsource.search('account.tax.code', [], 0, 1000000, 'id asc')
        for account_tax_code_id in account_tax_code_ids:
            res = self.__cree_account_tax_code(account_tax_code_id)
            self.add_old_id(account_tax_code_id, res, 'account.tax.code')

    def __migre_account_tax(self):
        """ migre les taxes """
        self.tables_processed.append('account.tax')
        account_tax_ids = self.connectionsource.search('account.tax', [('active', 'in', ['true', 'false'])], 0, 1000000,
                                                       'id asc')
        for account_tax_id in account_tax_ids:
            vals = self.get_values(account_tax_id, 'account.tax',
                                   ['ref_base_code_id', 'ref_tax_code_id', 'sequence', 'base_sign', 'child_depend',
                                    'include_base_amount', 'applicable_type', 'company_id', 'tax_code_id',
                                    'python_compute_inv', 'ref_tax_sign', 'type', 'ref_base_sign', 'type_tax_use',
                                    'base_code_id', 'active', 'name', 'account_paid_id', 'account_collected_id',
                                    'amount', 'python_compute', 'tax_sign', 'price_include'])
            vals['company_id'] = self.company_id
            vals['active'] = True
            exist_name = self.connectioncible.search('account.tax',
                                                     [('active', 'in', ['true', 'false']), ('name', '=', vals['name'])],
                                                     0, 20000, 'id asc')
            if exist_name:
                vals['name'] = vals['name'][:60] + "_" + str(account_tax_id)

            try:
                new_account_tax_id = self.connectioncible.create('account.tax', vals)
                self.add_old_id(account_tax_id, new_account_tax_id, 'account.tax')
            except BaseException, erreur_base:
                self.__affiche__erreur(erreur_base, account_tax_id, vals, "__migre_account_tax")

    def __migre_prod_uom_categ(self):
        """ migration categorie des unites produits """
        if self.company_id != 1:
            return
        self.tables_processed.append('product.uom.categ')
        cible_product_uom_categ_ids = self.connectioncible.search('product.uom.categ', [], 0, 1000000, 'id asc')
        self.connectioncible.unlink('product.uom.categ', cible_product_uom_categ_ids)
        product_uom_categ_ids = self.connectionsource.search('product.uom.categ', [], 0, 1000000, 'id asc')
        for product_uom_categ_id in product_uom_categ_ids:

            vals = self.get_values(product_uom_categ_id, 'product.uom.categ')
            try:
                new_product_uom_categ_id = self.connectioncible.create('product.uom.categ', vals)
                self.add_old_id(product_uom_categ_id, new_product_uom_categ_id, 'product.uom.categ')
            except BaseException, erreur_base:
                self.__affiche__erreur(erreur_base, product_uom_categ_id, vals, "__migre_prod_uom_categ")

    def __migre_product_uom(self):
        """ migration unite produit """
        if self.company_id != 1:
            return
        self.tables_processed.append('product.uom')
        cible_product_uom_ids = self.connectioncible.search('product.uom', [], 0, 1000000, 'id asc')
        self.connectioncible.unlink('product.uom', cible_product_uom_ids)
        product_uom_ids = self.connectionsource.search('product.uom', [], 0, 1000000, 'id asc')
        for product_uom_id in product_uom_ids:
            vals = self.get_values(product_uom_id, 'product.uom',
                                   ['active', 'category_id', 'name', 'rounding', 'factor'])
            try:
                new_product_uom_id = self.connectioncible.create('product.uom', vals)
                self.add_old_id(product_uom_id, new_product_uom_id, 'product.uom')
            except BaseException, erreur_base:
                self.__affiche__erreur(erreur_base, product_uom_id, vals, "__migre_product_uom")

    def __migre_account_journal_view(self):
        self.tables_processed.append('account.journal.view')

        account_journal_view_ids = self.connectionsource.search('account.journal.view',
                                                                [('active', 'in', ['true', 'false'])], 0, 20000,
                                                                'id asc')
        for account_journal_view_id in account_journal_view_ids:
            vals = self.get_values(account_journal_view_id, 'account.journal.view')
            vals['company_id'] = self.company_id
            res = self.connectioncible.create('account.journal.view', vals)
            self.add_old_id(account_journal_view_id, res, 'account.journal.view')

    def __migre_acc_analyt_journal(self):
        """ migre les journaux analytiques """
        self.tables_processed.append('account.analytic.journal')
        cible_account_analytic_journal_ids = self.connectioncible.search('account.analytic.journal', [], 0, 20000,
                                                                         'id asc')
        self.connectioncible.unlink('account.analytic.journal', cible_account_analytic_journal_ids)
        account_analytic_journal_ids = self.connectionsource.search('account.analytic.journal', [], 0, 20000, 'id asc')
        for account_analytic_journal_id in account_analytic_journal_ids:
            vals = self.get_values(account_analytic_journal_id, 'account.analytic.journal',
                                   ['name', 'code', 'active', 'type'])
            vals['company_id'] = self.company_id
            try:
                res = self.connectioncible.create('account.analytic.journal', vals)
                if "Timesheet" in vals['name'] and self.hr:
                    employee_ids = self.connectioncible.search('hr.employee', [], 0, 10000)
                    self.connectioncible.write('hr.employee', employee_ids, {'journal_id': res})
                self.add_old_id(account_analytic_journal_id, res, 'account.analytic.journal')
            except BaseException, erreur_base:
                self.__affiche__erreur(erreur_base, account_analytic_journal_id, vals, "__migre_acc_analyt_journal")

    def close_period(self):
        account_period_ids = self.connectionsource.search('account.period', [('state', '!=', 'draft')], 0, 200000,
                                                          'id asc')
        for account_period_id in account_period_ids:
            vals = self.connectionsource.read('account.period', account_period_id)
            res = self.connectioncible.search('account.period', [('date_start', '=', vals['date_start']),
                                                                 ('date_stop', '=', vals['date_stop'])])
            if res:
                self.connectioncible.write('account.period', res, {'state': vals['state']})

    def __migre_acc_fiscal_year(self):
        """ Migration annee fiscale et periode """
        self.tables_processed.append('account.fiscalyear')
        newfy = None
        account_fiscalyear_ids = self.connectionsource.search('account.fiscalyear', [], 0, 100 , 'date_stop desc')
        for account_fiscalyear_id in account_fiscalyear_ids:

            valsfiscal = self.get_values(account_fiscalyear_id, 'account.fiscalyear',
                                         ['date_stop', 'code', 'name', 'date_start', 'start_journal_id', 'company_id',
                                          'state', 'end_journal_id'])
            date_du_jour = datetime.now().strftime('%Y-%m-%d')

            if valsfiscal['date_start'] <= date_du_jour <= valsfiscal['date_stop']:
                self.current_account_period_ids.extend(
                    self.connectionsource.search('account.period', [('fiscalyear_id', '=', account_fiscalyear_id)]))
            valsfiscal['state'] = 'draft'
            valsfiscal['company_id'] = self.company_id

            try:
                newfy = self.connectioncible.create('account.fiscalyear', valsfiscal)
                self.add_old_id(account_fiscalyear_id, newfy, 'account.fiscalyear')


                account_period_ids = self.connectionsource.search('account.period',
                                                                  [('fiscalyear_id', '=', account_fiscalyear_id)], 0, 20000,
                                                                  'id asc')
                for account_period in account_period_ids:
                    period_read = self.connectionsource.read('account.period', account_period)
                    res = self.connectioncible.search('account.period', [('company_id', '=', self.company_id),
                                                                         ('date_start', '=', period_read['date_start']),
                                                                         ('date_stop', '=', period_read['date_stop'])], 0,
                                                      20000, 'id asc')
                    if not res:
                        vals = self.get_values(account_period, 'account.period')
                        vals['fiscalyear_id'] = newfy
                        vals['company_id'] = self.company_id
                        vals['state'] = 'draft'

                        try:
                            res = self.connectioncible.create('account.period', vals)
                            self.add_old_id(account_period, res, 'account.period')
                        except BaseException, erreur_base:

                            try:
                                vals['name'] = vals['name'] + vals['date_stop'].split('-')[0]
                                vals['code'] = vals['code'] + vals['date_stop'].split('-')[0]
                                res = self.connectioncible.create('account.period', vals)
                                self.add_old_id(account_period, res, 'account.period')
                            except BaseException, erreur_base:
                                self.__affiche__erreur(erreur_base, account_period, vals, "__migre_acc_fiscal_year")

                    elif (period_read['date_start'] >= valsfiscal['date_start']
                          and period_read['date_stop'] <= valsfiscal['date_stop']):
                        vals = {}

                        try:
                            self.connectioncible.write('account.period', res[0], vals)
                            self.add_old_id(account_period, res[0], 'account.period')
                        except BaseException, erreur_base:
                            self.__affiche__erreur(erreur_base, account_period, vals, "__migre_acc_fiscal_year")
            except BaseException, erreur_base:
                self.__affiche__erreur(erreur_base, account_fiscalyear_id, valsfiscal, "create fiscalyear")

    def __get_acc_pay_term(self, term_name=None):
        """ Migration des termes de payment """

        new_account_payment_term_id = None
        account_payment_term_id = self.connectioncible.search('account.payment.term', [('name', '=', term_name), (
            'active', 'in', ['true', 'false'])], 0, 2000)
        if account_payment_term_id:
            return account_payment_term_id[0]
        else:
            account_payment_term_ids = self.connectionsource.search('account.payment.term', [('name', '=', term_name), (
                'active', 'in', ['true', 'false'])], 0, 2000)
            for account_payment_term_id in account_payment_term_ids:

                vals = self.get_values(account_payment_term_id, 'account.payment.term')

                if 'line_ids' in vals:
                    vals.pop('line_ids')
                try:
                    new_account_payment_term_id = self.connectioncible.create('account.payment.term', vals)
                    self.add_old_id(account_payment_term_id, new_account_payment_term_id, 'account.payment.term')
                    self.__migre_acc_pay_term_line(account_payment_term_id, new_account_payment_term_id)
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, 0, vals, 'create migre_account_payment_term')

        return new_account_payment_term_id

    def __migre_acc_pay_term_line(self, old, new):
        """ migration des lignes de termes de paiement """
        self.tables_processed.append('account.payment.term.line')
        if self.company_id != 1:
            return
        account_payment_term_ids = self.connectionsource.search('account.payment.term.line', [('payment_id', '=', old)],
                                                                0, 2000)
        for account_payment_term_id in account_payment_term_ids:
            vals = self.get_values(account_payment_term_id, 'account.payment.term.line')

            payment_term = self.connectioncible.search('account.payment.term.line',
                                                       [('payment_id', '=', new), ('name', '=', vals['name'])], 0, 1)
            if not payment_term:
                try:
                    new_account_payment_term_id = self.connectioncible.create('account.payment.term.line', vals)
                    self.add_old_id(account_payment_term_id, new_account_payment_term_id, 'account.payment.term.line')
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, 0, vals, 'create migre_account_payment_term.line')

            else:
                try:
                    self.connectioncible.write('account.payment.term.line', payment_term, vals)
                    self.add_old_id(account_payment_term_id, payment_term[0], 'account.payment.term.line')
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, 0, vals, 'write migre_account_payment_term.line')

    def __migre_res_partner_title(self):
        """  migration titre des partenaires """
        if self.company_id != 1:
            return
        self.tables_processed.append('res.partner.title')
        partner_title_ids = self.connectionsource.search('res.partner.title', [], 0, 2000)
        for partner_title_id in partner_title_ids:
            vals = self.get_values(partner_title_id, 'res.partner.title')
            title = self.connectioncible.search('res.partner.title',
                                                [('domain', '=', vals['domain']), ('name', '=', vals['name'])], 0, 1)
            if not 'shortcut' in vals:
                vals['shortcut'] = vals['name'][:16]
            if not title:
                try:
                    new_partner_title_id = self.connectioncible.create('res.partner.title', vals)
                    self.add_old_id(partner_title_id, new_partner_title_id, 'res.partner.title')
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, partner_title_id, vals, "__migre_res_partner_title")

            else:
                try:
                    self.connectioncible.write('res.partner.title', title, vals)
                    self.add_old_id(partner_title_id, title[0], 'res.partner.title')
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, partner_title_id, vals, "__migre_res_partner_title write")

    def __migre_res_partner_bank(self):
        """ Migration banque partenaire """
        self.tables_processed.append('res.partner.bank')
        partner_bank_ids = self.connectionsource.search('res.partner.bank', [], 0, 2000)
        for partner_bank_id in partner_bank_ids:
            vals = self.get_values(partner_bank_id, 'res.partner.bank')
            if 'bank' in vals and vals['bank']:
                banque = self.connectionsource.read('res.bank', vals['bank'])
                vals['bank'] = self.__bank_get(banque['name'])
            if 'state' in vals:
                vals['state'] = 'bank'
            if not 'acc_number' in vals and 'iban' in vals:
                vals['acc_number'] = vals['iban']
            if not 'acc_number' in vals and 'post_number' in vals:
                vals['acc_number'] = vals['post_number']
            if not 'acc_number' in vals:
                vals['acc_number'] = "123456"
            vals['company_id'] = self.company_id
            try:
                new_partner_bank_id = self.connectioncible.create('res.partner.bank', vals)
                self.add_old_id(partner_bank_id, new_partner_bank_id, 'res.partner.bank')

            except BaseException, erreur_base:
                self.__affiche__erreur(erreur_base, partner_bank_id, vals, "migre res.partner.bank")

    def __migre_res_bank(self):
        """ migration banque """
        self.tables_processed.append('res.bank')
        cible_bank_ids = self.connectioncible.search('res.bank', [], 0, 2000)
        self.connectioncible.unlink('res.bank', cible_bank_ids)
        bank_ids = self.connectionsource.search('res.bank', [], 0, 2000)
        for bank_id in bank_ids:
            vals = self.get_values(bank_id, 'res.bank')
            if not self.__bank_get(vals['name']):

                # for field in ['code', 'bvr_zipcity', 'clearing', 'bvr_name', 'bvr_street']:
                #     try:
                #         vals.pop(field)
                #     except BaseException:
                #         pass
                bank = self.connectioncible.search('res.bank', [('name', '=', vals['name'])], 0, 2)
                if not bank:
                    try:
                        res = self.connectioncible.create('res.bank', vals)
                        self.add_old_id(bank_id, res, 'res.bank')

                    except BaseException, erreur_base:
                        self.__affiche__erreur(erreur_base, bank_id, vals, "__migre_res_bank")

    def __migre_res_part_bnk_type(self):
        """ migration type de banque """
        if self.company_id != 1:
            return
        self.tables_processed.append('res.partner.bank.type')
        cible_partner_bank_ids = self.connectioncible.search('res.partner.bank.type', [], 0, 2000)
        self.connectioncible.unlink('res.partner.bank.type', cible_partner_bank_ids)
        partner_bank_ids = self.connectionsource.search('res.partner.bank.type', [], 0, 2000)
        for partner_bank_id in partner_bank_ids:
            vals = self.get_values(partner_bank_id, 'res.partner.bank.type', ['code', 'name'])
            if 'field_ids' in vals:
                vals.pop('field_ids')

            res_partner_bank_id = self.connectioncible.search('res.partner.bank.type', [('name', '=', vals['name'])], 0,
                                                              2)
            if not res_partner_bank_id:
                try:
                    res = self.connectioncible.create('res.partner.bank.type', vals)
                    self.add_old_id(partner_bank_id, res, 'res.partner.bank.type')
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, partner_bank_id, vals, "__migre_res_part_bnk_type")
            else:
                try:
                    self.add_old_id(partner_bank_id, res_partner_bank_id[0], 'res.partner.bank.type')
                except sqlite3.OperationalError:
                    pass

    def __create_analytic_account(self, account_analytic_id):
        """ Creation d'un compte analytique """
        vals = self.get_values(account_analytic_id, 'account.analytic.account',
                               ['code', 'contact_id', 'date', 'partner_id', 'user_id', 'date_start', 'company_id',
                                'parent_id', 'state', 'complete_name', 'description', 'name'])

        res = self.connectioncible.search('account.analytic.account', [('code', '=', vals['code'])], 0, 1)
        if res:
            return res[0]
        vals['company_id'] = self.company_id
        vals['use_timesheets'] = True
        res = None
        try:
            res = self.connectioncible.create('account.analytic.account', vals)
        except BaseException, erreur_base:
            self.__affiche__erreur(erreur_base, account_analytic_id, vals, "__migre_acc_analytic_acc")
        return res

    def __create_analytic_line(self, account_analytic_line_id):
        vals = self.get_values(account_analytic_line_id, 'account.analytic.line')
        vals['company_id'] = self.company_id
        #line_exist = self.connectioncible.create('account.analytic.line'
        res = None
        try:
            res = self.connectioncible.create('account.analytic.line', vals)
        except BaseException, erreur_base:
            self.__affiche__erreur(erreur_base, account_analytic_line_id, vals, "__migre_acc_analytic_line")
        return res

    def __migre_acc_analytic_line(self):
        self.tables_processed.append('account.analytic.line')
        if self.hr:
            to_invoice_ids = self.connectionsource.search('hr_timesheet_invoice.factor', [('factor', '=', 0)])
            account_analytic_line_ids = self.connectionsource.search('account.analytic.line',
                                                                     [('invoice_id', '=', False),
                                                                      ('to_invoice', 'in', to_invoice_ids)],
                                                                     0, 200000,
                                                                     'id desc')
            account_analytic_line_ids.extend(
                self.connectionsource.search('account.analytic.line', [], 0, 200000,
                                             'id desc'))
        else:
            account_analytic_line_ids = self.connectionsource.search('account.analytic.line', [], 0, 200000,
                                                                     'id desc')
        compteur = 0
        nbr = len(account_analytic_line_ids)
        for account_analytic_line_id in account_analytic_line_ids:
            compteur += 1
            if (compteur == 1) or ((compteur % 100) == 0):
                print "Account Analytic Line %s / %s " % (compteur, nbr)
            res = self.__create_analytic_line(account_analytic_line_id)
            if res:
                self.add_old_id(account_analytic_line_id, res, 'account.analytic.line')
            else:
                print "Erreur de creation de ligne old is %s " % account_analytic_line_id
        len_account_analytic_source = len(account_analytic_line_ids)
        account_analytic_line_ids = self.connectioncible.search('account.analytic.line', [], 0, 200000, 'id desc')
        len_account_analytic_cible = len(account_analytic_line_ids)

        self.controle_analytique()

    def controle_analytique(self):
        self.curseur.execute('select sum(amount)::float  , sum(unit_amount)::float  from account_analytic_line')
        total_cible = self.curseur.fetchall()
        self.curseur_source.execute(
            "select sum(amount)::float , sum(unit_amount)::float from account_analytic_line ")
        total_source = self.curseur_source.fetchall()
        print 'total_source ', total_source
        print 'total_cible  ', total_cible
        if total_source[0][0] is not None and total_cible[0][1] is not None:
            if (abs(total_source[0][0] - total_cible[0][0]) > 0.1) or abs(total_source[0][1] - total_cible[0][1]) > 0.1:
                print "Total source controle analytique ", total_source
                print "Total cible  controle analytique ", total_cible
                print "ecart amount %s " % abs(total_source[0][0] - total_cible[0][0])
                print "ecart uni_amount %s " % abs(total_source[0][1] - total_cible[0][1])
            else:
                print "Controle Analytique OK"

    def __migre_acc_analytic_acc(self):
        """ migration des comptes analytiques """
        self.tables_processed.append('account.analytic.account')
        cible_account_analytic_ids = self.connectioncible.search('account.analytic.account', [], 0, 20000,
                                                                 'parent_id desc')
        self.connectioncible.unlink('account.analytic.account', cible_account_analytic_ids)
        account_analytic_ids = self.connectionsource.search('account.analytic.account',
                                                            [('active', 'in', ['true', 'false'])], 0, 20000,
                                                            'parent_id desc')
        compteur = 0
        nbr = len(account_analytic_ids)
        for account_analytic_id in account_analytic_ids:
            compteur += 1
            #if (compteur == 1) or ((compteur % 100) == 0):
            #    print "Account Analytic %s / %s " % (compteur, nbr)
            res = self.__create_analytic_account(account_analytic_id)

            if res:
                self.add_old_id(account_analytic_id, res, 'account.analytic.account')
            else:
                print "Erreur de creation de compte analytique old is %s " % account_analytic_id

    def __migre_account_bank_statement(self):
        """ migration etat de banque """
        new_account_bank_statement_id = None
        self.tables_processed.append('account.bank.statement')
        self.tables_processed.append('account.bank.statement.line')
        account_bank_statement_ids = self.connectionsource.search('account.bank.statement', [
            ('period_id', 'in', self.current_account_period_ids)], 0, 1000000, 'id asc')
        nbr = len(account_bank_statement_ids)
        compteur = 0
        for account_bank_statement_id in account_bank_statement_ids:
            vals = self.get_values(account_bank_statement_id, 'account.bank.statement',
                                   ['name', 'state', 'balance_end', 'balance_start', 'journal_id', 'currency',
                                    'period_id', 'date', 'balance_end_real'])
            compteur += 1
            #if (compteur == 1) or ((compteur % 100) == 0):
            #    print "Account bank statement %s / %s " % (compteur, nbr)

            vals['company_id'] = self.company_id
            vals['total_entry_encoding'] = vals['balance_end_real'] - vals['balance_start']
            #vals['state'] = 'draft'
            try:
                new_account_bank_statement_id = self.connectioncible.create('account.bank.statement', vals)
                self.add_old_id(account_bank_statement_id, new_account_bank_statement_id, 'account.bank.statement')

            except BaseException, erreur_base:
                print "Journal id ", vals['journal_id']
                self.__affiche__erreur(erreur_base, account_bank_statement_id, vals, "__migre_account_bank_statement")

            self.__migre_acc_bnk_stat_line(account_bank_statement_id, new_account_bank_statement_id)

    def __migre_acc_bnk_stat_line(self, account_bank_statement_id, new_account_bank_statement_id):
        """ migre les lignes de etat de banque """

        account_bank_statement_line_ids = self.connectionsource.search('account.bank.statement.line', [
            ('statement_id', '=', account_bank_statement_id)], 0, 1000000, 'id asc')
        for account_bank_statement_line_id in account_bank_statement_line_ids:
            vals = self.get_values(account_bank_statement_line_id, 'account.bank.statement.line',
                                   ['type', 'reconcile_id','account_id', 'amount', 'date', 'partner_id', 'name'])
            vals['company_id'] = self.company_id
            vals['statement_id'] = new_account_bank_statement_id

            try:
                if 'account_id' in vals:
                    new_account_bank_st_line_id = self.connectioncible.create('account.bank.statement.line', vals)
                    self.add_old_id(account_bank_statement_line_id, new_account_bank_st_line_id,
                                    'account.bank.statement.line')
            except BaseException, erreur_base:
                self.__affiche__erreur(erreur_base, account_bank_statement_line_id, vals, "__migre_acc_bnk_stat_line")

    def __migre_account_invoice(self):
        """ migration factures """
        if not self.current_account_period_ids:
            self.__migre_acc_fiscal_year()
        res = self.connectioncible.search('account.journal', [('code', '=', 'av-vente')])
        if res:
            journal_sale_refund = res[0]
        else:
            journal_sale_refund = self.connectioncible.create('account.journal',
                                                              {'type': 'sale_refund', 'name': 'Sale Refund Journal',
                                                               'code': "av-vente", 'company_id': 1})
        res = self.connectioncible.search('account.journal', [('code', '=', 'av-achat')])
        if res:
            journal_purchase_refund = res[0]
        else:
            journal_purchase_refund = self.connectioncible.create('account.journal', {'type': 'purchase_refund',
                                                                                      'name': 'Purchase Refund Journal',
                                                                                      'code': "av-achat",
                                                                                      'company_id': 1})
        self.tables_processed.append('account.invoice')
        self.tables_processed.append('account.invoice.line')
        account_invoice_ids = self.connectionsource.search('account.invoice', [('state', '!=', 'cancel'), (
            'period_id', 'in', self.current_account_period_ids)], 0, 1000000, 'id asc')
        nbr = len(account_invoice_ids)
        compteur = 0
        for old_account_invoice_id in account_invoice_ids:
            compteur += 1
            vals = self.get_values(old_account_invoice_id, 'account.invoice',
                                   ['period_id', 'move_id', 'date_due', 'check_total', 'payment_term', 'number',
                                    'journal_id', 'currency_id', 'address_invoice_id', 'reference', 'account_id',
                                    'amount_untaxed', 'address_contact_id', 'reference_type', 'company_id',
                                    'amount_tax', 'state', 'type', 'date_invoice', 'amount_total', 'partner_id', 'name',
                                    'create_uid'])
            vals['company_id'] = self.company_id
            if (compteur % 100) == 0:
                print "Account invoice :  ", compteur, '/', nbr

            if 'number' in vals:
                vals['internal_number'] = vals['number']
                vals.pop('number')
            if 'payment_term' in vals and vals['payment_term']:
                payment_term = self.connectioncible.read('account.payment.term', vals['payment_term'])
                if payment_term:
                    term_name = payment_term['name']
                    if term_name:
                        vals['payment_term'] = self.__get_acc_pay_term(term_name)
            if not 'period_id' in vals:
                periode = self.connectioncible.search('account.period', [('special', '=', False),
                                                                         ('date_stop', '>=', vals['date_invoice']),
                                                                         ('date_start', '<=', vals['date_invoice'])])
                if periode:
                    vals['period_id'] = periode[0]
            if 'address_invoice_id' in vals:
                vals.pop('address_invoice_id')
            if 'address_contact_id' in vals:
                vals.pop('address_contact_id')
            if 'number' in vals and vals['number'] == '/':
                vals['number'] = "/" + str(old_account_invoice_id)
            if not 'number' in vals:
                vals['number'] = "/" + str(old_account_invoice_id)
            save_state = vals['state']
            vals['state'] = 'draft'
            new_invoice_id = False
            if vals['type'] == 'in_refund':
                vals['journal_id'] = journal_purchase_refund
            if vals['type'] == 'out_refund':
                vals['journal_id'] = journal_sale_refund
            try:
                new_invoice_id = self.connectioncible.create('account.invoice', vals)
                self.add_old_id(old_account_invoice_id, new_invoice_id, 'account.invoice')

                self.connectioncible.write('account.invoice', new_invoice_id, {'number': vals['number']})
            except BaseException, erreur_base:
                self.__affiche__erreur(erreur_base, 0, vals, "__migre_account_invoice create")
            if new_invoice_id:
                self.__migre_account_invoice_line(old_account_invoice_id, new_invoice_id)

                if save_state != 'draft':
                    self.__valid_invoice(new_invoice_id)
                else:
                    self.connectioncible.execute('account.invoice', 'button_reset_taxes', [new_invoice_id])
                self.connectioncible.write('account.invoice', new_invoice_id, {'state': save_state})

        print "%s factures " % nbr
        #Controle Invoice
        print "Controle Invoice "
        self.curseur.execute('select sum(price_subtotal)::float from account_invoice_line')
        cible_total_invoice_line = self.curseur.fetchall()[0][0]
        self.curseur_source.execute(
            "select sum(price_subtotal)::float from account_invoice_line where invoice_id in"
            " (select id from account_invoice where period_id in %s and state != 'cancel')" %
            str(tuple(self.current_account_period_ids)))
        source_total_invoice_line = self.curseur_source.fetchall()[0][0]
        self.curseur_source.execute(
            "select sum(amount_total)::float from account_invoice  where id in "
            "(select id from account_invoice where period_id in %s and state != 'cancel')"
            % str(tuple(self.current_account_period_ids)))
        source_total_invoice = self.curseur_source.fetchall()[0][0]
        self.curseur.execute('select sum(amount_total)::float from account_invoice')
        cible_total_invoice = self.curseur.fetchall()[0][0]
        if cible_total_invoice and source_total_invoice and  (abs(source_total_invoice - cible_total_invoice) > 0.1 or \
            abs(source_total_invoice_line - cible_total_invoice_line) > 0.1):
            print "cible_total_invoice %s source_total_invoice % ecart  ", (
                cible_total_invoice, source_total_invoice, source_total_invoice - cible_total_invoice)
            print "cible_total_invoice_line %s source_total_invoice_line % ecart  ", (
                cible_total_invoice_line, source_total_invoice_line,
                source_total_invoice_line - cible_total_invoice_line)
        else:
            print "Controle Invoice ok"

    def __migre_account_invoice_line(self, invoice_id, new_invoice_id):
        """ Migration Lignes de factures """
        account_invoice_line_ids = self.connectionsource.search('account.invoice.line',
                                                                [('invoice_id', '=', invoice_id)], 0, 1000000, 'id asc')
        for old_account_invoice_line_id in account_invoice_line_ids:
            price_subtotal = self.connectionsource.read('account.invoice.line', old_account_invoice_line_id,
                                                        ['price_subtotal'])
            vals = self.get_values(old_account_invoice_line_id, 'account.invoice.line')
            vals['invoice_id'] = new_invoice_id
            vals['company_id'] = self.company_id
            if price_subtotal['price_subtotal'] != (vals['quantity'] * vals['price_unit']):
                if vals['discount']:
                    vals['price_unit'] = (price_subtotal['price_subtotal'] / vals['quantity']) * 100 / vals[
                        'discount']
                else:
                    vals['price_unit'] = (price_subtotal['price_subtotal'] / vals['quantity'])
                    #   print "change price unit  " , vals['price_unit']

            for val in ['state', 'price_subtotal_incl']:
                if val in vals:
                    vals.pop(val)

            if 'note' in vals:
                vals.pop('note')

            try:
                new_account_invoice_line_id = self.connectioncible.create('account.invoice.line', vals)
                self.add_old_id(old_account_invoice_line_id, new_account_invoice_line_id, 'account.invoice')
            except BaseException, erreur_base:
                self.__affiche__erreur(erreur_base, old_account_invoice_line_id, vals,
                                       "__migre_account_invoice_line create")

    def migre_product_price_list_item(self, pricelist_id, new_pricelist_id):
        product_pricelist_item_ids = self.connectionsource.search('product.pricelist.item',
                                                                  [('base_pricelist_id', '=', pricelist_id)], 0, LIMITE)
        for product_pricelist_item_id in product_pricelist_item_ids:

            vals = self.get_values(product_pricelist_item_id, 'product.pricelist.item',
                                   ['active', 'date_end', 'date_start', 'name'])
            vals['pricelist_id'] = new_pricelist_id

            try:
                res = self.connectioncible.create('product.pricelist.item', vals)
                self.add_old_id(product_pricelist_item_id, res[0], 'product.pricelist.item')
            except BaseException, erreur_base:
                print "product.pricelist.item ", erreur_base.__str__()
                #self.__affiche__erreur(erreur_base, product_pricelist_item_id, vals, "__migre_price_list item")

    def migre_product_price_list_version(self, pricelist_id, new_pricelist_id):
        self.tables_processed.append('product.pricelist.version')
        product_pricelist_version_ids = self.connectionsource.search('product.pricelist.version',
                                                                     [('pricelist_id', '=', pricelist_id)], 0, LIMITE)
        for product_pricelist_version_id in product_pricelist_version_ids:
            vals = self.get_values(product_pricelist_version_id, 'product.pricelist.version',
                                   ['active', 'date_end', 'date_start', 'name'])
            vals['pricelist_id'] = new_pricelist_id
            try:
                res = self.connectioncible.create('product.pricelist.version', vals)
                self.add_old_id(product_pricelist_version_id, res, 'product.pricelist.version')
            except BaseException, erreur_base:
                print "migre_product_price_list_version ", erreur_base.__str__()

    def migre_product_price_list(self):
        self.tables_processed.append('product.pricelist')
        product_pricelist_ids = self.connectionsource.search('product.pricelist', [], 0, LIMITE)
        for product_pricelist_id in product_pricelist_ids:
            price_list = self.connectionsource.read('product.pricelist', product_pricelist_id)
            res = self.connectioncible.search('product.pricelist', [('name', '=', price_list['name'])], 0, LIMITE)
            vals = {}
            if not res:
                vals = self.get_values(product_pricelist_id, 'product.pricelist',
                                       ['active', 'currency_id', 'type', 'name'])
                res_id = None
                try:
                    res_id = self.connectioncible.create('product.pricelist', vals)
                    self.add_old_id(product_pricelist_id, res_id, 'product.pricelist')
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, product_pricelist_id, vals, "__migre_price_list")
                self.migre_product_price_list_version(product_pricelist_id, res_id)
            else:
                self.connectioncible.write('product.pricelist', res, vals)
                self.add_old_id(product_pricelist_id, res[0], 'product.pricelist')

    def __migre_res_part_addr(self, partner_id=None, partner_new_id=None):
        """ migration des adresses partenaires """
        if not 'res.partner.address' in self.tables_processed:
            self.tables_processed.append('res.partner.address')
        if partner_id:
            res_partner_address_ids = self.connectionsource.search('res.partner.address',
                                                                   [('partner_id', '=', partner_id)], 0, 99999999,
                                                                   'id asc')
        else:
            res_partner_address_ids = self.connectionsource.search('res.partner.address', [('partner_id', '=', False)],
                                                                   0, 99999999, 'id asc')

        for res_partner_address_id in res_partner_address_ids:
            vals = self.get_values(res_partner_address_id, 'res.partner.address',
                                   ['name', 'type', 'partner_id', 'street', 'street2', 'zip', 'city', 'country_id',
                                    'phone', 'mail'])
            if not vals:
                continue
            if ('name' not in vals or not vals['name']) and partner_new_id:
                name = self.connectioncible.read('res.partner', partner_new_id, ['name'])['name']
                vals['name'] = name
            elif 'name' not in vals or not vals['name']:
                continue
            if 'partner_id' in vals:
                vals.pop('partner_id')
            vals['company_id'] = self.company_id
            if partner_new_id:
                vals['parent_id'] = partner_new_id
            if 'type' in vals:
                if vals['type'] not in ('default', 'invoice', 'delivery', 'contact', 'other'):
                    vals['type'] = 'other'

            if 'title' in vals:
                title = self.connectioncible.search('res.partner.title',
                                                    [('domain', '=', "contact"), ('name', '=', vals['title'])], 0, 2)
                if not title:
                    title = self.connectioncible.search('res.partner.title',
                                                        [('domain', '=', "contact"), ('shortcut', '=', vals['title'])],
                                                        0, 2)
                if title:
                    vals['title'] = title[0]
                else:
                    vals['title'] = self.connectioncible.create('res.partner.title',
                                                                {'domain': 'contact', 'name': vals['title'],
                                                                 'shortcut': vals['title']})
            vals['customer'] = False
            if partner_id and ((('type' in vals and vals['type'] == 'invoice')) or len(res_partner_address_ids) == 1):
                for k in vals.keys():
                    if k not in ('street', 'street2', 'zip', 'city', 'country_id', 'phone', 'mail'):
                        vals.pop(k)
                new_res_partner_address_id = partner_new_id
                self.connectioncible.write('res.partner', partner_new_id, vals)
            else:
                new_res_partner_address_id = self.connectioncible.create('res.partner', vals)
            self.add_old_id(res_partner_address_id, new_res_partner_address_id, 'res.partner.address')

    def __migre_res_partner(self):
        """ migration partenaire """
        self.tables_processed.append('res.partner')
        partner_ids = self.connectionsource.search('res.partner', [('active', 'in', ['true', 'false'])], 0, 20000,
                                                   'id asc')
        nbr = len(partner_ids)
        compteur = 0
        for partner_id in partner_ids:
            compteur += 1
            vals = self.get_values(partner_id, 'res.partner',
                                   ['property_product_pricelist', 'city', 'property_account_payable', 'debit', 'vat',
                                    'website', 'customer', 'supplier', 'date', 'active', 'lang', 'credit_limit', 'name',
                                    'country', 'property_account_receivable', 'credit', 'debit_limit', 'category_id'])
            vals['company_id'] = self.company_id

            if 'country' in vals:
                vals['country_id'] = vals['country']
                vals.pop('country')
            vals['is_company'] = 1
            if (compteur % 100) == 0:
                print "Partenaire %s/%s pour la company %s " % (compteur, nbr, self.company_id)
            if 'vat' in vals:
                try:
                    result_check = self.connectioncible.object.execute(self.connectioncible.dbname,
                                                                       self.connectioncible.uid,
                                                                       self.connectioncible.pwd, 'res.partner',
                                                                       'simple_vat_check', 'ch', vals['vat'])
                    if result_check is False:
                        vals['vat'] = ""
                except BaseException:
                    pass

            if 'lang' in vals and vals['lang'] == 'fr_FR':
                vals['lang'] = 'fr_CH'

            if partner_id == 1:
                self.connectioncible.write('res.partner', 1, vals)
                partner_new_id = 1
            else:
                try:
                    partner_new_id = self.connectioncible.create('res.partner', vals)

                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, partner_id, vals, "create partner __migre_res_partner")

            self.add_old_id(partner_id, partner_new_id, 'res.partner')
            self.__migre_res_part_addr(partner_id, partner_new_id)

    def sum_account_move_cible(self):
        query = 'select sum(debit)::float as debit, sum(credit)::float as credit from account_move_line '
        self.curseur.execute(query)
        res = self.curseur.fetchone()
        debit = float(res[0])
        credit = float(res[1])
        if res[0]:
            debit = float(res[0])
        else:
            debit = 0
        if res[1]:
            credit = float(res[1])
        else:
            credit = 0
        res = {'debit': debit, 'credit': credit}
        return res

    def sum_account_move_source(self, move_ids):
        somme_debit = 0
        somme_credit = 0
        if len(move_ids) == 1:
            str_ids = '(' + str(move_ids[0]) + ')'
        else:
            str_ids = str(tuple(move_ids))
            #print "str_ids ",str_ids
        requete = 'select sum(debit)::float as debit, sum(credit)::float as ' \
                  'credit from account_move_line where  move_id in ' + str_ids
        self.curseur_source.execute(requete)
        res = self.curseur_source.fetchone()
        if res[0]:
            debit = float(res[0])
        else:
            debit = 0
        if res[1]:
            credit = float(res[1])
        else:
            credit = 0

        return {'debit': debit, 'credit': credit}

    def migre_reconcile(self):
        self.curseur_source.execute("select  id , type, name from account_move_reconcile")
        for line in self.curseur_source.fetchall():
            self.curseur.execute("insert into account_move_reconcile (id,type,name) values (%s,'%s','%s')" % line)
        self.conn.commit()

    def __migre_account_move(self):
        """ Migration des ecritures comptables """
        #  TODO
        print "self.current_account_period_ids", self.current_account_period_ids
        self.tables_processed.append('account.move')
        self.tables_processed.append('account.move.line')
        account_move_ids = self.connectionsource.search('account.move',
                                                        [('period_id', 'in', self.current_account_period_ids),
                                                         ('line_id', '<>', False)], 0, LIMITE, 'id asc')
        source_nbr_line = len(
            self.connectionsource.search('account.move.line', [('period_id', 'in', self.current_account_period_ids)], 0,
                                         LIMITE))
        source_nbr_move = len(account_move_ids)
        compteur = 0
        self.move_already_process = []
        for account_move_id in account_move_ids:

            compteur += 1
            if (compteur % 100) == 0:
                print "Account Move %s / %s'" % (compteur, source_nbr_move)

            vals = self.get_values(account_move_id, 'account.move',
                                   ['ref', 'name', 'state', 'partner_id', 'journal_id', 'period_id', 'date',
                                    'to_check'])
            vals['company_id'] = self.company_id
            if vals['name'] == '/':
                vals['name'] = str(account_move_id)
            vals['state'] = 'draft'
            try:
                champs = ''
                values = ''
                for k in vals.keys():
                    if vals[k]:
                        champs = champs + k + ','
                        if isinstance(vals[k], int) or isinstance(vals[k], float):
                            values = values + str(vals[k]) + ','
                        elif isinstance(vals[k], str):
                            values = values + "'" + str(vals[k].replace("'", " ")) + "',"
                        elif isinstance(vals[k], unicode):
                            values = values + "'" + vals[k].replace("'", " ") + "',"
                        elif isinstance(vals[k], bool):
                            if vals[k] is True:
                                values += "True,"
                            else:
                                values += "False,"
                        else:
                            print k
                            print type(vals[k])
                            sys.exit()

                champs = champs[:-1]
                values = values[:-1]
                requete = "insert into account_move (%s) values (%s) returning id  " % (champs, values)
                self.curseur.execute(requete)
                new_move_id = self.curseur.fetchone()[0]
                self.conn.commit()
                #new_move_id = self.connectioncible.create('account.move', vals)
                self.add_old_id(account_move_id, new_move_id, 'account.move')
            except BaseException, erreur_base:

                self.__affiche__erreur(erreur_base, account_move_id, vals, "__migre_account_move ")
                sys.exit()
                # try:
            self.__migre_acc_move_line(account_move_id, new_move_id)

            # except BaseException, erreur_base:
            #     print "Erreur on move line account_move_id : %s, new_move_id : %s " % (account_move_id, new_move_id)
            self.move_already_process.append(account_move_id)

            somme_cible = self.sum_account_move_cible()
            somme_source = self.sum_account_move_source(self.move_already_process)
            if (abs(somme_cible['debit'] - somme_source['debit']) > 0.1) or\
                    (abs(somme_cible['credit'] - somme_source['credit']) > 0.1):
                print "cible debit %s , credit %s " % (somme_cible['debit'], somme_cible['credit'])
                print "source debit %s , credit %s " % (somme_source['debit'], somme_source['credit'])
                print "ecart"
                #print "already_process ",self.move_already_process
                print "account_move_id ", account_move_id
                print "abs(somme_cible['debit'] -somme_source['debit']) ", abs(
                    somme_cible['debit'] - somme_source['debit'])
                print "abs(somme_cible['credit'] - somme_source['credit']) ", abs(
                    somme_cible['credit'] - somme_source['credit'])
                if len(self.move_already_process) == 1:
                    source_ids = self.connectionsource.search('account.move.line',
                                                              [('move_id', '=', self.move_already_process[0])], 0,
                                                              LIMITE, 'id asc')
                else:
                    str_ids = str(tuple(self.move_already_process))
                    source_ids = self.connectionsource.search('account.move.line', [('move_id', 'in', str_ids)], 0,
                                                              LIMITE, 'id asc')

                cible_ids = self.connectioncible.search('account.move.line', [], 0, LIMITE, 'id asc')
                print "source_ids ", source_ids
                print "Source "
                for line_id in source_ids:
                    line = self.connectionsource.read('account.move.line', line_id,
                                                      ['name', 'account_id', 'debit', 'credit'])
                    print line['account_id'][1], ";", line['name'], ";", line['debit'], ";", line['credit']
                print "Cible "
                for line_id in cible_ids:
                    line = self.connectioncible.read('account.move.line', line_id,
                                                     ['name', 'account_id', 'debit', 'credit'])
                    print line['account_id'][1], ";", line['name'], ";", line['debit'], ";", line['credit']
                sys.exit()
            try:
                self.connectioncible.execute('account.move', 'button_validate', [new_move_id])
            except Exception, e:
                print e
                print sys.exc_info()
                print "Erreur sur validation de la piece "
                print "Move id source %s " % account_move_id
                line_ids = self.connectionsource.search('account.move.line', [('move_id', '=', account_move_id)], 0,
                                                        LIMITE)
                for line_id in line_ids:
                    print self.connectionsource.read('account.move.line', line_id,
                                                     ['id', 'name', 'debit', 'credit', 'currency_id'])
                pass
        cible_nbr_move = len(self.connectioncible.search('account.move', [], 0, LIMITE))
        cible_nbr_line = len(self.connectioncible.search('account.move.line', [], 0, LIMITE))
        print "source %s move %s lignes " % (source_nbr_move, source_nbr_line)
        print "cible  %s move %s lignes " % (cible_nbr_move, cible_nbr_line)

    def _check_currency_company(self, ids, context=None):
        for l in self.connectionsource.read('account.move.line', ids):
            if l['currency_id'] and (l['currency_id'][0] == self.company_currency_source):
                #print "l.currency_id" , l['currency_id']
                #print "l.currency_id.id ", l['currency_id'] and l['currency_id'][0]
                #print "Error company"
                self.connectionsource.write('account.move.line', ids, {'currency_id': None})
                return False
        return True

    def _check_currency(self, ids):
        for l in self.connectionsource.read('account.move.line', ids):
            account = self.connectionsource.read('account.account', l['account_id'][0], ['currency_id'])
            if account['currency_id']:
                if not l['currency_id'] or not l['currency_id'][0] == account['currency_id'][0]:
                    print "l = %s " % l
                    print "self.company_currency ", self.company_currency_source
                    print "account['currency_id'] ", account['currency_id']
                    print "l.currency_id", l['currency_id']
                    print "l.currency_id.id ", l['currency_id'] and l['currency_id'][0]
                    print "l.account_id.currency_id.id ", account['currency_id'][0]
                    return False

            if l['amount_currency'] and not l['currency_id']:
                print "Erreur amount currency and not currency "
                return False

        return True

    def __migre_acc_move_line(self, move_id, new_move_id):
        """ migration ligne d'ecritures"""
        account_move = self.connectioncible.read('account.move', new_move_id)
        #print "migre_acc_move_line(self, move_id, new_move_id): " , move_id, new_move_id
        account_move_line_ids = self.connectionsource.search('account.move.line', [('move_id', '=', move_id)], 0,
                                                             222000, 'id desc')
        #print "len source account_move_line_ids ", len(account_move_line_ids)
        for account_move_line_id in account_move_line_ids:
            vals = self.get_values(account_move_line_id, 'account.move.line',
                                   ['debit', 'credit', 'statement_id', 'currency_id', 'date_maturity', 'invoice',
                                    'partner_id', 'blocked', 'analytic_account_id', 'centralisation', 'journal_id',
                                    'tax_code_id', 'state', 'amount_taxed', 'ref', 'origin_link', 'account_id',
                                    'period_id', 'amount_currency', 'date', 'move_id', 'name', 'tax_amount',
                                    'product_id', 'account_tax_id', 'product_uom_id', 'followup_line_id', 'quantity'])
            #            print " account_move_line_id ", account_move_line_id, vals
            if self.reconciliation:
                mv_reconcile = self.connectionsource.read('account.move.line', account_move_line_id, ['reconcile_id'])
                if mv_reconcile['reconcile_id']:
                    vals['reconcile_id'] = mv_reconcile['reconcile_id'][0]
            res = self._check_currency_company([account_move_line_id])
            if not res:
                vals[
                    'currency_id'] = None
                #print "Erreur devise company"
            res = self._check_currency([account_move_line_id])

            if not res:
                print "check currency %s for %s " % (res, account_move_line_id)
                print "Erreur de devise "
                print "Vals = ", vals

            vals['company_id'] = self.company_id
            if 'amount_taxed' in vals:
                vals['tax_amount'] = vals['amount_taxed']
                vals.pop('amount_taxed')

            if 'period_id' in vals:
                # incoherence de periode entre move et line
                if account_move and account_move['period_id'] and (vals['period_id'] != account_move['period_id'][0]):
                    vals['period_id'] = account_move['period_id'][0]
            if 'name' in vals:
                if vals['name'] == '':
                    vals['name'] = str(account_move_line_id)
            else:
                vals['name'] = str(account_move_line_id)
            vals['move_id'] = new_move_id
            vals['state'] = 'draft'
            vals['analytic_lines'] = []

            champs = ''
            values = ''

            for k in vals.keys():
                if (k in ('debit', 'credit')) or vals[k]:
                    champs = champs + k + ','
                    if isinstance(vals[k], int) or isinstance(vals[k], float):
                        values = values + str(vals[k]) + ','
                    elif isinstance(vals[k], str):
                        values = values + "'" + str(vals[k].replace("'", " ")) + "',"
                    elif isinstance(vals[k], unicode):
                        values = values + "'" + vals[k].replace("'", " ") + "',"
                    elif isinstance(vals[k], bool):
                        if vals[k]:
                            values += "True,"
                        else:
                            values += "False,"
                    else:
                        print k
                        print type(vals[k])
                        sys.exit()

            champs = champs[:-1]
            values = values[:-1]
            requete = "insert into account_move_line (%s) values (%s) returning id  " % (champs, values)
            self.curseur.execute(requete)
            new_id = self.curseur.fetchone()[0]
            self.add_old_id(account_move_line_id, new_id, 'account.move.line')
            self.conn.commit()

    def __create_partner_categ(self, partner_category_id):
        """ creation des categories partenaires """
        vals = self.get_values(partner_category_id, 'res.partner.category')
        categ = None
        if 'child_ids' in vals:
            vals.pop('child_ids')
        if 'name' in vals:
            categ = self.connectioncible.search('res.partner.category', [('name', '=', vals['name'])], 0, 2)
        else:
            vals['name'] = 'undefined'

        #=======================================================================
        # if vals.has_key('parent_id'):
        #    res = self.__create_partner_categ(vals['parent_id'])
        #    self.add_old_id(vals['parent_id'] , res ,'res.partner.category')
        #
        #=======================================================================
        if not categ:
            try:
                res = self.connectioncible.create('res.partner.category', vals)
                self.add_old_id(partner_category_id, res, 'res.partner.category')
            except BaseException, erreur_base:
                self.__affiche__erreur(erreur_base, 0, vals, "__migre_res_partner_category create")
        else:
            try:
                self.connectioncible.write('res.partner.category', categ[0], vals)
                self.add_old_id(partner_category_id, categ[0], 'res.partner.category')

            except BaseException, erreur_base:
                self.__affiche__erreur(erreur_base, 0, vals, "__migre_res_partner_category write")

    def __migre_res_partner_category(self):
        """ migration des categories de partenaire """
        self.tables_processed.append('res.partner.category')
        if self.company_id != 1:
            return
        partner_category_ids = self.connectionsource.search('res.partner.category', [], 0, 2000)
        for partner_category_id in partner_category_ids:
            self.__create_partner_categ(partner_category_id)

    def __create_prod_categ(self, product_category_id):
        """ creation des categories produits """
        res = None
        vals = self.get_values(product_category_id, 'product.category')

        if 'child_id' in vals:
            vals.pop('child_id')
        if 'company_id' in vals:
            vals.pop('company_id')

        categ = self.connectioncible.search('product.category', [('name', '=', vals['name'])], 0, 2)

        if not categ:
            try:
                res = self.connectioncible.create('product.category', vals)
            except BaseException, erreur_base:
                self.__affiche__erreur(erreur_base, 0, vals, "__migre_product_category")

        else:
            try:
                self.connectioncible.write('product.category', categ[0], vals)
                res = categ[0]
            except BaseException, erreur_base:
                self.__affiche__erreur(erreur_base, 0, vals, "__migre_product_category write")
        return res

    def __migre_product_category(self):
        """ migration categorie produit """
        if self.company_id != 1:
            return
        self.tables_processed.append('product.category')

        product_template_ids = self.connectioncible.search('product.template', [], 0, 1000000, 'id asc')
        self.connectioncible.unlink('product.template', product_template_ids)
        product_ids = self.connectioncible.search('product.product', [], 0, 1000000, 'id asc')
        self.connectioncible.unlink('product.product', product_ids)
        cible_product_category_ids = self.connectioncible.search('product.category', [], 0, 1000000, 'id asc')
        self.connectioncible.unlink('product.category', cible_product_category_ids)
        product_category_ids = self.connectionsource.search('product.category', [], 0, 1000000, 'id asc')
        for product_category_id in product_category_ids:
            new_product_category_id = self.__create_prod_categ(product_category_id)
            self.add_old_id(product_category_id, new_product_category_id, 'product.category')

    def __migre_hr_timesheet_invoice_factor(self):

        if self.company_id != 1:
            print "self.company_id ", self.company_id
            return

        hr_timesheet_invoice_factor_ids = self.connectionsource.search('hr_timesheet_invoice.factor', [], 0, 2000)
        self.tables_processed.append('hr_timesheet_invoice.factor')
        for old_hr_timesheet_invoice_factor_id in hr_timesheet_invoice_factor_ids:
            vals = self.get_values(old_hr_timesheet_invoice_factor_id, 'hr_timesheet_invoice.factor',
                                   ['customer_name', 'name', 'factor'])
            hr_timesheet_invoice_factor_id = False
            if 'customer_name' in vals:
                hr_timesheet_invoice_factor_id = self.connectioncible.search('hr_timesheet_invoice.factor', [
                    ('customer_name', '=', vals['customer_name'])], 0, 1)
            elif 'name' in vals:
                hr_timesheet_invoice_factor_id = self.connectioncible.search('hr_timesheet_invoice.factor',
                                                                             [('name', '=', vals['name'])], 0, 1)
            if not hr_timesheet_invoice_factor_id:
                hr_timesheet_invoice_factor_id = self.connectioncible.search('hr_timesheet_invoice.factor',
                                                                             [('name', '=', vals['name'])], 0, 1)
                if not hr_timesheet_invoice_factor_id:
                    hr_timesheet_invoice_factor_id = self.connectioncible.create('hr_timesheet_invoice.factor', vals)

            self.connectioncible.write('hr_timesheet_invoice.factor', hr_timesheet_invoice_factor_id, vals)
            self.add_old_id(old_hr_timesheet_invoice_factor_id, hr_timesheet_invoice_factor_id,
                            'hr_timesheet_invoice.factor')

        return True

    def __migre_product_product(self):
        """ Migration produit et modele de produit"""
        self.tables_processed.append('product.template')

        product_template_ids = self.connectionsource.search('product.template', [], 0, 1000000, 'id asc')
        for product_template_id in product_template_ids:
            vals = self.get_values(product_template_id, 'product.template',
                                   ['warranty', 'property_stock_procurement', 'supply_method', 'code', 'list_price',
                                    'weight', 'track_production', 'incoming_qty', 'standard_price', 'uod_id', 'uom_id',
                                    'default_code', 'property_account_income', 'qty_available', 'uos_coeff',
                                    'partner_ref', 'virtual_available', 'purchase_ok', 'track_outgoing', 'company_id',
                                    'product_tmpl_id', 'uom_po_id', 'type', 'price', 'track_incoming',
                                    'property_stock_production', 'volume', 'outgoing_qty', 'procure_method',
                                    'property_stock_inventory', 'cost_method', 'price_extra', 'active', 'sale_ok',
                                    'weight_net', 'sale_delay', 'name', 'property_stock_account_output',
                                    'property_account_expense', 'categ_id', 'property_stock_account_input', 'lst_price',
                                    'price_margin'])
            vals['company_id'] = self.company_id

            if 'seller_ids' in vals:
                vals.pop('seller_ids')

            if 'uom_id' in vals:
                vals['uom_po_id'] = vals['uom_id']
                vals['uos_id'] = vals['uom_id']
            try:
                new_product_template_id = self.connectioncible.create('product.template', vals)
                self.add_old_id(product_template_id, new_product_template_id, 'product.template')
            except BaseException, erreur_base:
                self.__affiche__erreur(erreur_base, product_template_id, vals, "create migre_product_template")

        product_product_ids = self.connectionsource.search('product.product', [('active', 'in', ['true', 'false'])], 0,
                                                           1000000, 'id asc')
        self.tables_processed.append('product.product')
        for product_product_id in product_product_ids:
            vals = self.get_values(product_product_id, 'product.product')
            vals['company_id'] = self.company_id

            for val in ('packaging', 'pricelist_sale', 'pricelist_purchase', 'user_id', 'seller_ids'):
                if val in vals:
                    vals.pop(val)
            if 'uom_id' in vals:
                vals['uom_po_id'] = vals['uom_id']
                vals['uos_id'] = vals['uom_id']
            try:
                new_product_product_id = self.connectioncible.create('product.product', vals)
                self.add_old_id(product_product_id, new_product_product_id, 'product.product')

            except BaseException, erreur_base:
                self.__affiche__erreur(erreur_base, product_product_id, vals, 'create migre product_product')

    def __valid_invoice(self, invoice_id=None):
        """ validation facture """
        if not invoice_id:
            account_invoice_ids = self.connectionsource.search('account.invoice', [('state', '!=', 'draft')], 0,
                                                               1000000, 'id asc')
        else:
            account_invoice_ids = [invoice_id]
        for account_invoice_id in account_invoice_ids:
            nbr_lines = len(self.connectioncible.search('account.invoice.line', [('company_id', '=', self.company_id), (
                'invoice_id', '=', account_invoice_id)]))
            invoice_read = self.connectioncible.read('account.invoice', account_invoice_id)
            if invoice_read:
                self.connectioncible.execute('account.invoice', 'button_reset_taxes', [account_invoice_id])
                if nbr_lines > 0 and invoice_read['state'] == 'draft' and invoice_read['internal_number'] is not False:
                    try:
                        self.connectioncible.object.exec_workflow(self.connectioncible.dbname, self.connectioncible.uid,
                                                                  self.connectioncible.pwd, 'account.invoice',
                                                                  'invoice_open', account_invoice_id)
                    except BaseException, erreur_base:
                        self.__affiche__erreur(erreur_base, account_invoice_id, invoice_read)
            else:
                print "la facture n'existe pas %s ", account_invoice_id

    def connect_pg(self):
        self.cibleconnectstr = "host=%s user=%s password=%s dbname=%s port=%s" % (
            self.options.dbhostc, self.options.userdbc, self.options.passdbc, self.options.dbc, self.options.dbportc)
        self.conn = psycopg2.connect(self.cibleconnectstr)
        self.curseur = self.conn.cursor()
        print
        print "Cible ", self.cibleconnectstr
        self.sourceconnectstr = "host=%s user=%s password=%s dbname=%s port=%s" % (
            self.options.dbhosts, self.options.userdbs, self.options.passdbs, self.options.dbs, self.options.dbports)
        print "Source ", self.sourceconnectstr
        self.conn_source = psycopg2.connect(self.sourceconnectstr)
        self.curseur_source = self.conn_source.cursor()

    def deconnect_pg(self):
        self.curseur.close()
        self.conn.close()
        self.curseur_source.close()
        self.conn_source.close()

    def clean_source_db(self):
        self.curseur_source.execute(
         'update account_move_line set currency_id = Null where id in '
         '(select id  from account_move_line where currency_id = (select currency_id from res_company where id =1));')
        self.curseur_source.execute(
        "update account_account set currency_id = Null where id in "
        "(select id  from account_account where currency_id = (select currency_id from res_company where id =1));")
        self.curseur_source.execute(
        "update account_move_line set currency_id = Null where id in "
        "(select id  from account_move_line where currency_id = (select currency_id from res_company where id =1));")
        self.curseur_source.execute("update account_invoice_line set name = '  ' where name = '' or name is null ;")
        self.curseur_source.execute(
        "delete from account_account_type where id in "
        "(select id from account_account_type where id not in (select user_type from account_account));")
        self.curseur_source.execute(
        "update account_account_type set close_method = 'unreconciled' where id in"
        " (select user_type from account_account where type in ('payable','receivable'));")
        self.curseur_source.execute(
        "delete from account_move_line where move_id in"
        " (select id from (select id,(select sum(debit)+sum(credit) from account_move_line"
        " where move_id = account_move.id) as  total  "
        "from account_move) as compte_ligne  where total is null or total =0);")
        self.curseur_source.execute(
        "delete from account_move where id in (select id from (select id,(select sum(debit)+sum(credit) "
        "from account_move_line where move_id = account_move.id) as  total  from account_move) "
        "as compte_ligne  where total is null or total =0);")
        self.conn_source.commit()

    def controle_data(self):
        #TODO: supression currency_id Bonne idee ????
        self.curseur_source.execute("update account_account set currency_id = null")
        self.conn_source.commit()
        self.curseur_source.execute(
            'select id,move_id ,name,debit,credit, amount_currency from account_move_line '
            'where (credit <>0 and amount_currency > 0) or (debit <>0 and amount_currency < 0);')
        res = self.curseur_source.fetchall()
        if res:
            for l in res:
                print "Erreur de coherence debit, credit , montant devise ", l
        self.curseur_source.execute(
            'select id,(balance_start + total_line - balance_end_real), state from '
            '(select id,balance_start, balance_end_real, state , (select sum(amount)'
            ' from account_bank_statement_line where statement_id = st.id) as total_line '
            'from account_bank_statement st) as total')
        res = self.curseur_source.fetchall()
        for line in res:
            if line[1] > 0.01:
                print "Erreur de %s sur ecriture de banque %s state %s : " % (line[1], line[0], line[2])

    def parent_store_compute(self):
        """ recalcule l'arbre des comptes """

        _table = 'account_account'
        _parent_name = 'parent_id'
        _parent_order = 'code'

        def browse_rec(root, pos=0):
            """ cherche parent """
            where = _parent_name + '=' + str(root)
            if not root:
                where = _parent_name + ' IS NULL'
            if _parent_order:
                where += ' order by ' + _parent_order
            self.curseur.execute('SELECT id FROM ' + _table + ' WHERE active = True and ' + where)
            pos2 = pos + 1
            childs = self.curseur.fetchall()
            for child_id in childs:
                pos2 = browse_rec(child_id[0], pos2)
            self.curseur.execute('update ' + _table + ' set parent_left=%s, parent_right=%s where id=%s',
                                 (pos, pos2, root))
            self.conn.commit()
            return pos2 + 1

        query = 'SELECT id FROM ' + _table + ' WHERE active = True and ' + _parent_name + ' IS NULL'
        if _parent_order:
            query += ' order by ' + _parent_order
        pos = 0
        self.curseur.execute(query)
        for (root,) in self.curseur.fetchall():
            pos = browse_rec(root, pos)

        return True

    def controle_tables(self):
        self.tables_processed.sort()
        for table in self.tables_processed:
            if table in ('account.bank.statement', 'account.move.line', 'account.move'):
                source_table_ids = self.connectionsource.search(table,
                                                                [('period_id', 'in', self.current_account_period_ids)])
            elif table == 'account.invoice':
                source_table_ids = self.connectionsource.search('account.invoice', [('state', '!=', 'cancel'), (
                    'period_id', 'in', self.current_account_period_ids)], 0, 1000000, 'id asc')
            elif table == 'account.analytic.line':
                source_table_ids = self.connectionsource.search('account.analytic.line', [], 0, 200000, 'id desc')
            elif table in 'account.invoice.line':
                requete = "select * from account_invoice_line where invoice_id in (select id from account_invoice where period_id in %s and state != 'cancel')" % str(
                    tuple(self.current_account_period_ids))
                self.curseur_source.execute(requete)
                source_table_ids = self.curseur_source.fetchall()

            elif table in 'account.bank.statement.line':
                requete = "select * from account_bank_statement_line where statement_id in" \
                          " (select id from account_bank_statement where period_id in %s)" % \
                          str(tuple(self.current_account_period_ids))
                self.curseur_source.execute(requete)
                source_table_ids = self.curseur_source.fetchall()
            else:
                if table in FIELDS_TAB.keys() and ('active' in FIELDS_TAB_SOURCE[table]):
                    source_table_ids = self.connectionsource.search(table, [('active', 'in', ['true', 'false'])], 0,
                                                                    300000)
                else:
                    source_table_ids = self.connectionsource.search(table, [], 0, 300000)
            try:
                if table in FIELDS_TAB.keys() and 'active' in FIELDS_TAB[table]:
                    cible_table_ids = self.connectioncible.search(table, [('active', 'in', ['true', 'false'])], 0,
                                                                  300000)
                else:
                    cible_table_ids = self.connectioncible.search(table, [], 0, 300000)
            except:
                cible_table_ids = []
                print "table not exist ", table
                pass

            if not len(source_table_ids) == len(cible_table_ids):
                print "\033[1m\033[31mtable %s cible %s source %s  ERR ! \033[0m" % (
                    table, len(cible_table_ids), len(source_table_ids))
            else:
                print 'table %s cible  source %s  OK !' % (table, len(source_table_ids))

        print "Controle Move "
        somme_cible = self.sum_account_move_cible()
        somme_source = self.sum_account_move_source(self.move_already_process)
        if (abs(somme_cible['debit'] - somme_source['debit']) > 0.1) or (
                    abs(somme_cible['credit'] - somme_source['credit']) > 0.1):
            print "cible debit %s , credit %s " % (somme_cible['debit'], somme_cible['credit'])
            print "source debit %s , credit %s " % (somme_source['debit'], somme_source['credit'])
            print "ecart"
            print "already_process ", self.move_already_process

            print "abs(somme_cible['debit'] -somme_source['debit']) ", abs(somme_cible['debit'] - somme_source['debit'])
            print "abs(somme_cible['credit'] - somme_source['credit']) ", abs(
                somme_cible['credit'] - somme_source['credit'])
            if len(self.move_already_process) == 1:
                source_ids = self.connectionsource.search('account.move.line',
                                                          [('move_id', '=', self.move_already_process[0])], 0, LIMITE,
                                                          'id asc')
            else:
                str_ids = str(tuple(self.move_already_process))
                source_ids = self.connectionsource.search('account.move.line', [('move_id', 'in', str_ids)], 0, LIMITE,
                                                          'id asc')

            cible_ids = self.connectioncible.search('account.move.line', [], 0, LIMITE, 'id asc')
            print "source_ids ", source_ids
            print "Source "

        query = """ SELECT
            (SELECT ROUND(SUM(amount_invoice)::numeric,2)  FROM dblink('""" + self.sourceconnectstr + """',
            'SELECT amount_total from account_invoice') AS t2(amount_invoice float))  AS v7_invoice ,
            (SELECT ROUND(SUM(amount_total)::numeric,2) from account_invoice where  period_id IN
            (SELECT id from account_period WHERE fiscalyear_id = 11) and state != 'cancel') AS v5_invoice,
            (SELECT ROUND(SUM(amount)::numeric,2)  FROM dblink('""" + self.sourceconnectstr + """',
             'SELECT ail.name, price_subtotal::float, amount_total from account_invoice_line ail, account_invoice ai
              WHERE ail.invoice_id = ai.id')	AS t1(number varchar, amount float, amount_invoice float))
              AS v7_invoice_line ,
            (SELECT ROUND(SUM(ail.price_subtotal ::float)::numeric,2) AS v5 from account_invoice_line ail,
            account_invoice ai
            WHERE ail.invoice_id = ai.id and ai.period_id IN (SELECT id from account_period WHERE fiscalyear_id = 11)
             and state != 'cancel') AS v5_invoice_line,
            (SELECT ROUND(SUM(amount)::numeric,2) FROM dblink('""" + self.sourceconnectstr + """', 'SELECT amount from
            account_analytic_line') AS t2(amount float))  AS v7_analytic_line ,
            (SELECT ROUND(SUM(amount)::numeric,2) FROM account_analytic_line WHERE date >='01/01/2012')
            AS v5_analytic_line,
            (SELECT ROUND(SUM(debit)::numeric,2)  FROM dblink('""" + self.sourceconnectstr + """',
            'SELECT debit from account_move_line') AS t2(debit float))  AS v7_debit_move_line ,
            (SELECT ROUND(SUM(debit)::numeric,2)  FROM account_move_line WHERE move_id IN (select id
            from account_move WHERE period_id IN (select id from account_period WHERE fiscalyear_id = 11)))
             AS v5_debit_move_line ,
            (SELECT ROUND(SUM(credit)::numeric,2) FROM dblink('""" + self.sourceconnectstr + """',
            'SELECT credit from account_move_line') AS t2(credit float))  AS v7_credit_move_line  ,
            (SELECT ROUND(SUM(credit)::numeric,2) FROM account_move_line WHERE move_id IN
            (select id from account_move WHERE period_id IN (select id from account_period WHERE fiscalyear_id = 11)))
             AS v5_credit_move_line ,
            (SELECT ROUND((SUM(balance_end_real) - SUM(balance_start))::numeric,2)
            FROM dblink('""" + self.sourceconnectstr + """', 'SELECT balance_end_real, balance_start
            from account_bank_statement') AS t2(balance_end_real  float, balance_start float))
             AS v7_account_bank_statement ,
            (select ROUND((SUM(balance_end_real) - SUM(balance_start))::numeric,2)
            FROM account_bank_statement WHERE period_id IN (select id from account_period WHERE fiscalyear_id = 11)) AS
             v5_account_bank_statement,
            (SELECT ROUND(SUM(amount)::numeric,2) FROM dblink('""" + self.sourceconnectstr + """',
            'SELECT amount from account_bank_statement_line') AS t2(amount float))  AS v7_account_bank_statement_line ,
            (SELECT ROUND(SUM(amount)::numeric,2) FROM account_bank_statement_line WHERE statement_id
            IN (select id from account_bank_statement WHERE period_id IN (select id from account_period
             WHERE fiscalyear_id = 11)))	AS v5_account_bank_statement_line   """
        self.curseur_source.execute(query)
        res = self.curseur_source.fetchall()
        resultat = {'invoice': {'V7': res[0][0], 'V5': res[0][1]}, 'invoice_line': {'V7': res[0][2], 'V5': res[0][3]},
                    'analytic_line': {'V7': res[0][4], 'V5': res[0][5]},
                    'debit_move_line': {'V7': res[0][6], 'V5': res[0][7]},
                    'credit_move_line': {'V7': res[0][8], 'V5': res[0][9]},
                    'account_bank_statement': {'V7': res[0][10], 'V5': res[0][11]},
                    'account_bank_statement_line': {'V7': res[0][12], 'V5': res[0][13]}}

        cle = resultat.keys()
        cle.sort()
        for k in cle:
            if resultat[k]['V7'] and resultat[k]['V5']:
                print "%s V7 %10s V5 %10s ecart %10s " % (
                    k.ljust(30), resultat[k]['V7'], resultat[k]['V5'], resultat[k]['V7'] - resultat[k]['V5'])

    def migre_base_data(self):
        """ migration des donnees de base """
        #self.__add_old_id()
        print "migre Sequence"
        self.global_start = datetime.now()
        self.__migre_ir_sequence_type()
        self.__migre_ir_sequence()
        print datetime.now() - self.global_start
        print 'Migre res country'
        self.__migre_res_country()
        print datetime.now() - self.global_start
        print 'Migre res country State'
        self.__migre_res_country_state()
        print datetime.now() - self.global_start
        print 'Migre res users'
        self.__migre_res_users()
        print datetime.now() - self.global_start
        print 'Migre res currency'
        self.__migre_res_currency()
        print datetime.now() - self.global_start
        self.__migre_res_currency_rate()
        print datetime.now() - self.global_start
        print 'Migre res company'
        self.migre_res_company()

        self.company_currency = \
            self.connectioncible.read('res.company', self.company_id, ['currency_id'])['currency_id'][0]
        self.company_currency_source = \
            self.connectionsource.read('res.company', self.company_id, ['currency_id'])['currency_id'][0]
        print datetime.now() - self.global_start
        print 'Migre account type'
        self.__migre_account_type()
        print datetime.now() - self.global_start
        print "Migre account"
        self.__migre_account_account()
        print datetime.now() - self.global_start
        print "migre Analytique journal"
        self.__migre_acc_analyt_journal()
        print datetime.now() - self.global_start
        print "migre journal"
        self.__migre_account_journal()
        print datetime.now() - self.global_start
        print "parent store compute"
        self.parent_store_compute()
        print datetime.now() - self.global_start
        print "migre account tax code"
        self.__migre_acc_tax_code()
        print datetime.now() - self.global_start
        print "migre account tax"
        self.__migre_account_tax()
        print datetime.now() - self.global_start
        print "migre res bank"
        self.__migre_res_bank()
        print datetime.now() - self.global_start
        if self.hr:
            print "Migre timeshet factor"
            self.__migre_hr_timesheet_invoice_factor()
            print datetime.now() - self.global_start
        print "Fin Migre base ", (datetime.now() - self.global_start)
        #except (RuntimeError, TypeError, NameError):
        #    pass
        #else:
        #    pass

    def migre_product(self):
        """ Migration des donnees produits """
        print "migre product category"
        self.__migre_product_category()
        print "migre product uom category"
        self.__migre_prod_uom_categ()
        print "migre product uom"
        self.__migre_product_uom()
        print "migre product"
        self.__migre_product_product()
        print "Fin migre product ", (datetime.now() - self.global_start)

    def migre_partner(self):
        """ migration des donnees partenaire """
        print "Migre categorie partenaire"
        self.__migre_res_partner_category()
        print "Migre partner title"
        self.__migre_res_partner_title()
        print "Migre bank type"
        self.__migre_res_part_bnk_type()
        print "Migre product_list"
        self.migre_product_price_list()
        print "Migre partenaire"
        self.__migre_res_partner()
        print "Migre partenaire banque"
        self.__migre_res_partner_bank()
        print "Migre adresse partenaire"
        self.__migre_res_part_addr()
        print "Fin Migre partner ", (datetime.now() - self.global_start)

    def migre_compta(self):
        """ migration des donnees comptables """
        self.__migre_acc_fiscal_year()
        print "Migre Analytic Account"
        self.__migre_acc_analytic_acc()
        if self.options.lines:
            print "Migre bank statement"
            self.__migre_account_bank_statement()
            if self.reconciliation:
                print "Migre reconcile"
                self.migre_reconcile()
            print "Migre Account_move"
            self.__migre_account_move()
            print "Migre Analytic line"
            self.__migre_acc_analytic_line()
            print "Migre Account_invoice"
            self.__migre_account_invoice()
            self.controle_analytique()
        res = self.connectioncible.search('ir.actions.todo', [])
        res = self.connectioncible.write('ir.actions.todo', res, {'state': 'done'})
        #self.close_period()
        print "self.passwordsource ", self.passwordsource
        self.curseur.execute(
            "update  res_users set password = '%s' , password_crypt='' where id = 1 " % self.passwordsource)
        self.conn.commit()
        print "Fin Migre compta ", (datetime.now() - self.global_start)
