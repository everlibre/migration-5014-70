#!/usr/bin/python
# -*- encoding: utf-8 -*-
''' Module de migration openerp '''

import datetime
import psycopg2
import sys

FIELDS_TAB = {}
LIMITE = 100000000
recursion_level = 0
sourcecible = ""




def utf(vals):
    ''' converti une valeur en unicode'''
    if type(vals) == type(True):    
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
    ''' Librairie de migration openerp v5.014 -> V7.0 '''
    connectionsource = None
    connectioncible = None
    company_id = None
    newtab = {}
    options = None
    pass_admin_new_base = None
    current_account_period_ids = []
    company_currency = None
    def __init__(self, connectionsource, connectioncible, company_id, options):
        if not connectionsource or not connectioncible:
            raise 
        self.connectionsource = connectionsource
        self.connectioncible = connectioncible
        self.company_id = company_id
        self.options = options

    def __affiche__erreur(self,erreur , sid, vals, function=None):
        global sourcecible
        ''' fonction d'affichage des erreurs '''
        fichier = open(sourcecible+".err", "a")
        print sourcecible+".err"
        try:
            print "id d'origine ",sid
            for key in vals.keys():
                print "vals %s : %s " % (utf(key), utf(vals[key])),
                fichier.write("vals %s : %s \r\n" % (utf(key), utf(vals[key])))
            print
            if hasattr(erreur, 'faultCode'):
                print erreur.faultCode
                fichier.write(erreur.faultCode+"\r\n")
            if hasattr(erreur, 'faultString'):
                print erreur.faultString
                fichier.write(erreur.faultString+"\r\n")
            if hasattr(erreur, 'message'):
                print erreur.message
                fichier.write(erreur.message+"\r\n")
            if function:
                print function
                fichier.write(function+"\r\n"+"\r\n")
        except BaseException, erreur:
            if hasattr(erreur, 'faultCode'):   
                 print erreur.faultCode         
                 fichier.write(erreur.faultCode+"\r\n")
            if hasattr(erreur, 'faultString'): 
                 print erreur.faultString       
                 fichier.write(erreur.faultString+"\r\n")
            if hasattr(erreur, 'message'):     
                 print erreur.message           
                 fichier.write(erreur.message+"\r\n")
            print "SID ", sid
            fichier.write("SID "+str(sid)+"\r\n")
            print "Vals ", vals
            fichier.write("Vals "+str(vals)+"\r\n")
              
        fichier.close()
        sys.exit()
        
        return True
    
    def migre_base_data(self):
        ''' migration des donnees de base '''
        global sourcecible
        sourcecible = self.connectioncible.dbname
        print "Source cible ", sourcecible
        self.__add_old_id()
        self.load_fields()
        try:
            self.__migre_hr_timesheet_invoice_factor()
        except:
            pass
        print "debut migration donnees de base %s" % self.connectionsource.dbname
        
        print "migre Sequence"
        self.__migre_ir_sequence_type()
        self.__migre_ir_sequence()
        print 'Migre res country'
        self.__migre_res_country()
        print 'Migre res country State'
        self.__migre_res_country_state()
        print 'Migre res groups'
        self.__migre_res_groups()
        print 'Migre res users' 
        self.__migre_res_users()
        print 'Migre res currency'
        self.__migre_res_currency()
        self.migre_res_company()
        self.company_currency =  self.connectioncible.read('res.company', self.company_id, ['currency_id'])['currency_id'][0]
        print 'Migre account type'
        self.__migre_account_type()
        print "Migre account"
        self.__migre_account_account()
        print "migre journal"
        self.__migre_account_journal()

        print "parent store compute"
        self.parent_store_compute()
        print "migre account tax code"
        self.__migre_acc_tax_code()
        print "migre account tax"
        self.__migre_account_tax()
        print "migre res bank"
        self.__migre_res_bank()
        print "Fin migration donnees de base %s" % self.connectionsource.dbname
    


    def migre_product(self):
        ''' Migration des donnees produits '''
        print "debut migration product base %s" % self.connectionsource.dbname
        print "migre journal view "
        #self.__migre_account_journal_view
        
        print "migre product category"
        self.__migre_product_category()
        print "migre product uom category"
        self.__migre_prod_uom_categ()
        print "migre product uom"
        self.__migre_product_uom()
        print "migre product"
        self.__migre_product_product()
        print "fin migration product base %s" % self.connectionsource.dbname


    def migre_partner(self):
        ''' migration des donnees partenaire '''
        print "debut migration partenaire base %s" % self.connectionsource.dbname
        print "Migre categorie"
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
        print "Fin migration partenaire base %s" % self.connectionsource.dbname

    def migre_compta(self):
        ''' migration des donnees comptables '''
        print "debut migration compta %s" % self.connectionsource.dbname
        #self.__migre_account_account()
        print "migre Analytique journal"
        self.__migre_acc_analyt_journal()

        print "migre annee fiscale"
        self.__migre_acc_fiscal_year()
        print "migre compte analytique"
        self.__migre_acc_analytic_acc()
        print "migre etat comptable"
        self.__migre_account_bank_statement()
        print "migre ecriture"
        self.__migre_account_move()
        print "migre facture"
        self.__migre_account_invoice()
        print "Fin migration compta %s" % self.connectionsource.dbname
            


    def __add_old_id(self):
        ''' fonction ajoutant un champ contenant l'id de l'objet dans le connection source '''
        model_ids = self.connectioncible.search('ir.model', [('model', 'like', 'hr%')], 0, 500000)
        for model_id in model_ids:
            search_old = self.connectioncible.search('ir.model.fields', [('model_id', '=', model_id), ('name', '=', 'x_old_id')])
            if not search_old:
                try:
                    self.connectioncible.create('ir.model.fields', {'model_id':model_id, 'name':'x_old_id', 'field_description':'Ancien ID', 'ttype':'integer', 'state':'manual', 'size':64})
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, model_id, {}, 'add old  id')
        model_ids = self.connectioncible.search('ir.model', [('model', 'like', 'product%')], 0, 500000)
        for model_id in model_ids:
            search_old = self.connectioncible.search('ir.model.fields', [('model_id', '=', model_id), ('name', '=', 'x_old_id')])
            if not search_old:
                try:
                    self.connectioncible.create('ir.model.fields', {'model_id':model_id, 'name':'x_old_id', 'field_description':'Ancien ID', 'ttype':'integer', 'state':'manual', 'size':64})
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, model_id, {}, 'add old  id')

        model_ids = self.connectioncible.search('ir.model', [('model', 'like', 'ir.sequence%')], 0, 500000)
        for model_id in model_ids:
            search_old = self.connectioncible.search('ir.model.fields', [('model_id', '=', model_id), ('name', '=', 'x_old_id')])
            if not search_old:
                try:
                    self.connectioncible.create('ir.model.fields', {'model_id':model_id, 'name':'x_old_id', 'field_description':'Ancien ID', 'ttype':'integer', 'state':'manual', 'size':64})
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, model_id, {}, 'add old ps     id')
        model_ids = self.connectioncible.search('ir.model', [('model', 'like', 'account%')], 0, 500000)
        for model_id in model_ids:
            search_old = self.connectioncible.search('ir.model.fields', [('model_id', '=', model_id), ('name', '=', 'x_old_id')])
            if not search_old:
                try:
                    self.connectioncible.create('ir.model.fields', {'model_id':model_id, 'name':'x_old_id', 'field_description':'Ancien ID', 'ttype':'integer', 'state':'manual', 'size':64})
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, model_id, {}, 'add old ps     id')
                
        model_ids = self.connectioncible.search('ir.model', [('model', 'like', 'res%')], 0, 500000)
        for model_id in model_ids:
            search_old = self.connectioncible.search('ir.model.fields', [('model_id', '=', model_id), ('name', '=', 'x_old_id')])
            if not search_old:
                try :
                    self.connectioncible.create('ir.model.fields', {'model_id':model_id, 'name':'x_old_id', 'field_description':'Ancien ID', 'ttype':'integer', 'state':'manual', 'size':64})
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, model_id, {}, 'add old  id')
        
        
        model_ids = self.connectioncible.search('ir.model', [('model', 'like', 'stock.location%')], 0, 500000)
        for model_id in model_ids:
            search_old = self.connectioncible.search('ir.model.fields', [('model_id', '=', model_id), ('name', '=', 'x_old_id')])
            if not search_old:
                try :
                    self.connectioncible.create('ir.model.fields', {'model_id':model_id, 'name':'x_old_id', 'field_description':'Ancien ID', 'ttype':'integer', 'state':'manual', 'size':64})
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, model_id, {}, 'add old  id')
                    
        model_ids = self.connectioncible.search('ir.model', [('model', 'like', 'res.partner')], 0, 500000)
        for model_id in model_ids:
            search_old = self.connectioncible.search('ir.model.fields', [('model_id', '=', model_id), ('name', '=', 'x_old_address_id')])
            if not search_old:
                try :
                    self.connectioncible.create('ir.model.fields', {'model_id':model_id, 'name':'x_old_address_id', 'field_description':'Ancienne address ID', 'ttype':'integer', 'state':'manual', 'size':64})
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, model_id, {}, 'add old address id')

        
    def load_fields(self):
        ''' charge la definition des champs de tout les modeles ''' 
        model_ids  = self.connectioncible.search('ir.model', [], 0, 500000, 'model asc')
        for model_id in model_ids:
            model  = self.connectioncible.read('ir.model', model_id)['model']
            try:
                FIELDS_TAB[model] = self.connectioncible.exec_act(model, 'fields_get')
                
            except BaseException, erreur:
                print "Load Fields ",erreur.faultString
                sys.exit()



    def __new(self, sid, model):
        ''' recherche l'id dans la nouvelle base '''
        if (model == 'res.users') and (sid == 1): # user == root dans source -> user = admin dans cible
            return 3
        if self.newtab.has_key(model) and self.newtab[model].has_key(sid):
            return self.newtab[model][sid]
        fields = FIELDS_TAB[model] 
        if not 'x_old_id' in fields.keys():
            print "Model sans old id ", model
            raise "error"
        res = self.connectioncible.search(model, [('x_old_id', '=', sid)])  
        if not res:
            try:
                if 'active' in fields:
                    if 'company_id' in fields:
                        res = self.connectioncible.search(model, [\
                    ('x_old_id', '=', sid), ('company_id', '=',\
                    self.company_id), ('active', 'in', ['true', 'false'])])
                    else:
                        res = self.connectioncible.search(model, [\
                    ('x_old_id', '=', sid), \
                    ('active', 'in', ['true', 'false'])])
                    
                else:
                    if 'company_id' in fields:
                        res = self.connectioncible.search(model, [\
                        ('x_old_id', '=', sid), \
                    ('company_id', '=', self.company_id)])
                    else:
                        res = self.connectioncible.search(model,\
                    [('x_old_id', '=', sid)])
            except BaseException, erreur_base :
                self.__affiche__erreur(erreur_base, 0, {}, '__new')

        if res:
            res = res[0]
            if not self.newtab.has_key(model):
                self.newtab[model] = {}
            self.newtab[model][sid] = res
        else:
            res = None
            data = self.get_values(sid, model)
            data['x_old_id'] = sid
            try :
                res = self.connectioncible.create(model, data)
            except BaseException, erreur_base :
                print
                print "Data pour %s : %s " % (model, data)
                print "Old not exist ", self.company_id, model,data,  sid
                #print dir(erreur_base)
                print "Erreur " , erreur_base.faultCode,erreur_base.faultString,erreur_base.message
                print
                sys.exit()
                return sid
        return res

    def get_values(self, record_id, model, champs=None):
        ''' renvoie les valeurs de type model sur la base source ''' 
        global recursion_level
        res = {}
        if model == 'account.account':
            champs = ['code', 'reconcile', 'user_type', 'currency_id', 'company_id', 'shortcut', 'note', 'parent_id', 'type', 'active', 'company_currency_id', 'name', 'currency_mode']
        elif model == 'res.users':
            champs = ['login','name','password', 'user_email', 'context_lang', 'group_ids']
        elif model == 'account.journal':
            champs = ['view_id','default_debit_account_id',  'update_posted', 'code', 'name', 'centralisation',  'group_invoice_lines', 'type_control_ids', 'company_id', 'currency', 'sequence_id', 'account_control_ids', 'refund_journal', 'invoice_sequence_id', 'active', 'analytic_journal_id', 'entry_posted', 'type', 'default_credit_account_id']
        elif model == 'account.journal.column':
            champs = ['name', 'sequence', 'required', 'field', 'readonly']
        elif model == "product.pricelist":
            champs = ['active', 'currency_id', 'type', 'name']
        elif model == "product.pricelist.version":
            champs = [ 'name', 'date_end', 'date_start', 'active', 'pricelist_id']
        elif model == "account.payment.term":
            champs = ['active', 'note', 'name' ]
        elif model == "account.move.line":
            champs = ['debit', 'credit', 'statement_id', 'currency_id', 'date_maturity', 'partner_id', 'blocked', 'analytic_account_id', 'centralisation', 'journal_id', 'tax_code_id', 'state', 'amount_taxed', 'ref', 'origin_link', 'account_id', 'period_id', 'amount_currency', 'date', 'move_id', 'name', 'tax_amount', 'product_id', 'account_tax_id', 'product_uom_id', 'followup_line_id', 'quantity']
        elif model == "account.analytic.account":
            champs = ['code', 'quantity_max', 'contact_id',\
         'company_currency_id', 'date', 'crossovered_budget_line',\
         'amount_max', 'partner_id','to_invoice', 'date_start',\
         'company_id', 'parent_id', 'state', 'complete_name', 'debit',\
         'pricelist_id', 'type', 'description', 'amount_invoiced', \
          'active', 'name', 'credit',  'balance', 'quantity']
        elif model == 'account.bank.statement':
            champs = ['name', 'currency', 'balance_end', 'balance_start', 'journal_id', 'import_bvr_id',  'state', 'period_id', 'date',  'balance_end_real']
        elif model == 'stock.location':
            champs = ['comment', 'address_id', 'stock_virtual_value', 'allocation_method', 'location_id', 'chained_location_id', 'complete_name', 'usage', 'stock_real_value', 'chained_location_type', 'account_id',  'chained_delay', 'stock_virtual', 'posz', 'posx', 'posy', 'active', 'icon', 'parent_right', 'name', 'chained_auto_packing', 'parent_left', 'stock_real']
        elif model == 'product.category':
            champs = ['name', 'sequence', 'type']
        #elif model == 'res.partner.address':
             
        if FIELDS_TAB.has_key(model):
            fields = FIELDS_TAB[model] 
        else:
            fields ={}
        
        #fields = self.connectionsource.exec_act(model, 'fields_get')
        #print 
        recursion_level = recursion_level + 1
        #print ('\t'*recursion_level)+model,fields.keys()
         
        #print "Recursion level ", recursion_level
        if not champs:
            champs = fields.keys()
        #print champs
        if recursion_level > 6 :
            print ('\t'*recursion_level), "recursion level sup 6"
            print ('\t'*recursion_level), "Model ", model
            print ('\t'*recursion_level), "Champs ", champs
            print ('\t'*recursion_level), "record id", record_id
            print 
            sys.exit()
        values = self.connectionsource.read( model, record_id, champs)      
        #print ('\t'*recursion_level), "Values ",model, record_id, values
        for field in champs:
            #print "Field %s for model %s " % (field, model)
            if fields.has_key(field) and values.has_key(field):
                if fields[field].has_key('function') and fields[field]['function'] != '_fnct_read':
                    #print "Fonction ",field,fields[field]['function']
                    continue
                field_type = fields[field]['type'] 
                if field_type in ('text', 'char', 'selection') :      
                    if values[field]:
                        if unicode(values[field]).strip():
                            try:
                                res[field] = str(values[field])                                                                                                       
                            except BaseException:
                                res[field] = unicode(values[field])          
                elif field_type in ('boolean', 'date', 'datetime', 'reference', 'float', 'integer') :
                    res[field] = values[field]                                                                                                    
                
                elif field_type == 'many2one' :
                    if values[field]:
                        try:
                            res[field] = self.__new(values[field][0], fields[field]['relation'])
                        except RuntimeError, e:
                            print dir(e)
                            
                            print e.message
                            print "RuntimeError ",          fields[field]['relation'] , model                                                                               
                            sys.exit()
                elif field_type == 'one2many'  :
                    continue 
#                    if values[field]:
#                        o2m = []
#                        for o2m_id in values[field]:
#                            if fields[field]['relation'] == model:
#                                o2m.append(self.get_values(o2m_id, fields[field]['relation'],champs))
#                            else:
#                                o2m.append(self.get_values(o2m_id, fields[field]['relation']))
#    
#                        res[field] = o2m
                elif field_type == 'many2many'  :
                    if values[field]:
                        new_val = []
                        for val in values[field]:
                            new_value = self.__new(val, fields[field]['relation'])
                            if new_value not in new_val: # suppress duplicate value
                                new_val.append(new_value)
                        res[field] = [(6, 0, new_val)]
                        #print "many2many", res[field]
                elif field_type == 'binary' :
                    if values[field]:
                        res[field] = values[field]         
                else:
                    print "Erreur __get_values  ", model, field, fields[field]['type'], fields[field]
                    sys.exit()
                
            recursion_level = recursion_level - 1
        return res

    def __bank_get(self, name):
        ''' renvoi l'id de la banque en fonctiob du nom '''
        if name and name.strip():
            bank_id = self.connectioncible.search('res.bank', [('name', 'ilike', name.strip())], context={'lang':'fr_FR'})
            if not bank_id:
                bank_id = None
            else:
                bank_id = bank_id[0]
        else:
            bank_id = None
        return bank_id

    def __migre_hr_timesheet_invoice_factor(self):
        
        if self.company_id != 1:
            return
        hr_timesheet_invoice_factor_ids = self.connectionsource.search('hr.timesheet.invoice.factor', [], 0, 2000)
        for hr_timesheet_invoice_factor_id in hr_timesheet_invoice_factor_ids:
            vals = self.get_values(hr_timesheet_invoice_factor_id, 'hr.timesheet.invoice.factor',['customer_name','name','factor'])
            vals['x_old_id'] = hr_timesheet_invoice_factor_id
            hr_timesheet_invoice_factor_id = self.connectioncible.search('hr.timesheet.invoice.factor', [('customer_name', '=', vals['customer_name'])], 0, 1)
            if not hr_timesheet_invoice_factor_id:
                hr_timesheet_invoice_factor_id = self.connectioncible.search('hr.timesheet.invoice.factor', [('name', '=', vals['name'])], 0, 1)
                if not hr_timesheet_invoice_factor_id:
                    self.connectioncible.create('hr.timesheet.invoice.factor', vals)
            
            self.connectioncible.write('hr.timesheet.invoice.factor', hr_timesheet_invoice_factor_id, vals)
    
    def __migre_res_country(self):
        ''' migre les pays '''
        if self.company_id != 1:
            return
        country_ids = self.connectionsource.search('res.country', [], 0, 2000)
        for country_id in country_ids:
            vals = self.get_values(country_id, 'res.country')
            vals['x_old_id'] = country_id
            country_id = self.connectioncible.search('res.country', [('code', '=', vals['code'])], 0, 1)
            if not country_id:
                country_id = self.connectioncible.search('res.country', [('name', '=', vals['name'])], 0, 1)
                if not country_id:
                    self.connectioncible.create('res.country', vals)
            
            self.connectioncible.write('res.country', country_id, vals)
                
    def __migre_ir_sequence_type(self):
        ''' migre les pays '''
        if self.company_id != 1:
            return
        ir_sequence_ids = self.connectionsource.search('ir.sequence.type', [], 0, 2000)
        for ir_sequence_id in ir_sequence_ids:
            sequence = self.connectionsource.read('ir.sequence.type', ir_sequence_id)
            if sequence:
                sequence_id = self.connectioncible.search("ir.sequence.type", [('code', '=', sequence['code'])])
                if sequence_id:
                    self.connectioncible.write('ir.sequence.type',sequence_id, {'x_old_id':ir_sequence_id })
                else:
                    sequence.pop('id')
                    sequence['x_old_id'] = ir_sequence_id
                    self.connectioncible.create('ir.sequence.type', sequence)

    def __migre_ir_sequence(self):
        ''' migre les pays '''
        if self.company_id != 1:
            return
        ir_sequence_ids = self.connectionsource.search('ir.sequence', [], 0, 2000)
        for ir_sequence_id in ir_sequence_ids:
            sequence = self.connectionsource.read('ir.sequence', ir_sequence_id)
            if sequence:
                sequence_id = self.connectioncible.search("ir.sequence", [('name', '=', sequence['name'])])
                if sequence_id:
                    self.connectioncible.write('ir.sequence',sequence_id, {'x_old_id':ir_sequence_id })
                else:
                    sequence.pop('id')
                    sequence['x_old_id'] = ir_sequence_id
                    seq_type = self.connectioncible.search("ir.sequence.type", [('code', '=', sequence['code'])])
                    if not seq_type:
                        self.connectioncible.create("ir.sequence.type", {'code':sequence['code'], 'name':sequence['code']})
                    self.connectioncible.create('ir.sequence', sequence)
                  
    def __migre_res_country_state(self):
        ''' migration etat''' 
        if self.company_id != 1:
            return
        country_state_ids = self.connectionsource.search('res.country.state', [], 0, 2000)
        for country_state_id in country_state_ids:
            country_state = self.connectionsource.read('res.country.state', country_state_id)
            vals = self.get_values(country_state_id, 'res.country.state')
            vals['x_old_id'] = country_state_id
            country_id = self.connectioncible.search('res.country', [('name', '=', country_state['country_id'][1])], 0, 1)
            if country_id:
                vals['country_id'] = country_id[0]
            state = self.connectioncible.search('res.country.state', [('name', '=', vals['name'])], 0, 2)
            if not state:
                try:
                    self.connectioncible.create('res.country.state', vals)
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, 0, vals, '__migre_res_country_state')

            else:
                try:
                    self.connectioncible.write('res.country.state', state[0], vals)
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, state, vals, '__migre_res_country_state write')

                    
                
    def __get_acc_pay_term(self, term_name=None):
        ''' Migration des termes de payment '''
        account_payment_term_id = self.connectioncible.search('account.payment.term', [('name', '=', term_name), ('active', 'in', ['true', 'false'])], 0, 2000)
        if account_payment_term_id:
            return account_payment_term_id[0]
        else:
            account_payment_term_ids = self.connectionsource.search('account.payment.term', [('name', '=', term_name), ('active', 'in', ['true', 'false'])], 0, 2000)
            for account_payment_term_id in account_payment_term_ids:
                
                vals = self.get_values(account_payment_term_id, 'account.payment.term')
                vals['x_old_id'] = account_payment_term_id
                if vals.has_key('line_ids'):
                    vals.pop('line_ids')
                try:
                    new_account_payment_term_id = self.connectioncible.create('account.payment.term', vals)
                    self.__migre_acc_pay_term_line(account_payment_term_id, new_account_payment_term_id)
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, 0, vals, 'create migre_account_payment_term')

        return new_account_payment_term_id
        
    def __migre_acc_pay_term_line(self, old, new):
        ''' migration des lignes de termes de paiement '''
        if self.company_id != 1:
            return
        account_payment_term_ids = self.connectionsource.search('account.payment.term.line', [('payment_id', '=', old)], 0, 2000)
        for account_payment_term_id in account_payment_term_ids:
            vals = self.get_values(account_payment_term_id, 'account.payment.term.line')
            vals['x_old_id'] = account_payment_term_id
            payment_term = self.connectioncible.search('account.payment.term.line', [('payment_id', '=', new), ('name', '=', vals['name'])], 0, 1)
            if not payment_term:
                try:
                    self.connectioncible.create('account.payment.term.line', vals)
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, 0, vals, 'create migre_account_payment_term.line')

            else:
                try:
                    self.connectioncible.write('account.payment.term.line', payment_term, vals)
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, 0, vals, 'write migre_account_payment_term.line')

                    
    def __migre_res_groups(self):
        ''' migration groupes utilisateurs '''
        groupids = self.connectionsource.search('res.groups', [], 0, 2000)
        for groupid in groupids:
            vals = self.get_values(groupid, 'res.groups',['lang','context_lang','name'])
            vals['x_old_id'] = groupid
            group = self.connectioncible.search('res.groups', [('name', '=', vals['name'])], 0, 1)
            
            for champ in ['users', 'address_id', 'yubi_enable', 'roles_id', 'model_access', 'menu_access']:
                try:
                    vals.pop(champ)
                except BaseException:
                    pass
            if vals.has_key('lang') and vals['lang'] == 'fr_FR':
                vals['lang'] = 'fr_CH'
            if vals.has_key('context_lang') and vals['context_lang'] == 'fr_FR':
                vals['context_lang'] = 'fr_CH'
                
            if not group:
                try:
                    self.connectioncible.create('res.groups', vals)
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, 0, vals, 'create migre_res_grroups')
            else:
                try:
                    self.connectioncible.write('res.groups', group, vals)
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, 0, vals, 'write migre_res_grroups')


    def __migre_res_users(self):
        ''' migration utilisateurs '''
        users_ids = self.connectionsource.search('res.users', [('login', 'not like', '%admin%'), ('login', '!=', 'root'), ('id', '>', 3), ('active', 'in', ['true', 'false'])], 0, 2000)
        try:
            self.connectioncible.write('res.users', [3], {'x_old_id':3})
            self.connectioncible.write('res.users', [1], {'x_old_id':1})
        except BaseException:
            pass
        for user_id in users_ids:
            vals = self.get_values(user_id, 'res.users', ['login','name','password',  'user_email', 'context_lang', 'group_ids'])
            vals['x_old_id'] = user_id
            vals['menu_id'] = 1
            user = self.connectioncible.search('res.users', [('active', 'in', ['true', 'false']), ('login', '=', vals['login'])], 0, 1)
            vals['password'] = self.pass_admin_new_base
            vals['company_ids'] = [self.company_id]
            vals['company_id'] = self.company_id
            vals['company_ids'] = [self.company_id]
            if vals.has_key('user_email'):
                vals['email'] = vals['user_email']
                vals.pop('user_email')
            if vals.has_key('context_lang') and vals['context_lang'] == 'fr_FR':
                vals['context_lang'] = 'fr_CH'

            if not user:
                try:
                    
                    self.connectioncible.create('res.users', vals)
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, 0, vals, 'create __migre_res_users')

            else:
                try:
                    user_val = self.connectioncible.read('res.users', user[0], ['company_ids'])
                    company_ids = user_val['company_ids']
                    company_ids.append(self.company_id)
                    vals['company_ids'] = company_ids
                    self.connectioncible.write('res.users', user, vals)
                except BaseException, erreur_base :
                    self.__affiche__erreur(erreur_base, 0, vals, 'write __migre_res_users')


    def __migre_res_partner_title(self):
        '''  migration titre des partenaires '''
        if self.company_id != 1:
            return
        partner_title_ids = self.connectionsource.search('res.partner.title', [], 0, 2000)
        for partner_title_id in partner_title_ids:
            vals = self.get_values(partner_title_id, 'res.partner.title')
            vals['x_old_id'] = partner_title_id
            categ = self.connectioncible.search('res.partner.title', [('domain', '=', vals['domain']), ('name', '=', vals['name'])], 0, 1)
            if not vals.has_key('shortcut'):
                vals['shortcut'] = vals['name'][:16]
            if not categ:
                try:
                    self.connectioncible.create('res.partner.title', vals)
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, partner_title_id, vals, "__migre_res_partner_title")

            else:
                try:
                    self.connectioncible.write('res.partner.title', categ, vals)
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, partner_title_id, vals, "__migre_res_partner_title write")


    def __migre_res_currency(self):
        ''' Migration devises '''
        currency_ids = self.connectionsource.search('res.currency', [], 0, 2000)
        for  currency_id in  currency_ids:
            if not self.connectioncible.search('res.currency', [('company_id', '=', self.company_id), ('x_old_id', '=', currency_id)], 0, 20000, 'id asc'):
                vals = self.get_values(currency_id, 'res.currency', [  'name', 'rounding', 'rate', 'active', 'accuracy'])
                vals['x_old_id'] = currency_id
                
                res = self.connectioncible.search('res.currency', [('name', '=', vals['name'])])
                if not res:
                    try:
                        self.connectioncible.create('res.currency', vals)
                    except BaseException, erreur_base:
                        self.__affiche__erreur(erreur_base, currency_id, vals, "create __migre_res_currency")
                                    
                else:
                    try:
                        self.connectioncible.write('res.currency', [res[0]], vals)
                    except BaseException, erreur_base:
                        self.__affiche__erreur(erreur_base, currency_id, vals, "write __migre_res_currency")
                        
    def __migre_res_partner_bank(self):
        ''' Migration banque partenaire '''
        partner_bank_ids = self.connectionsource.search('res.partner.bank', [], 0, 2000)
        for partner_bank_id in partner_bank_ids:
            vals = self.get_values(partner_bank_id, 'res.partner.bank')
            vals['x_old_id'] = partner_bank_id
            for field in ['post_number', 'bvr_number', 'bvr_adherent_num', 'bvr_zipcity', 'clearing', 'bvr_name', 'bvr_street']:
                try:
                    vals.pop(field)
                except BaseException:
                    pass
            bank = self.connectioncible.search('res.partner.bank', [('company_id', '=', self.company_id), ('x_old_id', '=', partner_bank_id), ('company_id', '=', self.company_id)], 0, 2)
            if not bank:
                try:
                    if vals.has_key('bank'):
                        banque = self.connectionsource.read('res.bank', vals['bank'])
                        vals['bank'] = self.__bank_get(banque['name'])
                    if vals.has_key('state'):
                        vals['state'] = 'bank'
                    if not vals.has_key('acc_number') and vals.has_key('iban') :
                        vals['acc_number'] = vals['iban']
                    if not vals.has_key('acc_number') and vals.has_key('post_number') :
                        vals['acc_number'] = vals['post_number']
                    if not vals.has_key('acc_number'):
                        vals['acc_number'] = "123456"
                    vals['company_id'] = self.company_id    
                    self.connectioncible.create('res.partner.bank', vals)
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, partner_bank_id, vals, "migre res.partner.bank")

                           
    def __migre_res_bank(self):
        ''' migration banque '''
        bank_ids = self.connectionsource.search('res.bank', [], 0, 2000)
        for bank_id in bank_ids:
            vals = self.get_values(bank_id, 'res.bank')
            if not self.__bank_get(vals['name']):
                vals['x_old_id'] = bank_id
                for field in ['code', 'bvr_zipcity', 'clearing', 'bvr_name', 'bvr_street']:
                    try:
                        vals.pop(field)
                    except BaseException:
                        pass
                bank = self.connectioncible.search('res.bank', [('name', '=', vals['name'])], 0, 2)
                if not bank:
                    try:
                        self.connectioncible.create('res.bank', vals)
                    except BaseException, erreur_base:
                        self.__affiche__erreur(erreur_base, bank_id, vals, "__migre_res_bank")

                    
    def __migre_res_part_bnk_type(self):
        ''' migration type de banque '''
        if self.company_id != 1:
            return
        partner_bank_ids = self.connectionsource.search('res.partner.bank.type', [], 0, 2000)
        for partner_bank_id in partner_bank_ids:
            vals = self.get_values(partner_bank_id, 'res.partner.bank.type',['code','name'])
            if vals.has_key('field_ids'):
                vals.pop('field_ids')
            vals['x_old_id'] = partner_bank_id
            partner_bank_id = self.connectioncible.search('res.partner.bank.type', [('name', '=', vals['name'])], 0, 2)
            if not partner_bank_id:
                try:
                    self.connectioncible.create('res.partner.bank.type', vals)
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, partner_bank_id, vals, "__migre_res_part_bnk_type")
    
    def __migre_account_journal_view(self):
        account_journal_view_ids = self.connectionsource.search('account.journal.view', [('active', 'in', ['true', 'false'])], 0, 20000, 'id asc')         
        for account_journal_view_id in account_journal_view_ids:
 
            vals = self.get_values(account_journal_view_id, 'account.journal.view')
            vals['x_old_id'] = account_journal_view_id
            vals['company_id'] = self.company_id
            self.connectioncible.create('account.journal.view', vals)
            
    def __migre_account_journal(self):
        ''' migre les journaux comptable '''
 
        account_journal_ids = self.connectionsource.search('account.journal', [('active', 'in', ['true', 'false'])], 0, 20000, 'id asc')         
        for account_journal_id in account_journal_ids:
            #old_account_journal = self.connectionsource.read('account.journal', account_journal_id,['view_id'])
            vals = self.get_values(account_journal_id, 'account.journal')
#            for field in ['active', 'refund_journal']:
#                try:
#                    vals.pop(field)
#                except BaseException:
#                    pass

            vals['x_old_id'] = account_journal_id
            vals['company_id'] = self.company_id
            res_journal = self.connectioncible.search('account.journal', [('company_id', '=', self.company_id), ('x_old_id', '=', account_journal_id)], 0, 20000, 'id asc')
            
            if vals.has_key('currency'):
                if vals['currency'] == self.company_currency :
                    vals['currency'] = False
            else:
                vals['currency'] = False

            if vals.has_key('default_debit_account_id') and vals['default_debit_account_id']:
                try:
                    if vals['currency']:
                        self.connectioncible.write('account.account', vals['default_debit_account_id'], {'currency_id':vals['currency']})
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base,  vals['default_debit_account_id'], vals, "write account default_debit_account_id __migre_account_journal")


            if vals.has_key('default_credit_account_id') and vals['default_credit_account_id']:
                try:
                    if vals['currency']:
                        self.connectioncible.write('account.account', vals['default_credit_account_id'], {'currency_id':vals['currency']})
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, account_journal_id, vals, "write account default_credit_account_id __migre_account_journal")

            if not  vals.has_key('company_id'):
                vals['company_id'] = self.company_id


            exist_name = self.connectioncible.search('account.journal', [('name', '=', vals['name'])], 0, 20000, 'id asc')
            if exist_name:
                name = self.connectionsource.search('ir.translation', [('name' , '=', 'account.journal, name'), ('res_id', '=', account_journal_id), ('src', '=', vals['name']), ('lang', '=', 'fr_FR')])
                if name:
                    vals['name'] = self.connectionsource.read('ir.translation', name[0], ['value'])['value']
                    exist_name = self.connectioncible.search('account.journal', [('name', '=', vals['name'])], 0, 20000, 'id asc')
                vals['name'] = vals['name'] + " - " + vals['code']+" - " + str(account_journal_id)

            exist_code = self.connectioncible.search('account.journal', [('code', '=', vals['code'])], 0, 20000, 'id asc')
            if exist_code:
                journal_cible = self.connectioncible.read('account.journal', exist_code[0], ['name'])
                if journal_cible['name'] != vals['name']:
                    vals['code'] = vals['code']+" - "+ str(account_journal_id)
                    exist_code = False

            
            if not res_journal :  
                if exist_name and exist_code:
                    try:
                        self.connectioncible.write('account.journal', exist_name[0], vals)
                    except BaseException, erreur_base:
                        self.__affiche__erreur(erreur_base, account_journal_id, vals, "__migre_account_journal write exist name")

                #~ elif exist_code:
                    #~ try:
                        #~ self.connectioncible.write('account.journal', exist_code[0], vals)
                    #~ except BaseException, erreur_base:
                        #~ self.__affiche__erreur(erreur_base, account_journal_id, vals, "__migre_account_journal write exist code")

                else:
                    try:
                        self.connectioncible.create('account.journal', vals)   
                    except BaseException, erreur_base:
                        self.__affiche__erreur(erreur_base, account_journal_id, vals, "__migre_account_journal create")

            else:
                try:
                    vals.pop('code')
                    vals.pop('name')
                    self.connectioncible.write('account.journal', res_journal[0], vals)
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, account_journal_id, vals, "__migre_account_journal write on create")

                    
    def __migre_acc_analyt_journal(self):
        ''' migre les journaux analytiques '''
        account_analytic_journal_ids = self.connectionsource.search('account.analytic.journal', [], 0, 20000, 'id asc')         
        for account_analytic_journal_id in account_analytic_journal_ids:
            vals = self.get_values(account_analytic_journal_id, 'account.analytic.journal', ['name', 'code', 'active', 'type'])
            vals['x_old_id'] = account_analytic_journal_id
            vals['company_id'] = self.company_id
            res = self.connectioncible.search('account.analytic.journal', [('company_id', '=', self.company_id), ('x_old_id', '=', account_analytic_journal_id)], 0, 20000, 'id asc')
            if not res:
                try:
                    self.connectioncible.create('account.analytic.journal', vals)  
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, account_analytic_journal_id, vals, "__migre_acc_analyt_journal")

            else:
                try:
                    self.connectioncible.write('account.analytic.journal', res[0], vals)  
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, account_analytic_journal_id, vals, "__migre_acc_analyt_journal write")

                
    def __migre_acc_fiscal_year(self):
        ''' Migration annee fiscale et periode '''
        account_fiscalyear_ids = self.connectionsource.search('account.fiscalyear', [], 0, 100, 'date_stop asc')         
        for account_fiscalyear_id in account_fiscalyear_ids:
           
            valsfiscal = self.get_values(account_fiscalyear_id, 'account.fiscalyear', ['date_stop', 'code', 'name', 'date_start', 'start_journal_id', 'company_id', 'state', 'end_journal_id'])
            date_du_jour = datetime.datetime.now().strftime('%Y-%m-%d')
            
            if date_du_jour >= valsfiscal['date_start'] and date_du_jour <= valsfiscal['date_stop']:
                current_account_fiscalyear_id = account_fiscalyear_id 
                self.current_account_period_ids = self.connectionsource.search('account.period', [('fiscalyear_id', '=', current_account_fiscalyear_id)])

            valsfiscal['x_old_id'] = account_fiscalyear_id
            valsfiscal['state'] = 'draft'
            valsfiscal['company_id'] = self.company_id
            newfy = self.connectioncible.search('account.fiscalyear', [('x_old_id', '=', account_fiscalyear_id)], 0, 20000, 'id asc') 
            if not newfy:
                try:
                    newfy = self.connectioncible.create('account.fiscalyear', valsfiscal)
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, account_fiscalyear_id, valsfiscal, "create fiscalyear")

            else:
                newfy =  newfy[0]
            account_period_ids = self.connectionsource.search('account.period', [('fiscalyear_id', '=', account_fiscalyear_id)], 0, 20000, 'id asc')    
            for account_period in  account_period_ids:
                period_read = self.connectionsource.read('account.period', account_period)
                res = self.connectioncible.search('account.period', [('company_id', '=', self.company_id), ('x_old_id', '=', account_period)], 0, 20000, 'id asc')
                if not res:
                    
                    res = self.connectioncible.search('account.period', [('company_id', '=', self.company_id), ('date_start', '=', period_read['date_start']), ('date_stop', '=', period_read['date_stop'])], 0, 20000, 'id asc')
                if not res and (period_read['date_start'] >= valsfiscal['date_start'] and period_read['date_stop'] <= valsfiscal['date_stop']):     
                    vals = self.get_values(account_period, 'account.period')
                    vals['fiscalyear_id'] =  newfy
                    vals['company_id'] = self.company_id
                    vals['state'] = 'draft'
                    vals['x_old_id'] = account_period
                    try:
                        self.connectioncible.create('account.period', vals)
                    except BaseException, erreur_base:
                        
                        try:
                            vals['name'] = vals['name'] + vals['date_stop'] .split('-')[0] 
                            vals['code'] = vals['code'] + vals['date_stop'] .split('-')[0] 
                            self.connectioncible.create('account.period', vals)
                        except BaseException, erreur_base:
                            self.__affiche__erreur(erreur_base, account_period, vals, "__migre_acc_fiscal_year") 

                elif   (period_read['date_start'] >= valsfiscal['date_start'] and period_read['date_stop'] <= valsfiscal['date_stop']):
                    vals = {}
                    vals['x_old_id'] = account_period
                    try:
                        self.connectioncible.write('account.period', res[0], vals)
                    except BaseException, erreur_base:
                        self.__affiche__erreur(erreur_base, account_period, vals, "__migre_acc_fiscal_year") 

                    
    def __migre_account_type(self):
        ''' migration des types de comptes '''
        if self.company_id != 1:
            return
        account_account_type_ids = self.connectionsource.search('account.account.type', [], 0, 20000, 'id asc')         
        for account_account_type_id in account_account_type_ids:
            vals = self.get_values(account_account_type_id, 'account.account.type', ['code', 'name', 'close_method'])
            vals['x_old_id'] = account_account_type_id
            if not self.connectioncible.search('account.account.type', [('x_old_id', '=', account_account_type_id)], 0, 20000, 'id asc'):
                self.connectioncible.create('account.account.type', vals)
        return ""

    def __create_analytic_account(self, account_analytic_id):
        ''' Creation d'un compte analytique '''
        vals = self.get_values(account_analytic_id, 'account.analytic.account', ['code', 'contact_id', 'date', 'partner_id', 'user_id', 'date_start', 'company_id', 'parent_id', 'state', 'complete_name', 'description', 'name']
    )
        vals['x_old_id']   = account_analytic_id 
        vals['company_id'] = self.company_id
        if vals.has_key('parent_id'):
            if not self.connectioncible.search('account.analytic.account', [('x_old_id', '=', vals['parent_id'])]):
                self.__create_analytic_account(vals['parent_id'])
        res = self.connectioncible.search('account.analytic.account', [('x_old_id', '=', account_analytic_id), ('company_id', '=', self.company_id)])
        if not res:
            try:
                self.connectioncible.create('account.analytic.account', vals)
            except BaseException, erreur_base:
                self.__affiche__erreur(erreur_base, account_analytic_id, vals, "__migre_acc_analytic_acc") 
        else:
            try:
                self.connectioncible.write('account.analytic.account', res, vals)
            except BaseException, erreur_base:
                self.__affiche__erreur(erreur_base, account_analytic_id, vals, "__migre_acc_analytic_acc write")

                
    def __migre_acc_analytic_acc(self):
        ''' migration des comptes analytiques '''
        account_analytic_ids = self.connectionsource.search('account.analytic.account', [('active', 'in', ['true', 'false'])], 0, 20000, 'parent_id desc')         
        for account_analytic_id in account_analytic_ids:
            self.__create_analytic_account(account_analytic_id)

    def __create_account(self, account_id):
        ''' migration d'un compte comptable '''

        unreconciled_payable = self.connectioncible.search('account.account.type', [('code', '=', 'payable'), ('close_method', '=', 'unreconciled')], 0, 1, 'id asc')
        unreconciled_receivable = self.connectioncible.search('account.account.type', [('code', '=', 'receivable'), ('close_method', '=', 'unreconciled')], 0, 1, 'id asc')
        res = self.connectioncible.search('account.account', [('x_old_id', '=', account_id), ('company_id', '=', self.company_id), ('active', 'in', ['true', 'false'])])
        vals = self.get_values(account_id, 'account.account', ['code', 'reconcile', 'user_type', 'currency_id', 'company_id', 'shortcut', 'note', 'parent_id', 'type', 'active', 'company_currency_id',  'name', 'currency_mode'])
        vals['x_old_id'] = account_id
        vals['company_id'] = self.company_id
        
        if vals.has_key('currency_id') and vals['currency_id'] == self.company_currency:
            vals['currency_id'] = False
        vals['active'] = True
        if vals['type'] not in  ('view', 'other', 'receivable', 'payable', 'liquidity', 'consolidation', 'closed'):
            vals['type'] = 'other'  			
        if vals['type'] == 'payable' :
            vals['user_type'] = unreconciled_payable[0]
        elif vals['type'] == 'receivable':
            vals['user_type'] = unreconciled_receivable[0]
       
        try:
            #print vals['code'], vals['name'], vals['currency_id']
            if not res:
                exist_code = self.connectioncible.search('account.account', [('code', '=', vals['code'])], 0, 20000, 'id asc')
                if exist_code:
                    vals['code'] = vals['code'] + "_" + str(len(exist_code) + 1)
                self.connectioncible.create('account.account', vals)
            else:
                self.connectioncible.write('account.account', res[0], vals)
        except BaseException, erreur_base:
            self.__affiche__erreur(erreur_base, account_id, vals, "__migre_account_account")
            

    def __migre_account_account(self):
        ''' Migration des comptes compables '''
        account_ids = self.connectionsource.search('account.account', [], 0, 1, 'parent_id desc, id asc')         
        self.__create_account(account_ids[0])
        
        account_ids = self.connectionsource.search('account.account', [('active', 'in', ['true', 'false'])], 0, 20000000, 'parent_id desc, id asc')         
        for account_id in account_ids:
            #print account_id
#~            res = self.connectioncible.search('account.account', [('x_old_id', '=', account_id), ('company_id', '=', self.company_id), ('active', 'in', ['true', 'false'])])
#            if not res:
            self.__create_account(account_id)
        print 'Migration %s compte comptable '% len(account_ids)
        #    self.connectioncible.execute('account.account', '_parent_store_compute')
                
    def __migre_account_bank_statement(self):
        ''' migration etat de banque '''
        account_bank_statement_ids = self.connectionsource.search('account.bank.statement', [('period_id', 'in', self.current_account_period_ids)], 0, 1000000, 'id asc')         
        nbr = len(account_bank_statement_ids)
        compteur = 0
        for account_bank_statement_id in account_bank_statement_ids:
            res = self.connectioncible.search('account.bank.statement', [('company_id', '=', self.company_id), ('x_old_id', '=', account_bank_statement_id)])
            if not res :
                vals = self.get_values(account_bank_statement_id, 'account.bank.statement', ['name', 'state', 'balance_end', 'balance_start', 'journal_id', 'currency', 'period_id', 'date', 'x_old_id', 'balance_end_real'])
                compteur = compteur +1
                if (compteur % 100) == 0:
                    print "Account bank statement %s / %s " % (compteur, nbr)
                vals['x_old_id'] = account_bank_statement_id
                vals['company_id'] = self.company_id
                vals['total_entry_encoding'] = vals['balance_end_real'] - vals['balance_start']
                vals['state'] = 'draft' 
                try:
                    account_bank_statement_id = self.connectioncible.create('account.bank.statement', vals)
                except BaseException, erreur_base:
                    print "Journal id ", vals['journal_id']
                    self.__affiche__erreur(erreur_base, account_bank_statement_id, vals, "__migre_account_bank_statement")
 
            else:
                vals = self.connectioncible.read('account.bank.statement', res[0])
                account_bank_statement_id = res[0]
            self.__migre_acc_bnk_stat_line(account_bank_statement_id)             

    def __migre_acc_bnk_stat_line(self, account_bank_statement_line_id):
        ''' migre les lignes de etat de banque '''
        account_bank_statement_line_ids = self.connectionsource.search('account.bank.statement.line', [('statement_id', '=', account_bank_statement_line_id)], 0, 1000000, 'id asc')         
        for account_bank_statement_line_id in account_bank_statement_line_ids:
            res = self.connectioncible.search('account.bank.statement.line', [('company_id', '=', self.company_id), ('x_old_id', '=', account_bank_statement_line_id)])
            if not res :
                
                #debug=True
                vals = self.get_values(account_bank_statement_line_id, 'account.bank.statement.line', [ 'statement_id', 'type', 'account_id',  'amount', 'date', 'x_old_id', 'partner_id', 'name'])
                vals['company_id'] = self.company_id
                vals['x_old_id'] = account_bank_statement_line_id
                try:
                    if vals.has_key('account_id'):
                        account_bank_statement_line_id = self.connectioncible.create('account.bank.statement.line', vals)
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, account_bank_statement_line_id, vals, "__migre_acc_bnk_stat_line") 


    def __migre_account_invoice(self):
        ''' migration factures '''
        account_invoice_ids = self.connectionsource.search('account.invoice', [('period_id', 'in', self.current_account_period_ids)], 0, 1000000, 'id asc')         
        nbr = len(account_invoice_ids)
        compteur = 0
        for account_invoice_id in account_invoice_ids:
            res = self.connectioncible.search('account.invoice', [('company_id', '=', self.company_id), ('x_old_id', '=', account_invoice_id)])
            #print "invoice ", self.connectionsource.execute('account.invoice, action, ids, offset, limit, order, context)
            compteur = compteur + 1
            
            vals = self.get_values(account_invoice_id, 'account.invoice', ['period_id', 'move_id', 'date_due', 'check_total', 'payment_term', 'number', 'journal_id', 'currency_id', 'address_invoice_id', 'reference', 'account_id', 'amount_untaxed', 'address_contact_id', 'reference_type', 'company_id', 'amount_tax', 'state', 'type', 'date_invoice', 'amount_total', 'partner_id', 'name', 'create_uid'])
            vals['company_id'] = self.company_id
            if (compteur%100) == 0:
                print "Account invoice :  ", compteur, '/', nbr
            vals['x_old_id'] = account_invoice_id
            if vals.has_key('number'):
                vals['internal_number'] = vals['number']
                vals.pop('number')
            if vals.has_key('payment_term'):
                payment_term = self.connectioncible.read('account.payment.term', vals['payment_term'])
                if payment_term:
                    term_name = payment_term['name']
                    if term_name:
                        vals['payment_term'] =  self.__get_acc_pay_term(term_name)
            if not vals.has_key('period_id'):
                periode = self.connectioncible.search('account.period', [('special', '=', False), ('date_stop', '>=', vals['date_invoice']), ('date_start', '<=', vals['date_invoice'])])
                if periode:
                    vals['period_id'] = periode[0]
            if vals.has_key('address_invoice_id'):
                vals.pop('address_invoice_id')
            if vals.has_key('address_contact_id'):
                vals.pop('address_contact_id')
            if vals.has_key('number') and vals['number'] == '/':
                vals['number'] = "/" + str(account_invoice_id)
            if not  vals.has_key('number'):
                vals['number'] = "/" + str(account_invoice_id)
            vals['state'] = 'draft' 
            new_invoice_id = False
            
            if not res :
                try:
                    new_invoice_id = self.connectioncible.create('account.invoice', vals)    
                    self.connectioncible.write('account.invoice', new_invoice_id, {'number':vals['number']})  
                except BaseException, erreur_base :
                    self.__affiche__erreur(erreur_base, 0, vals, "__migre_account_invoice create")
            else:
                try:
                    new_invoice_id = res[0]
                    self.connectioncible.write('account.invoice', new_invoice_id, vals)
                except BaseException, erreur_base :
                    self.__affiche__erreur(erreur_base, new_invoice_id, vals, "__migre_account_invoice write")
            if new_invoice_id:
                self.__migre_account_invoice_line(account_invoice_id, new_invoice_id)  
                self.__valid_invoice(new_invoice_id)
        print "%s factures "% nbr
            
    def __migre_account_invoice_line(self, invoice_id, new_invoice_id):
        ''' Migration Lignes de factures '''
        account_invoice_line_ids = self.connectionsource.search('account.invoice.line', [('invoice_id', '=', invoice_id)], 0, 1000000, 'id asc')         
        for account_invoice_line_id in account_invoice_line_ids:
            res = self.connectioncible.search('account.invoice.line', [('company_id', '=', self.company_id), ('x_old_id', '=', account_invoice_line_id)])
            vals = self.get_values(account_invoice_line_id, 'account.invoice.line')
            vals['x_old_id'] = account_invoice_line_id
            vals['invoice_id'] =  new_invoice_id
            vals['company_id'] = self.company_id
            for val in ['state', 'price_subtotal_incl']:
                if vals.has_key(val):
                    vals.pop(val)
                
            if vals.has_key('note'):
                vals.pop('note')

            if not res :
                try:
                    account_invoice_line_id = self.connectioncible.create('account.invoice.line', vals)      
                except BaseException, erreur_base :
                    self.__affiche__erreur(erreur_base, account_invoice_line_id, vals, "__migre_account_invoice_line create")
            else:
                try:
                    account_invoice_line_id = res[0]
                    self.connectioncible.write('account.invoice.line', account_invoice_line_id , vals)
                except BaseException, erreur_base :
                    self.__affiche__erreur(erreur_base, account_invoice_line_id, vals, "__migre_account_invoice_line write")

     
    def migre_product_price_list_item(self,pricelist_id,new_pricelist_id):
        product_pricelist_item_ids = self.connectionsource.search('product.pricelist.item', [('base_pricelist_id','=',pricelist_id)], 0, LIMITE)       
        for product_pricelist_item_id in product_pricelist_item_ids :
            res  = self.connectioncible.search('product.pricelist.item', [('x_old_id', '=', product_pricelist_item_id)], 0, LIMITE)       
            if not res:
                vals = self.get_values(product_pricelist_item_id, 'product.pricelist.item', ['active','date_end','date_start','name'])
                vals['pricelist_id'] = new_pricelist_id
                vals['x_old_id'] = product_pricelist_item_id        
                try:
                    res = self.connectioncible.create('product.pricelist.item', vals)
                except BaseException, erreur_base :
                    print erreur_base.__str__()
                    #self.__affiche__erreur(erreur_base, product_pricelist_item_id, vals, "__migre_price_list item")
                    
    def migre_product_price_list_version(self,pricelist_id,new_pricelist_id):
        product_pricelist_version_ids = self.connectionsource.search('product.pricelist.version', [('pricelist_id','=',pricelist_id)], 0, LIMITE)       
        for product_pricelist_version_id in product_pricelist_version_ids :
            res  = self.connectioncible.search('product.pricelist.version', [('x_old_id', '=', product_pricelist_version_id)], 0, LIMITE)
            if not res:
                vals = self.get_values(product_pricelist_version_id, 'product.pricelist.version', ['active','date_end','date_start','name'])
                vals['pricelist_id'] = new_pricelist_id
                vals['x_old_id'] = product_pricelist_version_id        
                try:
                    res = self.connectioncible.create('product.pricelist.version', vals)
                except BaseException, erreur_base :
                    print erreur_base.__str__()
                    
                
                #    self.__affiche__erreur(erreur_base, product_pricelist_version_id, vals, "__migre_price_list version")

    def migre_product_price_list(self):
        product_pricelist_ids = self.connectionsource.search('product.pricelist', [], 0, LIMITE)       
        for product_pricelist_id in product_pricelist_ids :
            price_list = self.connectionsource.read('product.pricelist', product_pricelist_id)
            res  = self.connectioncible.search('product.pricelist', [('name', '=', price_list['name'])], 0, LIMITE)
            vals={}       
            if not res:
                vals = self.get_values(product_pricelist_id, 'product.pricelist', ['active','currency_id','type','name'])
                vals['x_old_id'] = product_pricelist_id
                res_id = None
                try:
                    res_id = self.connectioncible.create('product.pricelist', vals)
                except BaseException, erreur_base :
                    self.__affiche__erreur(erreur_base, product_pricelist_id, vals, "__migre_price_list")
                self.migre_product_price_list_version(product_pricelist_id, res_id)
                self.migre_product_price_list_item(product_pricelist_id, res_id)
            else:
                self.connectioncible.write('product.pricelist', res,vals)
                
    def __migre_account_move(self):
        ''' Migration des ecritures comptables '''
        account_move_ids = self.connectionsource.search('account.move', [('period_id', 'in', self.current_account_period_ids)], 0, LIMITE)       
        nbr = len(account_move_ids)
        compteur = 0  
        for account_move_id in account_move_ids:
            res = self.connectioncible.search('account.move', [('company_id', '=', self.company_id), ('x_old_id', '=', account_move_id)])
            compteur = compteur + 1
            if (compteur % 100) == 0:
                print "Account Move %s / %s'" % (compteur, nbr)

            if not res:
                vals = self.get_values(account_move_id, 'account.move', ['ref', 'name', 'state', 'partner_id', 'journal_id', 'period_id', 'date',  'to_check'])
                vals['company_id'] = self.company_id
                vals['x_old_id'] = account_move_id

                if vals['name'] == '/':
                    #print "New Name"
                    vals['name'] = str(account_move_id)
                vals['state'] = 'draft' 
                #print vals
                
                try:
            #       print "create"
                    new_move_id = self.connectioncible.create('account.move', vals)
                except BaseException, erreur_base :
                    self.__affiche__erreur(erreur_base, account_move_id, vals, "__migre_account_move ")
                self.__migre_acc_move_line(account_move_id, new_move_id)  
        print "%s move " % nbr
            
    def __migre_acc_move_line(self, move_id, new_move_id):
        ''' migration ligne d'ecritures'''
        account_move = self.connectioncible.read('account.move', new_move_id )  
        account_move_line_ids = self.connectionsource.search('account.move.line', [('move_id', '=', move_id)], 0, 222000)         
        for account_move_line_id in account_move_line_ids:
            res = self.connectioncible.search('account.move.line', [('company_id', '=', self.company_id), ('x_old_id', '=', account_move_line_id)])
            vals = self.get_values(account_move_line_id, 'account.move.line', ['debit', 'credit', 'statement_id', 'currency_id', 'date_maturity', 'invoice', 'partner_id', 'blocked', 'analytic_account_id', 'centralisation', 'journal_id', 'tax_code_id', 'state', 'amount_taxed', 'ref', 'origin_link', 'account_id', 'period_id', 'amount_currency', 'date', 'move_id', 'name', 'tax_amount', 'product_id', 'account_tax_id', 'product_uom_id', 'followup_line_id', 'quantity'])
            vals['company_id'] = self.company_id
            vals['x_old_id'] = account_move_line_id
#            if vals.has_key('account_id'):
#                compte = self.connectioncible.read('account.account', vals['account_id'] , ['currency_id'])
#                if compte['currency_id'] and compte['currency_id'][0] == self.company_currency:
#                    self.connectioncible.write('account.account',vals['account_id'],{'currency_id':None})
                #if not vals.has_key('currency_id') and compte['currency_id'] and compte['currency_id'][0] != self.company_currency:
                #    vals['currency_id'] = compte['currency_id'][0]
            # print "Compte ",compte
            if vals.has_key('amount_taxed'):
                vals['tax_amount'] = vals['amount_taxed']
                vals.pop('amount_taxed')
            
            if vals.has_key('period_id'):
                if vals['period_id'] != account_move['period_id'][0]: ## incoherence de periode entre move et line
                    vals['period_id'] = account_move['period_id'][0]
            if vals.has_key('currency_id'):
                if vals['currency_id'] == self.company_currency:
                    vals['currency_id'] = None
                    vals['amount_currency'] = None
            #   if compte and compte['currency_id'] and vals.has_key('currency_id'):
            #       if compte['currency_id'] != vals['currency_id']:
            #           try:
            #               self.connectioncible.write('account.account', vals['account_id'] , {'currency_id':False})
            #           except BaseException, erreur_base :
            #               self.__affiche__erreur(erreur_base, account_move_line_id, vals, "write account.account currency __migre_acc_move_line  ")
            else:
                vals['currency_id'] = None
                vals['amount_currency'] = None
            if vals.has_key('name'):
                if vals['name'] == '':
                    vals['name'] = str(account_move_line_id)
            else:
                vals['name'] = str(account_move_line_id)
            vals['move_id'] =  new_move_id
            vals['state'] = 'draft' 
            if not res:
                try:
                    self.connectioncible.create('account.move.line', vals)      
                except BaseException, erreur :
                    print "Erreur Move Line new ", vals
                    print "Erreur Move line old", self.connectionsource.read('account.move.line',account_move_line_id)
                    if hasattr(erreur, 'faultCode'):
                        print erreur.faultCode
                    if hasattr(erreur, 'faultString'):
                        print erreur.faultString
                    if hasattr(erreur, 'message'):
                        print erreur.message
                    #pass
                    #self.__affiche__erreur(erreur_base, account_move_line_id, vals, "__migre_acc_move_line create ")

            
    def migre_res_company(self):
        ''' migration compagnie '''
        res_company_ids = self.connectionsource.search('res.company', [], 0, 1, 'id asc')         
        for res_company_id in res_company_ids:
            vals = self.get_values(res_company_id, 'res.company',['name','logo','currency_id','rml_header'])
            for field in ['bvr_delta_vert', 'bvr_delta_horz', 'bvr_header','partner_id']:
                try:
                    vals.pop(field)
                except BaseException:
                    pass
            vals['x_old_id'] = res_company_id
            try:
                vals['partner_id'] = 1
                res_company_id = self.connectioncible.write('res.company', self.company_id, vals)
            except BaseException, erreur_base:
                self.__affiche__erreur(erreur_base, res_company_id, vals, "migre_res_company") 


    def __migre_res_part_addr(self, partner_id=None, partner_new_id=None):
        ''' migration des adresses partenaires '''
        if partner_id:
            res_partner_address_ids = self.connectionsource.search('res.partner.address', [('partner_id', '=', partner_id)], 0, 99999999, 'id asc')
        else:
            res_partner_address_ids = self.connectionsource.search('res.partner.address', [('partner_id', '=', False)], 0, 99999999, 'id asc')         
        
        for res_partner_address_id in res_partner_address_ids:
            res = self.connectioncible.search('res.partner', [('company_id', '=', self.company_id), ('x_old_address_id', '=', res_partner_address_id)])
            vals = self.connectionsource.read('res.partner.address',res_partner_address_id, ['name','partner_id','street','street2','zip','city','country_id','phone','mail'])
            if vals['name']:

                if vals.has_key('partner_id'):
                    vals.pop('partner_id')
                vals['company_id'] = self.company_id
                vals["use_parent_address"] = 1
                if partner_new_id:
                    vals['parent_id'] = partner_new_id
                vals['x_old_address_id'] = res_partner_address_id
                if vals.has_key('type'):
                    if vals['type'] not in ('default', 'invoice', 'delivery', 'contact', 'other'):
                        vals['type'] = 'other'
                if vals.has_key('country_id') and vals['country_id']:
                    vals['country_id'] = vals['country_id'][0]
                    
    
                if vals.has_key('title'):
                    title = self.connectioncible.search('res.partner.title', [('domain', '=', "contact"), ('name', '=', vals['title'])], 0, 2)
                    if not title:
                        title = self.connectioncible.search('res.partner.title', [('domain', '=', "contact"), ('shortcut', '=', vals['title'])], 0, 2)
                    if title:
                        vals['title'] = title[0]
                    else:
                        vals['title'] = self.connectioncible.create('res.partner.title', {'domain':'contact', 'name':vals['title'], 'shortcut':vals['title']})
    
                if res:
                    try:
                        res_partner_address_id = self.connectioncible.write('res.partner', res[0], vals)
                    except BaseException, erreur_base:
                        self.__affiche__erreur(erreur_base, res_partner_address_id, vals, "__migre_res_part_addr write")
    
                else:
                    try:
                        res_partner_address_id = self.connectioncible.create('res.partner', vals)
                    except BaseException, erreur_base:
                        self.__affiche__erreur(erreur_base, res_partner_address_id, vals, "__migre_res_part_addr create") 


    def __cree_account_tax_code(self, account_tax_code_id):
        ''' creation des codes de taxes '''
        res = self.connectioncible.search('account.tax.code', [('x_old_id', '=', account_tax_code_id)] , 0, 8000, 'id')
        if not res :
            vals = self.get_values(account_tax_code_id, 'account.tax.code', ['info', 'name', 'sign', 'parent_id', 'notprintable', 'code'])
            vals['company_id'] = self.company_id
            vals['x_old_id'] = account_tax_code_id

            try:
                account_tax_code_id = self.connectioncible.create('account.tax.code', vals)
            except BaseException, erreur_base:
                self.__affiche__erreur(erreur_base, account_tax_code_id, vals, "__migre_acc_tax_code") 	


    def __migre_acc_tax_code(self):
        ''' migre compte de taxe '''
        account_tax_code_ids = self.connectionsource.search('account.tax.code', [], 0, 1000000, 'id asc')         
        for account_tax_code_id in account_tax_code_ids:
            self.__cree_account_tax_code(account_tax_code_id)

    def __migre_account_tax(self):
        ''' migre les taxes '''
        account_tax_ids = self.connectionsource.search('account.tax', [('active', 'in', ['true', 'false'])], 0, 1000000, 'id asc')         
        for account_tax_id in account_tax_ids:
            res = self.connectioncible.search('account.tax', [('company_id', '=', self.company_id), ('active', 'in', ['true', 'false']), ('x_old_id', '=', account_tax_id)])
            if not res :
                vals = self.get_values(account_tax_id, 'account.tax', ['ref_base_code_id', 'ref_tax_code_id', 'sequence', 'base_sign', 'child_depend', 'include_base_amount', 'applicable_type', 'company_id', 'tax_code_id', 'python_compute_inv', 'ref_tax_sign', 'type', 'ref_base_sign', 'type_tax_use', 'base_code_id', 'active', 'x_old_id', 'name', 'account_paid_id', 'account_collected_id', 'amount', 'python_compute', 'tax_sign', 'price_include'])
                vals['company_id'] = self.company_id
                vals['x_old_id'] = account_tax_id

                vals['active'] = True
                exist_name = self.connectioncible.search('account.tax', [('active', 'in', ['true', 'false']), ('name', '=', vals['name'])], 0, 20000, 'id asc')
                if exist_name:
                    vals['name'] = vals['name'][:60] + "_" + str(account_tax_id)
                    #print vals['name']
                try:
                    account_tax_id = self.connectioncible.create('account.tax', vals)
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, account_tax_id, vals, "__migre_account_tax") 

                
    def __migre_res_partner(self):
        ''' migration partenaire '''
        partner_ids = self.connectionsource.search('res.partner', [('active', 'in', ['true', 'false'])], 0, 20000, 'id asc')
        nbr = len(partner_ids)
        compteur = 0

        for partner_id in partner_ids:
            compteur += 1
            
            partner_cible_id = self.connectioncible.search('res.partner', [('company_id', '=', self.company_id), ('x_old_id', '=', partner_id), ('active', 'in', ['true', 'false'])], 0, 80)
            if not partner_cible_id:
                #print "partner_id ", partenaire['name']
                vals = self.get_values(partner_id, 'res.partner', ['address', 'property_product_pricelist', 'city', 'property_account_payable', 'debit', 'x_old_id', 'vat', 'website', 'customer', 'supplier', 'date', 'active', 'lang', 'credit_limit', 'name', 'country', 'property_account_receivable', 'credit', 'debit_limit', 'category_id'])
                vals['company_id'] = self.company_id
                
                if vals.has_key('country'):
                    vals['country_id'] = vals['country']
                    vals.pop('country') 
                vals['is_company'] = 1
                if (compteur % 100) == 0:
                    print "Partenaire %s/%s pour la company %s " % (compteur, nbr, self.company_id)
                vals['x_old_id'] = partner_id
                if vals.has_key('vat'):
                    try:
                        result_check = self.connectioncible.object.execute(self.connectioncible.dbname, self.connectioncible.uid, self.connectioncible.pwd, 'res.partner', 'simple_vat_check', 'ch', vals['vat'])
                        if result_check == False:
                            vals['vat'] = ""
                    except BaseException:
                        pass
                
                if vals.has_key('address'):
                    addresses = vals['address']
                else:
                    addresses = None
          
                #~ for champ in ['bank_ids', 'events', 'address', 'property_stock_supplier', 'vat_subjected', 'property_stock_customer', 'property_product_pricelist_purchase', 'suid']:
                    #~ try:
                        #~ vals.pop(champ)
                    #~ except BaseException:
                        #~ pass
                    
                
                if vals.has_key('lang') and vals['lang'] == 'fr_FR':
                    vals['lang'] = 'fr_CH'
                
                if partner_id == 1:
                    partner_cible_id = [1]
                if partner_cible_id:
                    try:
                        self.connectioncible.write('res.partner', partner_cible_id[0], vals)
                    except BaseException, erreur_base :
                        self.__affiche__erreur(erreur_base, partner_id, vals, "write partner __migre_res_partner ")
                    partner_new_id = partner_cible_id[0]
                else:
                    try:
                        partner_new_id = self.connectioncible.create('res.partner', vals)
                        #print partner_new_id, vals
                    except BaseException, erreur_base:
                        self.__affiche__erreur(erreur_base, partner_id, vals, "create partner __migre_res_partner")

                if addresses:
                    self.__migre_res_part_addr(partner_id, partner_new_id)


    def __create_partner_categ(self, partner_category_id):
        ''' creation des categories partenaires '''        
        vals = self.get_values(partner_category_id, 'res.partner.category')
        
        vals['x_old_id'] = partner_category_id
        if vals.has_key('child_ids'):
            vals.pop('child_ids')
        if vals.has_key('name'):
            categ = self.connectioncible.search('res.partner.category', [('name', '=', vals['name'])], 0, 2)
        else:
            vals['name'] = 'undefined'
       
        if vals.has_key('parent_id'):
            if not self.connectioncible.search('res.partner.category', [('x_old_id', '=', vals['parent_id'])]):
                self.__create_partner_categ(vals['parent_id'])
        
        if not categ:
            try:
                self.connectioncible.create('res.partner.category', vals)
            except BaseException, erreur_base:
                self.__affiche__erreur(erreur_base, 0, vals, "__migre_res_partner_category")
        else:
            try:
                self.connectioncible.write('res.partner.category', categ[0], vals)
            except BaseException, erreur_base:
                self.__affiche__erreur(erreur_base, 0, vals, "__migre_res_partner_category write")

                
    def __migre_res_partner_category(self):
        ''' migration des categories de partenaire '''
        if self.company_id != 1:
            return
        partner_category_ids = self.connectionsource.search('res.partner.category', [], 0, 2000)
        for partner_category_id in partner_category_ids:
            self.__create_partner_categ(partner_category_id)
            
    def __create_prod_categ(self, product_category_id):     
        ''' creation des categories produits '''                     
        vals = self.get_values(product_category_id, 'product.category')
        
        vals['x_old_id'] = product_category_id
        if vals.has_key('child_id'):
            vals.pop('child_id')
        if vals.has_key('company_id'):
            vals.pop('company_id')
            
        categ = self.connectioncible.search('product.category', [('name', '=', vals['name'])], 0, 2)
        if vals.has_key('parent_id'):
            if not self.connectioncible.search('product.category', [('x_old_id', '=', vals['parent_id'])]):
                self.__create_prod_categ(vals['parent_id'])
                
        if not categ:
            try:
                self.connectioncible.create('product.category', vals)
            except BaseException, erreur_base:
                self.__affiche__erreur(erreur_base, 0, vals, "__migre_product_category")

        else:
            try:
                self.connectioncible.write('product.category', categ[0], vals)
            except BaseException, erreur_base:
                self.__affiche__erreur(erreur_base, 0, vals, "__migre_product_category write")


    def __migre_product_category(self):
        ''' migration categorie produit '''
        if self.company_id != 1:
            return
        product_category_ids = self.connectionsource.search('product.category', [], 0, 1000000, 'id asc')         
        for product_category_id in product_category_ids:
            self.__create_prod_categ(product_category_id)
                    
    def __migre_product_uom(self):
        ''' migration unite produit '''
        if self.company_id != 1:
            return
        product_uom_ids = self.connectionsource.search('product.uom', [], 0, 1000000, 'id asc')         
        for product_uom_id in product_uom_ids:
            res = self.connectioncible.search('product.uom', [('x_old_id', '=', product_uom_id)])
            if not res :
                vals = self.get_values(product_uom_id, 'product.uom', ['active', 'category_id', 'name', 'rounding', 'factor'])
                vals['x_old_id'] = product_uom_id

                try:
                    product_uom_id = self.connectioncible.create('product.uom', vals)
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, product_uom_id, vals, "__migre_product_uom")

                    
    def __migre_prod_uom_categ(self):
        ''' migration categorie des unites produits ''' 
        if self.company_id != 1:
            return
        product_uom_categ_ids = self.connectionsource.search('product.uom.categ', [], 0, 1000000, 'id asc')         
        for product_uom_categ_id in product_uom_categ_ids:
            res = self.connectioncible.search('product.uom.categ', [('x_old_id', '=', product_uom_categ_id)])
            if not res :
                vals = self.get_values(product_uom_categ_id, 'product.uom.categ')
                vals['x_old_id'] = product_uom_categ_id
                try:
                    product_uom_categ_id = self.connectioncible.create('product.uom.categ', vals)
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, product_uom_categ_id, vals, "__migre_prod_uom_categ") 

                            
    def __migre_product_product(self):
        ''' Migration produit et modele de produit'''
        product_template_ids = self.connectionsource.search('product.template', [], 0, 1000000, 'id asc')         
        for product_template_id in product_template_ids:
            res = self.connectioncible.search('product.template', [('company_id', '=', self.company_id), ('x_old_id', '=', product_template_id)])
            if not res :
                vals = self.get_values(product_template_id, 'product.template', ['x_old_id', 'warranty', 'property_stock_procurement', 'supply_method', 'code', 'list_price', 'weight', 'track_production', 'incoming_qty', 'standard_price',  'uod_id', 'uom_id', 'default_code', 'property_account_income', 'qty_available', 'uos_coeff', 'partner_ref', 'virtual_available',  'purchase_ok', 'track_outgoing', 'company_id', 'product_tmpl_id',  'uom_po_id', 'x_old_id', 'type', 'price', 'track_incoming', 'property_stock_production', 'volume', 'outgoing_qty', 'procure_method', 'property_stock_inventory', 'cost_method', 'price_extra', 'active', 'sale_ok', 'weight_net',  'sale_delay', 'name', 'property_stock_account_output', 'property_account_expense', 'categ_id', 'property_stock_account_input', 'lst_price',  'price_margin'])
                vals['company_id'] = self.company_id
                vals['x_old_id'] = product_template_id
                if vals.has_key('seller_ids'):
                    vals.pop('seller_ids')


                if vals.has_key('uom_id'):
                    vals['uom_po_id'] = vals['uom_id'] 
                    vals['uos_id'] = vals['uom_id']
                try:
                    product_template_id = self.connectioncible.create('product.template', vals)
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, product_template_id, vals, "create migre_product_template")
        
        product_product_ids = self.connectionsource.search('product.product', [('active', 'in', ['true', 'false'])], 0, 1000000, 'id asc')         
        
        for product_product_id in product_product_ids:
            res = self.connectioncible.search('product.product', [('company_id', '=', self.company_id), ('active', 'in', ['true', 'false']), ('x_old_id', '=', product_product_id)])
            if not res :
                vals = self.get_values(product_product_id, 'product.product')
                vals['company_id'] = self.company_id
                vals['x_old_id'] = product_product_id
                for val in ('packaging','pricelist_sale','pricelist_purchase','user_id','seller_ids'):
                    if vals.has_key(val):
                        vals.pop(val)
                if vals.has_key('uom_id'):
                    vals['uom_po_id'] = vals['uom_id'] 
                    vals['uos_id'] = vals['uom_id'] 
                try:
                    product_product_id = self.connectioncible.create('product.product', vals)
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, product_product_id, vals, 'create migre product_product') 

    def __valid_invoice(self, invoice_id=None):
        ''' validation facture '''
        if not invoice_id:
            account_invoice_ids = self.connectionsource.search('account.invoice', [('state', '!=', 'draft')], 0, 1000000, 'id asc')    
        else:
            account_invoice_ids = [invoice_id]
        for account_invoice_id in account_invoice_ids:
            new_id = self.connectioncible.search('account.invoice', [('company_id', '=', self.company_id), ('x_old_id', '=', account_invoice_id)])
            if new_id:
                nbr_lines = self.connectioncible.search('account.invoice.line', [('company_id', '=', self.company_id), ('invoice_id', '=', new_id[0])])
            else:
                nbr_lines = 0
            invoice_read = self.connectioncible.read('account.invoice', new_id)
            if new_id and (len(nbr_lines) > 0) and invoice_read[0]['state'] == 'draft' and invoice_read[0]['internal_number'] != False:
                try:
                    self.connectioncible.object.exec_workflow(self.connectioncible.dbname, self.connectioncible.uid, self.connectioncible.pwd, 'account.invoice', 'invoice_open', new_id[0])
                except BaseException, erreur_base:
                    self.__affiche__erreur(erreur_base, account_invoice_id, invoice_read[0])    


    def parent_store_compute(self):
        ''' recalcule l'arbre des comptes '''
        
        connectstr = "host= %s user=%s  password=%s  dbname=%s" % (self.options.dbhostc, self.options.userdb, self.options.passdb, self.options.dbc)
        conn = psycopg2.connect(connectstr) 
        curseur = conn.cursor()
        _table = 'account_account'
        _parent_name = 'parent_id'
        _parent_order = 'code'
        def browse_rec(root, pos=0):   
            ''' cherche parent '''
            where = _parent_name + '=' + str(root)
            if not root:
                where = _parent_name+' IS NULL'
            if _parent_order:
                where += ' order by '+_parent_order
            curseur.execute('SELECT id FROM '+_table+' WHERE active = True and '+where)
            pos2 = pos + 1
            childs = curseur.fetchall()
            #print 'root:', root, ' -> childs :', childs
            for child_id in childs:
                pos2 = browse_rec(child_id[0], pos2) 
            curseur.execute('update ' + _table + ' set parent_left=%s, parent_right=%s where id=%s', (pos, pos2, root))
            conn.commit()
            return pos2 + 1
        query = 'SELECT id FROM ' + _table + ' WHERE active = True and ' + _parent_name + ' IS NULL'
        if _parent_order:
            query += ' order by ' + _parent_order
        pos = 0
        curseur.execute(query)
        for (root, ) in curseur.fetchall():  
            #print root, pos
            pos = browse_rec(root, pos)    
        curseur.close()
        conn.close()
        return True
