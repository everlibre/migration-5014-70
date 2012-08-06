#!/usr/bin/python0.6
# -*- encoding: utf-8 -*-

import sys
import openerp_connection
import datetime
import time             
import psycopg2

from optparse import OptionParser

def utf(val):
    if type(val) == type(True):    
        str_utf8=""
    elif isinstance(val, str):
        str_utf8 = val
    elif isinstance(val, unicode): 
        str_utf8 = val.encode('utf-8') 
    else:
        str_utf8 = str(val)
    res = str_utf8.replace(";",",").replace('\n','')
    return res

def add_old_id():
    model_ids = connectioncible.search('ir.model', [('model', 'like', 'product%')], 0, 500000)
    for model_id in model_ids:
        search_old = connectioncible.search('ir.model.fields', [('model_id', '=', model_id), ('name', '=', 'x_old_id')])
        if not search_old:
            connectioncible.create('ir.model.fields', {'model_id':model_id, 'name':'x_old_id', 'field_description':'Ancien ID', 'ttype':'integer', 'state':'manual', 'size':64})
    
    model_ids = connectioncible.search('ir.model', [('model', 'like', 'account%')], 0, 500000)
    for model_id in model_ids:
        search_old = connectioncible.search('ir.model.fields', [('model_id', '=', model_id), ('name', '=', 'x_old_id')])
        if not search_old:
            connectioncible.create('ir.model.fields', {'model_id':model_id, 'name':'x_old_id', 'field_description':'Ancien ID', 'ttype':'integer', 'state':'manual', 'size':64})
            
    model_ids = connectioncible.search('ir.model', [('model', 'like', 'res%')], 0, 500000)
    for model_id in model_ids:
        search_old = connectioncible.search('ir.model.fields', [('model_id', '=', model_id), ('name', '=', 'x_old_id')])
        if not search_old:
            connectioncible.create('ir.model.fields', {'model_id':model_id, 'name':'x_old_id', 'field_description':'Ancien ID', 'ttype':'integer', 'state':'manual', 'size':64})

def affiche_erreur(e, sid, vals, function=""):
    fichier = open(options.dbc+".err","a")
    print "old id ", sid
    print "Keys ", utf(vals.keys())
    for key in vals.keys():
        print "vals %s : %s " % (utf(key), utf(vals[key]))
        fichier.write("vals %s : %s \r\n" % (utf(key), utf(vals[key])))
    try:
        print "Erreur",e.message,
        fichier.write(e.message+"\r\n")
    except:
        pass
    try:
        print e.faultCode
        fichier.write(e.faultCode+"\r\n")
    except:
        pass
    try:    
        print e.faultString
        fichier.write(e.faultString+"\r\n")
    except:
        pass
    try:
        print function
        fichier.write(function+"\r\n"+"\r\n")
    except:
        pass
    fichier.close()

def new(sid, model):
    if newtab.has_key(model) and newtab[model].has_key(sid):
        return newtab[model][sid]
    res = connectioncible.search(model, [('x_old_id', '=', sid)])  
    base = connectionsource.dbname
    uid = connectionsource.uid
    pwd = connectionsource.pwd
    fields = connectionsource.object.execute(base, uid, pwd, model, 'fields_get')     
    if not res:
        try:
            if 'active' in fields:
                res = connectioncible.search(model, [('x_old_id', '=', sid), ('active', 'in', ['true', 'false'])])
            else:
                res = connectioncible.search(model, [('x_old_id', '=', sid)])
        except Exception, e :
            affiche_erreur(e, 0, model, 'new')
            pass
    if res:
        res = res[0]
        if not newtab.has_key(model):
            newtab[model] = {}
        newtab[model][sid]=res
    else:
        res = None
        print "Old not exist", model, sid
    return res

def get_values(record_id, model, champs=None):
    res = {}
    #fields = rpc.session.rpc_exec_auth('/object','execute',  model,'fields_get')
    #values =  rpc.session.rpc_exec_auth('/object', 'execute', model, 'read', record_id)
    base = connectionsource.dbname
    uid = connectionsource.uid
    pwd = connectionsource.pwd
    fields = connectionsource.object.execute(base, uid, pwd, model, 'fields_get')
    
    if champs:
        for field in fields.copy():
            if field not in champs:
                fields.pop(field)
            
    if not champs:
        champs = [x for x in fields.keys()]
    if debug:
        print champs
    values = connectionsource.object.execute(base, uid, pwd, model, 'read', record_id, champs)      
    
    for f in fields:
        #print f
        #res[f]={}
        field_type = fields[f]['type'] 
        if field_type in ('text', 'char', 'selection') :      
            if values[f]:
                if unicode(values[f]).strip():
                    try:
                        res[f] = str(values[f])                                                                                                       
                    except:
                        res[f] = unicode(values[f])          
        elif field_type in ('boolean', 'date', 'datetime', 'reference', 'float', 'integer') :
            res[f] = values[f]                                                                                                    
        
        elif field_type == 'many2one' :
            if values[f]:
                res[f] = values[f][0]                                                                                        
        elif field_type == 'one2many'  :
            if values[f]:
                o2m = []
                for o2m_id in values[f]:
                    o2m.append(get_values(o2m_id, fields[f]['relation']))
                res[f] = o2m
        elif field_type == 'many2many'  :
            if values[f]:
                res[f] = [(6, 0, values[f])]
        elif field_type == 'binary' :
            if values[f]:
                res[f] = values[f]         
        else:
            print "Erreur ", model, f, fields[f]['type'], fields[f]
    return res

def etat(name,code):
    if name.strip():
        state_id = connectioncible.search('res.country.state', [('name', 'ilike', name.strip())])
        if not state_id:
            try:
                state_id = connectioncible.create('res.country.state', {'code':code.strip(), 'name':name.strip()})
            except Exception, e:
                affiche_erreur(e, 0, {'code':name.strip(), 'name':name.strip()}, 'creation etat')
                pass
        else:
            state_id = state_id[0]
    else:
        state_id = None
    return state_id    
    
def pays(name):
    if name.strip():
        if name.upper() == "ENGLAND" or name.upper() == "GRANDE-BRETAGNE":
            name = "United Kingdom"
        if name.upper() == "SUISSE" :
            name = "Switzerland"
        country_id = connectioncible.search('res.country', [('name', 'ilike', name.strip())])
        if not country_id:
            country_id = None
            print "name not found ",name
            
            #try:
            #    country_id = connectioncible.create('res.country', {'code':name.strip(), 'name':name.strip()})
            #except Exception, e:
            #    affiche_erreur(e, 0,  {'code':name.strip(), 'name':name.strip()},'pays')
            #    country_id = None
            #    pass
        else:
            country_id = country_id[0]
    else:
        country_id = None
    return country_id

def bank(name):
    if name.strip():
        bank_id = connectioncible.search('res.bank', [('name', 'ilike', name.strip())], context={'lang':'fr_FR'})
        if not bank_id:
                print "Erreur de bank", name
                bank_id = None
        else:
            bank_id = bank_id[0]
    else:
        bank_id = None
    return bank_id

def migre_etat():
    country_state_ids = connectionsource.search('res.country.state', [], 0, 2000)
    for country_state_id in country_state_ids:
        country_state = connectionsource.read('res.country.state', country_state_id)
        val = get_values(country_state_id, 'res.country.state')
        val['x_old_id'] = country_state_id
        country_id = connectioncible.search('res.country', [('name', '=', country_state['country_id'][1])], 0, 1)
        if country_id:
            val['country_id'] = country_id[0]
        state = connectioncible.search('res.country.state', [('name', '=', val['name'])], 0, 2)
        if not state:
            try:
                connectioncible.create('res.country.state', val)
            except Exception, e:
                affiche_erreur(e, 0, val, 'migre_etat')
                pass
        else:
            try:
                connectioncible.write('res.country.state',state[0], val)
            except Exception, e:
                affiche_erreur(e, state, val, 'migre_etat write')
                pass
                
            
def migre_account_payment_term():
    account_payment_term_ids = connectionsource.search('account.payment.term', [('active', 'in', ['true', 'false'])], 0, 2000)
    try :
        connectioncible.unlink('account.payment.term.line', connectioncible.search('account.payment.term.line', [], 0, 2000))
        connectioncible.unlink('account.payment.term', connectioncible.search('account.payment.term', [('active', 'in', ['true', 'false'])], 0, 2000))
    except Exception, e:
        #affiche_erreur(e, 0, {}, 'unlink account_payment_term')
        pass
    for account_payment_term_id in account_payment_term_ids:
        
        val = get_values(account_payment_term_id, 'account.payment.term')
        val['x_old_id'] = account_payment_term_id
        #payment_term = connectioncible.search('account.payment.term', [('name', '=', val['name'])], 0, 1)
        if val.has_key('line_ids'):
            val.pop('line_ids')
        try:
            new_account_payment_term_id = connectioncible.create('account.payment.term', val)
            migre_account_payment_term_line(account_payment_term_id,new_account_payment_term_id)
        except Exception, e:
            affiche_erreur(e, 0, val, 'create migre_account_payment_term')
            pass
        
def migre_account_payment_term_line(old,new):
    account_payment_term_ids = connectionsource.search('account.payment.term.line', [('payment_id','=',old)], 0, 2000)
    for account_payment_term_id in account_payment_term_ids:
        val = get_values(account_payment_term_id, 'account.payment.term.line')
        val['x_old_id'] = account_payment_term_id
        val['payment_id'] = new
        payment_term = connectioncible.search('account.payment.term.line', [('payment_id', '=', new),('name','=',val['name'])], 0, 1)
        if not payment_term:
            try:
                connectioncible.create('account.payment.term.line', val)
            except Exception, e:
                affiche_erreur(e, 0, val, 'create migre_account_payment_term.line')
                pass
        else:
            try:
                connectioncible.write('account.payment.term.line', payment_term, val)
            except Exception, e:
                affiche_erreur(e, 0, val, 'write migre_account_payment_term.line')
                pass
def migre_res_groups():
    groupids = connectionsource.search('res.groups', [], 0, 2000)
    for groupid in groupids:
        val = get_values(groupid, 'res.groups')
        val['x_old_id'] = groupid
        group = connectioncible.search('res.groups', [('name', '=', val['name'])], 0, 1)
        
        for champ in ['users', 'address_id', 'yubi_enable', 'roles_id', 'model_access', 'menu_access']:
            try:
                val.pop(champ)
            except:
                pass
        if val.has_key('lang') and val['lang'] == 'fr_FR':
            val['lang'] = 'fr_CH'
        if val.has_key('context_lang') and val['context_lang'] == 'fr_FR':
            val['context_lang'] = 'fr_CH'
            
        if not group:
            try:
                connectioncible.create('res.groups', val)
            except Exception, e:
                affiche_erreur(e, 0, val, 'create migre_res_grroups')
                pass
        else:
            try:
                connectioncible.write('res.groups', group, val)
            except Exception, e:
                affiche_erreur(e, 0, val, 'write migre_res_grroups')
                pass


def migre_res_users():
    users_ids = connectionsource.search('res.users', [('active', 'in', ['true', 'false'])], 0, 2000)
    for user_id in users_ids:
        val = get_values(user_id, 'res.users')
        val['x_old_id'] = user_id
        user = connectioncible.search('res.users', [('active', 'in', ['true', 'false']), ('login', '=', val['login'])], 0, 1)
        if 'admin' in val['login']:
            val = {}
            val['x_old_id'] = user_id
            user = connectioncible.search('res.users', [('active', 'in', ['true', 'false']), ('login', '=', 'admin')], 0, 1)
            val['password'] = pass_admin_new_base
            try:
                connectioncible.write('res.users', user, val)
            except Exception, e:
                affiche_erreur(e, 0, val, 'write migre_res_users')
                pass

        else:
            for champ in ['yubi_prik', 'yubi_id', 'yubi_pubk','address_id', 'yubi_enable', 'roles_id', 'password']:
                try:
                    val.pop(champ)
                except:
                    pass
            val['password'] = pass_admin_new_base
            if val.has_key('lang') and val['lang'] == 'fr_FR':
                val['lang'] = 'fr_CH'
            if val.has_key('context_lang') and val['context_lang'] == 'fr_FR':
                val['context_lang'] = 'fr_CH'
            if val.has_key('groups_id'):
                group_ids = val['groups_id'][0][2]
                newgroup = []
                for group_id in group_ids:
                    group = connectionsource.read('res.groups', group_id)
                    cible_group_id = connectioncible.search('res.groups', [('name', '=', group['name'])])
                    if cible_group_id[0] not in newgroup:
                        newgroup.append(cible_group_id[0])
                val['groups_id'] = [(6, 0, newgroup)]
            if len(user) == 0:
                try:
                    connectioncible.create('res.users', val)
                except Exception, e:
                    affiche_erreur(e, 0, val, 'create migre_res_users')
                    pass

            else:
                try:
                    connectioncible.write('res.users', user, val)
                except Exception, e:
                    affiche_erreur(e, 0, val, 'write migre_res_users')
                    pass

def migre_partner_title():
    partner_title_ids = connectionsource.search('res.partner.title', [], 0, 2000)
    for partner_title_id in partner_title_ids:
        val = get_values(partner_title_id, 'res.partner.title')
        val['x_old_id'] = partner_title_id
        categ = connectioncible.search('res.partner.title', [('domain', '=', val['domain']), ('name', '=', val['name'])], 0, 1)
        if not val.has_key('shortcut'):
            val['shortcut'] = val['name'][:16]
        if not categ:
            try:
                connectioncible.create('res.partner.title', val)
            except Exception, e:
                affiche_erreur(e, partner_title_id, val, "migre_partner_title")
                pass
        else:
            try:
                connectioncible.write('res.partner.title', categ, val)
            except Exception, e:
                affiche_erreur(e, partner_title_id, val, "migre_partner_title write")
                pass

def migre_res_currency():

    currency_ids = connectionsource.search('res.currency', [], 0, 2000)
    for  currency_id in  currency_ids:
        if not connectioncible.search('res.currency', [('x_old_id', '=', currency_id)], 0, 20000, 'id asc'):
            val = get_values(currency_id, 'res.currency', [  'name', 'rounding', 'rate', 'active', 'accuracy'])
            val['x_old_id'] = currency_id
            
            res = connectioncible.search('res.currency', [('name', '=', val['name'])])
            if not res:
                try:
                    connectioncible.create('res.currency', val)
                except Exception, e:
                    affiche_erreur(e, currency_id, val, "create migre_res_currency")
                    pass
                                
            else:
                try:
                    connectioncible.write('res.currency', [res[0]], val)
                except Exception, e:
                    affiche_erreur(e, currency_id, val, "write migre_res_currency")
                    pass
           
def migre_bank():
    bank_ids = connectionsource.search('res.bank', [], 0, 2000)
    for bank_id in bank_ids:
        val = get_values(bank_id, 'res.bank')
        val['x_old_id'] = bank_id
        for x in ['bvr_zipcity', 'clearing', 'bvr_name', 'bvr_street']:
            try:
                val.pop(x)
            except:
                pass
        bank = connectioncible.search('res.bank', [('name', '=', val['name'])], 0, 2)
        if not bank:
            try:
                if val.has_key('state'):
                    type_res = connectioncible.search('res.partner.bank.type', [('code', '=', val['state'])], 0, 2)
                    if not type_res:
                        val['state'] = connectioncible.search('res.partner.bank.type', [('code', '=', 'bank')], 0, 2)[0]
                connectioncible.create('res.bank', val)
            except Exception, e:
                affiche_erreur(e, bank_id, val, "migre_bank")
                pass
                
def migre_partner_bank_type():
    partner_bank_ids = connectionsource.search('res.partner.bank.type', [], 0, 2000)
    for partner_bank_id in partner_bank_ids:
        val = get_values(partner_bank_id, 'res.partner.bank.type')
        if val.has_key('field_ids'):
            val.pop('field_ids')
        val['x_old_id'] = partner_bank_id
        partner_bank_id = connectioncible.search('res.partner.bank.type', [('name', '=', val['name'])], 0, 2)
        if not partner_bank_id:
            try:
                connectioncible.create('res.partner.bank.type', val)
            except Exception, e:
                affiche_erreur(e, partner_bank_id, val, "migre_partner_bank_type")
                pass
    
def migre_account_journal():
    account_journal_ids = connectionsource.search('account.journal', [('active', 'in', ['true', 'false'])], 0, 20000, 'id asc')         
    for account_journal_id in account_journal_ids:
        val = get_values(account_journal_id, 'account.journal')
        for x in ['active','refund_journal']:
            try:
                val.pop(x)
            except:
                pass
        cle = val.keys()
        cle.sort()

        val['x_old_id'] = account_journal_id
        res_journal =connectioncible.search('account.journal', [('x_old_id', '=', account_journal_id)], 0, 20000, 'id asc')
        company_currency = connectioncible.read('res.company',1,['currency_id'])['currency_id'][0]
        if val.has_key('currency'):
            if val['currency'] <> company_currency:
                val['currency'] = new(val['currency'], 'res.currency')
            else:
                val['currency'] = False
        else:
            val['currency'] = False

        if val.has_key('sequence_id'):
            sequence = connectionsource.read('ir.sequence', val['sequence_id'])
            if sequence:
                sequence_id = connectioncible.search("ir.sequence", [('name', '=', sequence['name'])])
                if sequence_id:
                    val['sequence_id'] = sequence_id[0]
                else:
                    sequence.pop('id')
                    seq_type = connectioncible.search("ir.sequence.type", [('code', '=', sequence['code'])])
                    if not seq_type:
                        connectioncible.create("ir.sequence.type", {'code':sequence['code'], 'name':sequence['code']})
                    val['sequence_id'] = connectioncible.create('ir.sequence', sequence)
                    
        if val.has_key('analytic_journal_id'):
            val['analytic_journal_id'] = new(val['analytic_journal_id'], 'account.analytic.journal')

        if val.has_key('default_debit_account_id') and val['default_debit_account_id']:
            val['default_debit_account_id'] = new(val['default_debit_account_id'], 'account.account')
            try:
                connectioncible.write('account.account', val['default_debit_account_id'], {'currency_id':val['currency']})
            except Exception, e:
                affiche_erreur(e, account_journal_id, val, "write account default_debit_account_id migre_account_journal")
                pass

        if val.has_key('default_credit_account_id') and val['default_credit_account_id']:
            val['default_credit_account_id'] = new(val['default_credit_account_id'], 'account.account')
            try:
                connectioncible.write('account.account', val['default_credit_account_id'], {'currency_id':val['currency']})
            except Exception, e:
                affiche_erreur(e, account_journal_id, val, "write account default_credit_account_id migre_account_journal")
                pass
        if not  val.has_key('company_id'):
            val['company_id'] = 1
        if val.has_key('user_id'):
            val['user_id'] = new(val['user_id'], 'res.users')
        exist_name = connectioncible.search('account.journal', [('name', '=', val['name'])], 0, 20000, 'id asc')
        if exist_name:
            name = connectionsource.search('ir.translation',[('name' , '=','account.journal,name'),('res_id','=',account_journal_id),('src','=',val['name']),('lang','=','fr_FR')])
            if name:
                val['name'] = connectionsource.read('ir.translation',name[0],['value'])['value']
                exist_name = connectioncible.search('account.journal', [('name', '=', val['name'])], 0, 20000, 'id asc')

        exist_code = connectioncible.search('account.journal', [('code', '=', val['code'])], 0, 20000, 'id asc')
        if exist_code:
            journal_cible = connectioncible.read('account.journal',exist_code[0],['name'])
            if journal_cible['name'] <> val['name']:
                val['code']= val['code']+str(account_journal_id)
                exist_code = False
            #val['currency_id'] = False #TODO la devise du journal doit être la devise du compte
            
        if not res_journal :  
            if exist_name:
                try:
                    connectioncible.write('account.journal', exist_name[0], val)
                except Exception, e:
                    affiche_erreur(e, account_journal_id, val, "migre_account_journal write exist name")
                    pass
            elif exist_code:
                try:
                    connectioncible.write('account.journal', exist_code[0], val)
                except Exception, e:
                    affiche_erreur(e, account_journal_id, val, "migre_account_journal write exist code")
                    pass
            else:
                try:
                    connectioncible.create('account.journal', val)   
                except Exception, e:
                    affiche_erreur(e, account_journal_id, val, "migre_account_journal create")
                    pass
        else:
            try:
                val.pop('code')
                val.pop('name')
                connectioncible.write('account.journal', res_journal[0], val)
            except Exception, e:
                affiche_erreur(e, account_journal_id, val, "migre_account_journal write on create")
                pass
                
def migre_analytic_account_journal():
    account_analytic_journal_ids = connectionsource.search('account.analytic.journal', [], 0, 20000, 'id asc')         
    for account_analytic_journal_id in account_analytic_journal_ids:
        vals = get_values(account_analytic_journal_id, 'account.analytic.journal', ['name','code','active','type'])
        vals['x_old_id'] = account_analytic_journal_id
        res = connectioncible.search('account.analytic.journal', [('x_old_id', '=', account_analytic_journal_id)], 0, 20000, 'id asc')
        if not res:
            try:
                connectioncible.create('account.analytic.journal', vals)  
            except Exception, e:
                affiche_erreur(e, account_analytic_journal_id, vals, "migre_account_analytic_journal")
                pass
        else:
            try:
                connectioncible.write('account.analytic.journal', res[0],vals)  
            except Exception, e:
                affiche_erreur(e, account_analytic_journal_id, vals, "migre_account_analytic_journal write")
                pass
            
def migre_account_fiscal_year():
    global current_account_fiscalyear_id
    global current_account_period_ids
    account_fiscalyear_ids = connectionsource.search('account.fiscalyear', [], 0, 11, 'date_stop asc')         
    for account_fiscalyear_id in account_fiscalyear_ids:
       
        valsfiscal = get_values(account_fiscalyear_id, 'account.fiscalyear', ['date_stop', 'code', 'name', 'date_start', 'start_journal_id', 'company_id', 'state', 'end_journal_id'])
        date_du_jour = datetime.datetime.now().strftime('%Y-%m-%d')
        
        if date_du_jour >= valsfiscal['date_start'] and date_du_jour <= valsfiscal['date_stop']:
            current_account_fiscalyear_id = account_fiscalyear_id 
            current_account_period_ids = connectionsource.search('account.period',[('fiscalyear_id','=',current_account_fiscalyear_id)])
            print "Current ",valsfiscal
        valsfiscal['x_old_id'] = account_fiscalyear_id
        valsfiscal['state'] = 'draft'
        if not valsfiscal.has_key('company_id'):
                valsfiscal['company_id']=1
        if valsfiscal.has_key('start_journal_id'):
                valsfiscal['start_journal_id'] = new(valsfiscal['start_journal_id'],'account.journal')
        if valsfiscal.has_key('end_journal_id'):
                valsfiscal['end_journal_id']   = new(valsfiscal['end_journal_id'],'account.journal')
        newfy = connectioncible.search('account.fiscalyear', [('x_old_id', '=', account_fiscalyear_id)], 0, 20000, 'id asc') 
        if not newfy:
            try:
                newfy = connectioncible.create('account.fiscalyear', valsfiscal)
            except Exception, e:
                affiche_erreur(e, account_fiscalyear_id, valsfiscal, "create fiscalyear")
                pass
        else:
            newfy = newfy[0]
        account_period_ids = connectionsource.search('account.period', [('fiscalyear_id', '=', account_fiscalyear_id)], 0, 20000, 'id asc')    
        for account_period in  account_period_ids:
            period_read = connectionsource.read('account.period', account_period)
            res = connectioncible.search('account.period', [('x_old_id', '=', account_period)], 0, 20000, 'id asc')
            if not res:
                
                res = connectioncible.search('account.period', [('date_start', '=', period_read['date_start']), ('date_stop', '=', period_read['date_stop'])], 0, 20000, 'id asc')
            if not res and (period_read['date_start'] >= valsfiscal['date_start'] and period_read['date_stop'] <= valsfiscal['date_stop']):     
                vals = get_values(account_period, 'account.period')
                vals['fiscalyear_id'] = newfy
                vals['state'] = 'draft'
                vals['x_old_id'] = account_period
                try:
                    connectioncible.create('account.period', vals)
                except Exception, e:
                    
                    try:
                        vals['name'] = vals['name'] + vals['date_stop'] .split('-')[0] 
                        vals['code'] = vals['code'] + vals['date_stop'] .split('-')[0] 
                        connectioncible.create('account.period', vals)
                    except Exception, e:
                        affiche_erreur(e,account_period, vals, "migre_account_fiscal_year") 
                        pass
            elif   (period_read['date_start'] >= valsfiscal['date_start'] and period_read['date_stop'] <= valsfiscal['date_stop']):
                vals = {}
                vals['x_old_id'] = account_period
                try:
                    connectioncible.write('account.period', res[0], vals)
                except Exception, e:
                    affiche_erreur(e,account_period, vals, "migre_account_fiscal_year") 
                    pass
                
def migre_account_type():
    account_account_type_ids = connectionsource.search('account.account.type', [], 0, 20000, 'id asc')         
    for account_account_type_id in account_account_type_ids:
        vals = get_values(account_account_type_id, 'account.account.type',['code','name','close_method'])
        vals['x_old_id'] = account_account_type_id
        if not connectioncible.search('account.account.type', [('x_old_id', '=', account_account_type_id)], 0, 20000, 'id asc'):
            connectioncible.create('account.account.type', vals)
    return ""

def create_analytic_account(account_analytic_id):
    
        vals = get_values(account_analytic_id, 'account.analytic.account', ['code', 'contact_id', 'date', 'partner_id', 'user_id', 'date_start', 'company_id', 'parent_id', 'state', 'complete_name', 'description', 'name']
    )
        vals['x_old_id'] = account_analytic_id 
        if vals.has_key('partner_id'):
            vals['partner_id'] = new(vals['partner_id'], 'res.partner')
        if vals.has_key('parent_id'):
            if not connectioncible.search('account.analytic.account', [('x_old_id', '=', vals['parent_id'])]):
                create_analytic_account(vals['parent_id'])
            vals['parent_id'] = new(vals['parent_id'], 'account.analytic.account')
        if vals.has_key('contact_id'):
            vals['contact_id'] = new(vals['contact_id'], 'res.partner.address')
        if vals.has_key('user_id'):
            vals['user_id'] = new(vals['user_id'], 'res.users')
        res = connectioncible.search('account.analytic.account', [('x_old_id', '=', account_analytic_id)])
        if not res:
            try:
                connectioncible.create('account.analytic.account', vals)
            except Exception, e:
                affiche_erreur(e, account_analytic_id, vals, "migre_analytic_account") 
                pass
        else:
            try:
                connectioncible.write('account.analytic.account', res,vals)
            except Exception, e:
                affiche_erreur(e, account_analytic_id, vals, "migre_analytic_account write")
                pass 
            
def migre_analytic_account():
    #TODO : Gastion des taxes
    account_analytic_ids = connectionsource.search('account.analytic.account', [('active', 'in', ['true', 'false'])], 0, 20000, 'parent_id desc')         
    #account_analytic_cible_ids = connectioncible.search('account.analytic.account',[],0,20000,'id desc')
   
    for account_analytic_id in account_analytic_ids:
        create_analytic_account(account_analytic_id)

def create_account(account_id):
        unreconciled_payable = connectioncible.search('account.account.type', [('code', '=', 'payable'), ('close_method', '=', 'unreconciled')], 0, 1, 'id asc')
        unreconciled_receivable = connectioncible.search('account.account.type', [('code', '=', 'receivable'), ('close_method', '=', 'unreconciled')], 0, 1, 'id asc')

        res = connectioncible.search('account.account', [('x_old_id', '=', account_id), ('active', 'in', ['true', 'false'])])
        vals = get_values(account_id, 'account.account', ['code', 'reconcile', 'user_type', 'currency_id', 'company_id', 'shortcut', 'note', 'parent_id', 'type', 'active', 'company_currency_id', 'parent_right', 'name', 'parent_left', 'currency_mode'])
        vals['x_old_id'] = account_id
        vals['user_type'] = new(vals['user_type'], 'account.account.type')
        company_currency = connectionsource.read('res.company',1,['currency_id'])['currency_id'][0]
        if vals.has_key('currency_id') and vals['currency_id'] and vals['currency_id'] <> company_currency:
            vals['currency_id'] = new(vals['currency_id'], 'res.currency')
        else:
            vals['currency_id'] = False
        vals['active'] = True
        if vals['type'] not in  ('view', 'other', 'receivable', 'payable', 'liquidity', 'consolidation', 'closed'):
            vals['type'] = 'other'  			
        if vals['type'] == 'payable' :
            vals['user_type'] = unreconciled_payable[0]
        elif vals['type'] == 'receivable':
            vals['user_type'] = unreconciled_receivable[0]
       
        if vals.has_key('parent_id') and vals['parent_id']:

            resp = connectioncible.search('account.account', [('x_old_id', '=', vals['parent_id']), ('active', 'in', ['true', 'false'])])
            if not resp:
                create_account(vals['parent_id'])		
            vals['parent_id'] = new(vals['parent_id'], 'account.account')
        try:
            #print vals['code'],vals['name'],vals['currency_id']
            if not res:
                exist_code = connectioncible.search('account.account', [('code', '=', vals['code'])], 0, 20000, 'id asc')
                if exist_code:
                    vals['code'] = vals['code'] + "_" + str(len(exist_code) + 1)
                connectioncible.create('account.account', vals)
            else:
                connectioncible.write('account.account', res[0], vals)
        except Exception, e:
            affiche_erreur(e, account_id, vals, "migre_account")
            pass 

def migre_account():
    #TODO : Gastion des taxes
    account_ids = connectionsource.search('account.account', [('active', 'in', ['true', 'false'])], 0, 20000000, 'parent_id asc, id asc')         
    for account_id in account_ids:
        create_account(account_id)
    print u'%s compte(s) migre'%len(account_ids)
    #    connectioncible.execute('account.account', '_parent_store_compute')
            
def migre_account_bank_statement():
    #TODO : Gastion des taxes

    account_bank_statement_ids = connectionsource.search('account.bank.statement', [('period_id','in',current_account_period_ids)], 0, 1000000, 'id asc')         
    nbr = len(account_bank_statement_ids)
    x=0
    for account_bank_statement_id in account_bank_statement_ids:
        res = connectioncible.search('account.bank.statement', [('x_old_id', '=', account_bank_statement_id)])
        if not res :
            vals = get_values(account_bank_statement_id, 'account.bank.statement', ['name', 'state', 'balance_end', 'balance_start', 'journal_id', 'currency', 'period_id', 'date', 'x_old_id', 'balance_end_real'])
            x = x +1
            if (x%100) == 0:
                print "Account bank statement ", x,'/',nbr
            vals['x_old_id'] = account_bank_statement_id
            if vals.has_key('partner_id'):
                vals['partner_id'] = new(vals['partner_id'], 'res.partner')
            if vals.has_key('journal_id'):
                vals['journal_id'] = new(vals['journal_id'], 'account.journal')
            if vals.has_key('period_id'):
                vals['period_id'] = new(vals['period_id'], 'account.period')
            if vals.has_key('currency'):
                vals['currency'] = new(vals['currency'], 'res.currency')
            vals['total_entry_encoding'] = vals['balance_end'] - vals['balance_start']
            vals['state'] = 'draft' 
            try:
                account_bank_statement_id = connectioncible.create('account.bank.statement', vals)
            except Exception, e:
                affiche_erreur(e, account_bank_statement_id, vals, "migre_account_bank_statement")
                pass 
        else:
            vals = connectioncible.read('account.bank.statement', res[0])
            account_bank_statement_id = res[0]
        migre_account_bank_statement_line(account_bank_statement_id)             

def migre_account_bank_statement_line(account_bank_statement_line_id):
    account_bank_statement_line_ids = connectionsource.search('account.bank.statement.line', [('statement_id', '=', account_bank_statement_line_id)], 0, 1000000, 'id asc')         
    for account_bank_statement_line_id in account_bank_statement_line_ids:
        res = connectioncible.search('account.bank.statement.line', [('x_old_id', '=', account_bank_statement_line_id)])
        if not res :
            
            #debug=True
            vals = get_values(account_bank_statement_line_id, 'account.bank.statement.line', [ 'statement_id', 'type', 'account_id',  'amount', 'date', 'x_old_id', 'partner_id', 'name'])
            
            vals['x_old_id'] = account_bank_statement_line_id
            if vals.has_key('partner_id'):
                vals['partner_id'] = new(vals['partner_id'], 'res.partner')
            if vals.has_key('account_id'):
                vals['account_id'] = new(vals['account_id'], 'account.account')
            try:
                if vals.has_key('account_id'):
                    account_bank_statement_line_id = connectioncible.create('account.bank.statement.line', vals)
            except Exception, e:
                affiche_erreur(e, account_bank_statement_line_id, vals, "migre_account_bank_statement_line") 
                pass

def migre_account_invoice():
    account_invoice_ids = connectionsource.search('account.invoice', [('period_id','in',current_account_period_ids)], 0, 1000000, 'id asc')         
    nbr = len(account_invoice_ids)
    x = 0
    for account_invoice_id in account_invoice_ids:
        res = connectioncible.search('account.invoice', [('x_old_id', '=', account_invoice_id)])
        #print "invoice ",connectionsource.execute('account.invoice, action, ids, offset, limit, order, context)
        x = x + 1
        
        vals = get_values(account_invoice_id, 'account.invoice', ['period_id', 'move_id', 'date_due', 'check_total', 'payment_term', 'number', 'journal_id', 'currency_id', 'address_invoice_id', 'reference', 'account_id', 'amount_untaxed', 'address_contact_id', 'reference_type', 'company_id', 'amount_tax', 'state', 'type', 'date_invoice', 'amount_total', 'partner_id', 'name', 'create_uid'])
        if (x%100) == 0:
            print "Account invoice :  ",x,'/',nbr
        #print "Account invoice ", vals
        vals['x_old_id'] = account_invoice_id
        if vals.has_key('number'):
            vals['internal_number'] = vals['number']
            vals.pop('number')
        if vals.has_key('partner_id'):
            vals['partner_id'] = new(vals['partner_id'], 'res.partner')
        if vals.has_key('payment_term'):
            vals['payment_term'] = new(vals['payment_term'], 'account.payment.term')
        if vals.has_key('account_id'):
            vals['account_id'] = new(vals['account_id'], 'account.account')
        if vals.has_key('journal_id'):
            vals['journal_id'] = new(vals['journal_id'], 'account.journal')
        if vals.has_key('period_id'):
            vals['period_id'] = new(vals['period_id'], 'account.period')
        else:
            periode = connectioncible.search('account.period', [('special', '=', False), ('date_stop', '>=', vals['date_invoice']), ('date_start', '<=', vals['date_invoice'])])
            if periode:
                vals['period_id'] = periode[0]
#            else:
#                print vals,periode
                
        if vals.has_key('address_invoice_id'):
            vals['address_invoice_id'] = new(vals['address_invoice_id'], 'res.partner.address')
        if vals.has_key('address_contact_id'):
            vals['address_contact_id'] = new(vals['address_contact_id'], 'res.partner.address')
        if vals.has_key('move_id'):
            vals['move_id'] = new(vals['move_id'], 'account.move')
        if vals.has_key('create_uid'):
            vals['user_id'] = new(vals['create_uid'], 'res.users')
        if vals.has_key('number') and vals['number'] == '/':
            vals['number'] = "/" + str(account_invoice_id)
        if not  vals.has_key('number'):
            vals['number'] = "/" + str(account_invoice_id)
        vals['state'] = 'draft' 
        #print "vals",vals
        if not res :
            try:
                new_invoice_id = connectioncible.create('account.invoice', vals)    
                connectioncible.write('account.invoice', new_invoice_id, {'number':vals['number']})  
            except Exception, e :
                affiche_erreur(e, new_invoice_id, vals, "migre_account_invoice create")
                pass
        else:
            try:
                new_invoice_id = res[0]
                connectioncible.write('account.invoice', new_invoice_id, vals)
            except Exception, e :
                affiche_erreur(e, new_invoice_id, vals, "migre_account_invoice write")
                pass
        migre_account_invoice_line(account_invoice_id,new_invoice_id)  
        valide_invoice(account_invoice_id)
    print "%s factures "%nbr
        
def migre_account_invoice_line(invoice_id, new_invoice_id):
    account_invoice_line_ids = connectionsource.search('account.invoice.line', [('invoice_id', '=', invoice_id)], 0, 1000000, 'id asc')         
    for account_invoice_line_id in account_invoice_line_ids:
        res = connectioncible.search('account.invoice.line', [('x_old_id', '=', account_invoice_line_id)])
        #p#rint "Invoice line ",res
        
        vals = get_values(account_invoice_line_id, 'account.invoice.line')
        #print "Account invoice line ", vals.keys()
        #print "Account invoice line ", vals
        if vals.has_key('price_subtotal_incl'):
            vals.pop('price_subtotal_incl')
        if vals.has_key('state'):
            vals.pop('state')
            
        vals['x_old_id'] = account_invoice_line_id
        vals['invoice_id'] = new_invoice_id
        if vals.has_key('invoice_line_tax_id'):
            new_taxe = []
            for taxe in  vals['invoice_line_tax_id'][0][2]:
                new_taxe.append(new(taxe, 'account.tax'))
            vals['invoice_line_tax_id'] = [(6, 0, new_taxe)]
            
        if vals.has_key('product_id'):
            vals['product_id'] = new(vals['product_id'], 'product.product')
        if vals.has_key('partner_id'):
            vals['partner_id'] = new(vals['partner_id'], 'res.partner')
        if vals.has_key('account_id'):
            vals['account_id'] = new(vals['account_id'], 'account.account')
        if vals.has_key('account_analytic_id'):
            vals['account_analytic_id'] = new(vals['account_analytic_id'], 'account.analytic.account')
        if vals.has_key('journal_id'):
            vals['journal_id'] = new(vals['journal_id'], 'account.journal')
        if vals.has_key('period_id'):
            vals['period_id'] = new(vals['period_id'], 'account.period')
        if vals.has_key('address_invoice_id'):
            vals['address_invoice_id'] = new(vals['address_invoice_id'], 'res.partner.address')
        if vals.has_key('address_contact_id'):
            vals['address_contact_id'] = new(vals['address_contact_id'], 'res.partner.address')
        
        if not res :
            try:
                account_invoice_line_id = connectioncible.create('account.invoice.line', vals)      
            except Exception, e :
                affiche_erreur(e, account_invoice_line_id, vals, "migre_account_invoice_line create")
                pass
        else:
            try:
                account_invoice_line_id = res[0]
                connectioncible.write('account.invoice.line', account_invoice_line_id , vals)
            except Exception, e :
                affiche_erreur(e, account_invoice_line_id, vals, "migre_account_invoice_line write")
                pass

def migre_account_move():
    account_move_ids = connectionsource.search('account.move', [('period_id','in',current_account_period_ids)], 0, 1000000)       
    nbr = len(account_move_ids)
    x = 0  
    for account_move_id in account_move_ids:
        res = connectioncible.search('account.move', [('x_old_id', '=', account_move_id)])
        x = x + 1
        vals = get_values(account_move_id, 'account.move', ['ref', 'name', 'state', 'partner_id', 'journal_id', 'period_id', 'date',  'to_check'])
        if (x%100) == 0:
            print "Account Move ",x,'/',nbr 
        vals['x_old_id'] = account_move_id
        if vals.has_key('partner_id'):
            vals['partner_id'] = new(vals['partner_id'], 'res.partner')
        if vals.has_key('journal_id'):
            vals['journal_id'] = new(vals['journal_id'], 'account.journal')
        if vals.has_key('period_id'):
            vals['period_id'] = new(vals['period_id'], 'account.period')
        if vals['name'] == '/':
            #print "New Name"
            vals['name'] = str(account_move_id)
        vals['state'] = 'draft' 
        #print vals
        if not res:
            try:
        #       print "create"
                new_move_id = connectioncible.create('account.move', vals)
            except Exception, e :
                affiche_erreur(e, account_move_id, vals, "migre_account_move ")
                pass
        else:
            new_move_id = res[0]
            try:
                connectioncible.write('account.move', [new_move_id], vals)
            except Exception, e :
                affiche_erreur(e, new_move_id, vals, 'migre account move')
                pass
        migre_account_move_line(account_move_id, new_move_id)  
    print "%s move "%nbr
        
def migre_account_move_line(move_id, new_move_id):
    #TODO : Gastion des taxes
    account_move = connectioncible.read('account.move', new_move_id )  
    account_move_line_ids = connectionsource.search('account.move.line', [('move_id', '=', move_id)], 0, 222000)         
    for account_move_line_id in account_move_line_ids:
        res = connectioncible.search('account.move.line', [('x_old_id', '=', account_move_line_id)])
        vals = get_values(account_move_line_id, 'account.move.line', ['debit', 'credit', 'statement_id', 'currency_id', 'date_maturity', 'invoice', 'partner_id', 'blocked', 'analytic_account_id', 'centralisation', 'journal_id', 'tax_code_id', 'state', 'amount_taxed', 'ref', 'origin_link', 'account_id', 'period_id', 'amount_currency', 'date', 'move_id', 'name', 'tax_amount', 'product_id', 'account_tax_id', 'product_uom_id', 'followup_line_id', 'quantity'])
        vals['x_old_id'] = account_move_line_id
        if vals.has_key('partner_id'):
            vals['partner_id'] = new(vals['partner_id'], 'res.partner')
        if vals.has_key('analytic_account_id'):
            vals['analytic_account_id'] = new(vals['analytic_account_id'], 'account.analytic.account')
        if vals.has_key('tax_code_id'):
            vals['tax_code_id'] = new(vals['tax_code_id'], 'account.tax.code')
        if vals.has_key('account_tax_id'):
            vals['account_tax_id'] = new(vals['account_tax_id'], 'account.tax')
        if vals.has_key('product_id'):
            vals['product_id'] = new(vals['product_id'], 'product.product')
        if vals.has_key('account_id'):
            vals['account_id'] = new(vals['account_id'], 'account.account')
            compte = connectioncible.read('account.account',vals['account_id'] ,['currency_id'])
            
        if vals.has_key('journal_id'):
            vals['journal_id'] = new(vals['journal_id'], 'account.journal')
        if vals.has_key('amount_taxed'):
            vals['tax_amount'] = vals['amount_taxed']
            vals.pop('amount_taxed')
        
        if vals.has_key('period_id'):
            vals['period_id'] = new(vals['period_id'], 'account.period')
            if vals['period_id'] <> account_move['period_id'][0]: ## incoherence de periode entre move et line
                vals['period_id'] = account_move['period_id'][0]
        if vals.has_key('currency_id'):
            vals['currency_id'] = new(vals['currency_id'], 'res.currency')
            if compte and compte['currency_id']:
                if compte['currency_id'] <> vals['currency_id']:
                    try:
                        connectioncible.write('account.account',vals['account_id'] ,{'currency_id':False})
                    except Exception, e :
                        affiche_erreur(e, account_move_line_id, vals, "write account.account currency migre_account_move_line  ")
                        pass
        if vals.has_key('statement_id'):
            vals['statement_id'] = new(vals['statement_id'], 'account.bank.statement')
        if vals.has_key('name'):
            if vals['name'] == '':
                vals['name'] = str(account_move_line_id)
        else:
            vals['name'] = str(account_move_line_id)
        vals['move_id'] = new_move_id
        vals['state'] = 'draft' 
        if not res:
            try:
                
                connectioncible.create('account.move.line', vals)      
            except Exception, e :
                affiche_erreur(e, account_move_line_id, vals, "migre_account_move_line create ")
                pass
        
def migre_res_company():
    res_company_ids = connectionsource.search('res.company', [], 0, 1, 'id asc')         
    for res_company_id in res_company_ids:

        vals = get_values(res_company_id, 'res.company')
        for x in ['bvr_delta_vert', 'bvr_delta_horz', 'bvr_header']:
            try:
                vals.pop(x)
            except:
                pass
        vals['x_old_id'] = res_company_id
        if vals.has_key('partner_id'):
            vals['partner_id'] = new(vals['partner_id'], 'res.partner')
        if vals.has_key('account_id'):
            vals['account_id'] = new(vals['account_id'], 'account.account')
        try:
            res_company_id = connectioncible.write('res.company', 1, vals)
        except Exception, e:
            affiche_erreur(e, res_company_id, vals, "migre_res_company") 
            pass

def migre_res_partner_address(partner_id=None):
    if partner_id:
        res_partner_address_ids = connectionsource.search('res.partner.address', [('partner_id', '=', partner_id)], 0, 99999999, 'id asc')
    else:
        res_partner_address_ids = connectionsource.search('res.partner.address', [], 0, 99999999, 'id asc')         
    for res_partner_address_id in res_partner_address_ids:
        res = connectioncible.search('res.partner.address', [('x_old_id', '=', res_partner_address_id)])
        vals = get_values(res_partner_address_id, 'res.partner.address')
        vals['x_old_id'] = res_partner_address_id
        if vals.has_key('type'):
            if vals['type'] not in ('default','invoice','delivery','contact','other'):
                vals['type'] = 'other'
        if vals.has_key('country_id'):
            pays_address = connectionsource.read('res.country', vals['country_id'])
            vals['country_id'] = pays(pays_address['name'])
        if vals.has_key('state_id'):
            vals['state_id'] = new(vals['state_id'],'res.country.state')
        if vals.has_key('title'):
            title = connectioncible.search('res.partner.title', [('domain', '=', "contact"), ('name', '=', vals['title'])], 0, 2)
            if not title:
                title = connectioncible.search('res.partner.title', [('domain', '=', "contact"), ('shortcut', '=', vals['title'])], 0, 2)
            if title:
                vals['title'] = title[0]
            else:
                vals['title'] = connectioncible.create('res.partner.title', {'domain':'contact','name':vals['title'],'shortcut':vals['title']})

        if vals.has_key('partner_id'):
            vals['partner_id'] = new(vals['partner_id'], 'res.partner')
        
        if res:
            try:
                res_partner_address_id = connectioncible.write('res.partner.address', res[0], vals)
            except Exception, e:
                affiche_erreur(e, res_partner_address_id, vals, "migre_res_partner_address write")
                pass
        else:
            try:
                res_partner_address_id = connectioncible.create('res.partner.address', vals)
            except Exception, e:
                affiche_erreur(e, res_partner_address_id, vals, "migre_res_partner_address create") 
                pass

def cree_account_tax_code(account_tax_code_id):
    res = connectioncible.search('account.tax.code', [('x_old_id', '=', account_tax_code_id)] , 0, 8000, 'id')
    if not res :
        vals = get_values(account_tax_code_id, 'account.tax.code', ['info', 'name', 'sign', 'parent_id', 'notprintable', 'code'])
        vals['x_old_id'] = account_tax_code_id
        if vals.has_key('parent_id'):
            if not  connectioncible.search('account.tax.code', [('x_old_id', '=', vals['parent_id'])] , 0, 8000, 'id'):
                cree_account_tax_code(vals['parent_id'])
            vals['parent_id'] = new(vals['parent_id'], 'account.tax.code')
        try:
            account_tax_code_id = connectioncible.create('account.tax.code', vals)
        except Exception, e:
            affiche_erreur(e, account_tax_code_id, vals, "migre_account_tax_code") 	
            pass

def migre_account_tax_code():
    account_tax_code_ids = connectionsource.search('account.tax.code', [], 0, 1000000, 'id asc')         
    for account_tax_code_id in account_tax_code_ids:
        cree_account_tax_code(account_tax_code_id)

def migre_account_tax():
    account_tax_ids = connectionsource.search('account.tax', [('active', 'in', ['true', 'false'])], 0, 1000000, 'id asc')         
    for account_tax_id in account_tax_ids:
        res = connectioncible.search('account.tax', [('active', 'in', ['true', 'false']), ('x_old_id', '=', account_tax_id)])
        if not res :
            
            #debug=True
            vals = get_values(account_tax_id, 'account.tax', ['ref_base_code_id', 'ref_tax_code_id', 'sequence', 'base_sign', 'child_depend', 'include_base_amount', 'applicable_type', 'company_id', 'tax_code_id', 'python_compute_inv', 'ref_tax_sign', 'type', 'ref_base_sign', 'type_tax_use', 'base_code_id', 'active', 'x_old_id', 'name', 'account_paid_id', 'account_collected_id', 'amount', 'python_compute', 'tax_sign', 'price_include'])
            vals['x_old_id'] = account_tax_id
            if vals.has_key('ref_base_code_id'):
                vals['ref_base_code_id'] = new(vals['ref_base_code_id'], 'account.tax.code')
            if vals.has_key('base_code_id'):
                vals['base_code_id'] = new(vals['base_code_id'], 'account.tax.code')
            if vals.has_key('tax_code_id'):
                vals['tax_code_id'] = new(vals['tax_code_id'], 'account.tax.code')    
            if vals.has_key('ref_tax_code_id'):
                vals['ref_tax_code_id'] = new(vals['ref_tax_code_id'], 'account.tax.code')
            if vals.has_key('account_paid_id'):
                vals['account_paid_id'] = new(vals['account_paid_id'], 'account.account')
            if vals.has_key('account_collected_id'):
                vals['account_collected_id'] = new(vals['account_collected_id'], 'account.account')
            vals['active'] = True
            exist_name = connectioncible.search('account.tax', [('active', 'in', ['true', 'false']), ('name', '=', vals['name'])], 0, 20000, 'id asc')
            if exist_name:
                vals['name'] = vals['name'][:60] + "_" + str(account_tax_id)
                #print vals['name']
            try:
                account_tax_id = connectioncible.create('account.tax', vals)
            except Exception, e:
                affiche_erreur(e, account_tax_id, vals, "migre_account_tax") 
                pass
            
def migre_partenaire():
    partner_ids = connectionsource.search('res.partner', [('active', 'in', ['true', 'false'])], 0, 20000, 'id asc')
    nbr = len(partner_ids)
    x = 0
    for partner_id in partner_ids:
        x = x + 1
        
        partner_cible_id = connectioncible.search('res.partner', [('x_old_id', '=', partner_id), ('active', 'in', ['true', 'false'])], 0, 80)
        if not partner_cible_id:
            #print "partner_id ",partenaire['name']
            val = get_values(partner_id, 'res.partner', ['bank_ids', 'address', 'property_product_pricelist', 'city', 'property_account_payable', 'debit', 'x_old_id', 'vat', 'website', 'customer', 'supplier', 'date', 'active', 'lang', 'credit_limit', 'name', 'country', 'property_account_receivable', 'credit', 'debit_limit', 'category_id'])
            

            if (x%100) == 0:
                print "Partenaire ", x,'/',nbr
            val['x_old_id'] = partner_id
            if val.has_key('vat'):
                try:
                    result_check = connectioncible.object.execute(basecible, connectioncible.uid, connectioncible.pwd, 'res.partner', 'simple_vat_check', 'ch', val['vat'])
                    if result_check == False:
                        val['vat'] = ""
                except:
                    pass
            if val.has_key('property_account_payable'):
                val['property_account_payable'] = new(val['property_account_payable'] , 'account.account')
            if val.has_key('property_account_receivable'):
                val['property_account_receivable'] = new(val['property_account_receivable'] , 'account.account')
            if val.has_key('bank_ids'):
                bank_ids = val['bank_ids']
            else:
                bank_ids = None
            if val.has_key('address'):
                addresses = val['address']
            else:
                addresses = None
#            if val.has_key('events'):
#                events = val['events']
#            else:
#                events = None
            
            if val.has_key('category_id'):
                category_ids = val['category_id'][0][2]
                newcategory = []
                for category_id in category_ids:
                    categ_cible = new(category_id, 'res.partner.category')
                    if categ_cible and categ_cible not in newcategory:
                        newcategory.append(categ_cible)
                val['category_id'] = [(6, 0, newcategory)]
           
            if val.has_key('title'):
                val['title'] = new(val['title'], 'res.partner.title')
              
                    
            if val.has_key('user_id'):
                #user = connectionsource.read('res.users',val['user_id'])
                #user_id = connectioncible.search('res.users',[('name','=',user['name'])])[0]
                val['user_id'] = new(val['user_id'], 'res.users') 
           
            for champ in ['bank_ids', 'events', 'address', 'property_stock_supplier', 'vat_subjected', 'property_stock_customer', 'property_product_pricelist_purchase', 'suid']:
                try:
                    val.pop(champ)
                except:
                    pass
                
            
            if val.has_key('lang') and val['lang'] == 'fr_FR':
                val['lang'] = 'fr_CH'
            
            if partner_id == 1:
                partner_cible_id = [1]
            if partner_cible_id:
                try:
                    connectioncible.write('res.partner', partner_cible_id[0], val)
                except Exception, e :
                    affiche_erreur(e, partner_id, val, "write partner migre_partenaire ")
                    pass
                partner_new_id = partner_cible_id[0]
            else:
                try:
                    partner_new_id = connectioncible.create('res.partner', val)
                except Exception, e:
                    affiche_erreur(e, partner_id, val, "create partner migre_partenaire")
                    pass
            if addresses:
                migre_res_partner_address(partner_id)
            if bank_ids:
                for bank_id in bank_ids:
                    #print bank_id
                    bank_id['partner_id'] = partner_new_id
                    if bank_id.has_key('country_id'):
                        pays_bank = connectionsource.read('res.country', bank_id['country_id'])
                        bank_id['country_id'] = pays(pays_bank['name'])
                    if bank_id.has_key('state_id'):
                        state_bank = connectionsource.read('res.country.state', bank_id['state_id'])
                        bank_id['state_id'] = new(bank_id['state_id'], 'res.country.state')
                        if bank_id.has_key('bank'):
                            banksource = connectionsource.read('res.bank', bank_id['bank'])
                            bank_id['bank'] = bank(banksource['name'])
                        if not bank_id.has_key('acc_number') and bank_id.has_key('iban') :
                            bank_id['acc_number'] = bank_id['iban']
                        if not bank_id.has_key('acc_number') and bank_id.has_key('post_number') :
                            bank_id['acc_number'] = bank_id['post_number']
                        if bank_id.has_key('state'):
                            type_res = connectioncible.search('res.partner.bank.type', [('code', '=', bank_id['state'])], 0, 2)
                            if not type_res:
                                bank_id['state'] = 'bank'
                        connectioncible.create('res.partner.bank', bank_id)

def create_partner_category(partner_category_id):        
        val = get_values(partner_category_id, 'res.partner.category')
        val['x_old_id'] = partner_category_id
        if val.has_key('child_ids'):
            val.pop('child_ids')
        if val.has_key('name'):
            categ = connectioncible.search('res.partner.category', [('name', '=', val['name'])], 0, 2)
        else:
            val['name']='undefined'
       
        if val.has_key('parent_id'):
            if not connectioncible.search('res.partner.category', [('x_old_id', '=', val['parent_id'])]):
                create_partner_category(val['parent_id'])
            val['parent_id'] = new(val['parent_id'], 'res.partner.category')
        if not categ:
            try:
                connectioncible.create('res.partner.category', val)
            except Exception, e:
                affiche_erreur(e, 0, val, "migre_partner_category")
                pass
        else:
            try:
                connectioncible.write('res.partner.category', categ[0], val)
            except Exception, e:
                affiche_erreur(e, 0, val, "migre_partner_category write")
                pass
            
def migre_partner_category():
    partner_category_ids = connectionsource.search('res.partner.category', [], 0, 2000)
    for partner_category_id in partner_category_ids:
        create_partner_category(partner_category_id)
        
def create_product_category(product_category_id):                          
    val = get_values(product_category_id, 'product.category')
    val['x_old_id'] = product_category_id
    if val.has_key('child_id'):
        val.pop('child_id')
    categ = connectioncible.search('product.category', [('name', '=', val['name'])], 0, 2)
   
    if val.has_key('parent_id'):
        if not connectioncible.search('product.category', [('x_old_id', '=', val['parent_id'])]):
            create_product_category(val['parent_id'])
        val['parent_id'] = new(val['parent_id'], 'product.category')
    if not categ:
        try:
            connectioncible.create('product.category', val)
        except Exception, e:
            affiche_erreur(e, 0, val, "migre_product_category")
            pass
    else:
        try:
            connectioncible.write('product.category', categ[0], val)
        except Exception, e:
            affiche_erreur(e, 0, val, "migre_product_category write")
            pass

def migre_product_category():
    product_category_ids = connectionsource.search('product.category', [], 0, 1000000, 'id asc')         
    for product_category_id in product_category_ids:
        create_product_category(product_category_id)
                
def migre_product_uom():
    product_uom_ids = connectionsource.search('product.uom', [], 0, 1000000, 'id asc')         
    for product_uom_id in product_uom_ids:
        res = connectioncible.search('product.uom', [('x_old_id', '=', product_uom_id)])
        if not res :
            
            #debug=True
            vals = get_values(product_uom_id, 'product.uom',['active','category_id','name','rounding','factor'])
            vals['x_old_id'] = product_uom_id
            if vals.has_key('category_id'):
                vals['category_id'] = new(vals['category_id'], 'product.uom.categ')
            try:
                product_uom_id = connectioncible.create('product.uom', vals)
            except Exception, e:
                affiche_erreur(e, product_uom_id, vals, "migre_product_uom")
                pass
                
def migre_product_uom_categ():
    product_uom_categ_ids = connectionsource.search('product.uom.categ', [], 0, 1000000, 'id asc')         
    for product_uom_categ_id in product_uom_categ_ids:
        res = connectioncible.search('product.uom.categ', [('x_old_id', '=', product_uom_categ_id)])
        if not res :
            vals = get_values(product_uom_categ_id, 'product.uom.categ')
            vals['x_old_id'] = product_uom_categ_id
            try:
                product_uom_categ_id = connectioncible.create('product.uom.categ', vals)
            except Exception, e:
                affiche_erreur(e, product_uom_categ_id, vals, "migre_product_uom_categ") 
                pass
                        
def migre_product_product():
    product_template_ids = connectionsource.search('product.template', [], 0, 1000000, 'id asc')         
    for product_template_id in product_template_ids:
        res = connectioncible.search('product.template', [('x_old_id', '=', product_template_id)])
        if not res :
            
            #debug=True
            vals = get_values(product_template_id, 'product.template',['x_old_id','warranty', 'property_stock_procurement', 'supply_method', 'code', 'list_price', 'weight', 'track_production', 'incoming_qty', 'standard_price',  'uom_id', 'default_code', 'property_account_income', 'qty_available', 'uos_coeff', 'partner_ref', 'virtual_available',  'purchase_ok', 'track_outgoing', 'company_id', 'product_tmpl_id',  'uom_po_id', 'x_old_id', 'type', 'price', 'track_incoming', 'property_stock_production', 'volume', 'outgoing_qty', 'procure_method', 'property_stock_inventory', 'cost_method', 'price_extra', 'active', 'sale_ok', 'weight_net',  'sale_delay', 'name', 'property_stock_account_output', 'property_account_expense', 'categ_id', 'property_stock_account_input', 'lst_price',  'price_margin'])
            vals['x_old_id'] = product_template_id
            if vals.has_key('seller_ids'):
                vals.pop('seller_ids')
            if vals.has_key('taxes_id'):
                new_taxe = []
                #print vals['taxes_id']  
                for taxe in  vals['taxes_id'][0][2]:
                   
                    ntaxe = new(taxe, 'account.tax')
                    #print ntaxe,taxe,new_taxe
                    if ntaxe not in new_taxe:
                        
                        new_taxe.append(ntaxe)
                vals['taxes_id'] = [(6, 0, new_taxe)]
            if vals.has_key('uom_po_id'):
                vals['uom_po_id'] = new(vals['uom_po_id'], 'product.uom')
            if vals.has_key('uom_id'):
                vals['uom_id'] = new(vals['uom_id'], 'product.uom')
                vals['uom_po_id'] = vals['uom_id'] 
            if vals.has_key('categ_id'):
                vals['categ_id'] = new(vals['categ_id'], 'product.category')
            if vals.has_key('account_id'):
                vals['account_id'] = new(vals['account_id'], 'account.account')
            try:
                product_template_id = connectioncible.create('product.template', vals)
            except Exception, e:
                affiche_erreur(e, product_template_id, vals, "migre_product_product")
                pass 
    
    
    product_product_ids = connectionsource.search('product.product', [('active', 'in', ['true', 'false'])], 0, 1000000, 'id asc')         
    
    
    for product_product_id in product_product_ids:
        res = connectioncible.search('product.product', [('active', 'in', ['true', 'false']), ('x_old_id', '=', product_product_id)])
        if not res :
            
            #debug=True
            vals = get_values(product_product_id, 'product.product')
            vals['x_old_id'] = product_product_id
            if vals.has_key('packaging'):
                vals.pop('packaging')
            if vals.has_key('user_id'):
                vals.pop('user_id')
            if vals.has_key('categ_id'):
                vals['categ_id'] = new(vals['categ_id'], 'product.category')
            if vals.has_key('seller_ids'):
                vals.pop('seller_ids')
            if vals.has_key('product_tmpl_id'):
                vals['product_tmpl_id'] = new(vals['product_tmpl_id'], 'product.template')
            if vals.has_key('property_account_income'):
                vals['property_account_income'] = new(vals['property_account_income'], 'account.account')
            if vals.has_key('property_account_expense'):
                vals['property_account_expense'] = new(vals['property_account_expense'], 'account.account')
            if vals.has_key('account_id'):
                vals['account_id'] = new(vals['account_id'], 'account.account')
            if vals.has_key('uom_po_id'):
                vals['uom_po_id'] = new(vals['uom_po_id'], 'product.uom')
            if vals.has_key('uom_id'):
                vals['uom_id'] = new(vals['uom_id'], 'product.uom')
                vals['uom_po_id'] = vals['uom_id'] 
            if vals.has_key('supplier_taxes_id'):
                new_taxe = []
                for taxe in  vals['supplier_taxes_id'][0][2]:
                    ntaxe = new(taxe, 'account.tax')
                    if ntaxe not in new_taxe:
                        new_taxe.append(ntaxe)
                vals['supplier_taxes_id'] = [(6, 0, new_taxe)]
            if vals.has_key('taxes_id'):
                new_taxe = []
                for taxe in  vals['taxes_id'][0][2]:
                    ntaxe = new(taxe, 'account.tax')
                    if ntaxe not in new_taxe:
                        new_taxe.append(ntaxe)
                vals['taxes_id'] = [(6, 0, new_taxe)]
            try:
                product_product_id = connectioncible.create('product.product', vals)
            except Exception, e:
                affiche_erreur(e, product_product_id, vals) 
                pass

def valide_invoice(invoice_id=None):
    if not invoice_id:
        account_invoice_ids = connectionsource.search('account.invoice', [('state', '<>', 'draft')], 0, 1000000, 'id asc')    
    else:
        account_invoice_ids = [invoice_id]
    for account_invoice_id in account_invoice_ids:
        new_id = connectioncible.search('account.invoice', [('x_old_id', '=', account_invoice_id)])
        if new_id:
            nbr_lines = connectioncible.search('account.invoice.line', [('invoice_id', '=', new_id[0])])
        else:
            nbr_lines = 0
        invoice_read = connectioncible.read('account.invoice', new_id)
        if new_id and (len(nbr_lines) > 0) and invoice_read[0]['state'] == 'draft' and invoice_read[0]['internal_number'] <> False:
            try:
                connectioncible.object.exec_workflow(connectioncible.dbname, connectioncible.uid, connectioncible.pwd, 'account.invoice', 'invoice_open', new_id[0])
            except Exception, e:
                affiche_erreur(e, account_invoice_id, invoice_read[0])    
                pass

def parent_store_compute():
    connectstr="host= %s user=%s  password=%s  dbname=%s"%(options.hostc,options.userdb,options.passdb,options.dbc)
    conn = psycopg2.connect(connectstr) 
    cr = conn.cursor()
    _table = 'account_account'
    _parent_name = 'parent_id'
    _parent_order= 'code'
    def browse_rec(root, pos=0):   
        where = _parent_name+'='+str(root)
        if not root:
            where = _parent_name+' IS NULL'
        if _parent_order:
            where += ' order by '+_parent_order
        cr.execute('SELECT id FROM '+_table+' WHERE active = True and '+where)
        pos2 = pos + 1
        childs = cr.fetchall()
        #print 'root:', root, ' -> childs :', childs
        for child_id in childs:
            pos2 = browse_rec(child_id[0], pos2) 
        cr.execute('update '+_table+' set parent_left=%s, parent_right=%s where id=%s', (pos,pos2,root))
        conn.commit()
        return pos2+1
    query = 'SELECT id FROM '+_table+' WHERE active = True and '+_parent_name+' IS NULL'
    if _parent_order:
        query += ' order by '+_parent_order
    pos = 0
    cr.execute(query)
    for (root,) in cr.fetchall():  
        #print root, pos
        pos = browse_rec(root, pos)    
    cr.close()
    conn.close()
    return True

def recree_db():
    print u"Creation base %s "%basecible    
    connect_db = openerp_connection.openerp_db(options.protocolec + '://', options.hostc, options.portc)
    
    try:
        connect_db.sock.drop(passadminopenerp, basecible)
        print u"Base drope"
    except:
        pass
    idnewbase = connect_db.sock.create(passadminopenerp, basecible, False, 'fr_CH', pass_admin_new_base)
    x = 0
    print "Creation en cours",
    while x == 0:
        try:
            connect_db.sock.get_progress(passadminopenerp, idnewbase)
        except:
            x = 1
        time.sleep(5)
        print ".",
    print 
    print u"Base %s cree" % basecible


debug = False
parser = OptionParser()
parser.add_option("-a", "--userdb", dest="userdb", default='postgres', help="User Postgres db")
parser.add_option("-C", "--createdb", dest="createdb", default='true', help="Create base cible ?")
parser.add_option("-b", "--passwordb", dest="passdb", default='postgres', help="Password User Postgres db")
parser.add_option("-d", "--dbs", dest="dbs", default='terp', help="Nom de la base source")
parser.add_option("-u", "--users", dest="users", default='terp', help="User Openerp source")
parser.add_option("-w", "--passwds", dest="passwds", default='terp', help="mot de passe Openerp  source")
parser.add_option("-s", "--serveurs", dest="hosts", default='127.0.0.1', help="Adresse  Serveur source")
parser.add_option("-o", "--ports", dest="ports", default='8069', help="port du serveur source")
parser.add_option("-p", "--protocoles", dest="protocoles", default='https', help="protocole http/https source")
parser.add_option("-D", "--dbc", dest="dbc", default='terp', help="Nom de la base cible ")
parser.add_option("-U", "--userc", dest="userc", default='terp', help="User Openerp cible")
parser.add_option("-W", "--passwdc", dest="passwdc", default='terp', help="mot de passe Openerp cible  ")
parser.add_option("-S", "--serveurc", dest="hostc", default='192.168.12.19', help="Adresse  Serveur cible")
parser.add_option("-O", "--portc", dest="portc", default='8090', help="port du serveur cible")
parser.add_option("-P", "--protocolec", dest="protocolec", default='http', help="protocole http/https cible")
(options, args) = parser.parse_args()
module_list = {}
module_ids = []
newtab = {}
Start = datetime.datetime.now()
current_account_fiscalyear_id = 0
current_account_period_ids = []


if options.userc == 'terp':
    options.userc = "admin"
    options.passwdc = "admin"
    
if options.dbc == 'terp':
    options.dbc = options.dbs + "v70"
    
basecible = options.dbc
basesource = options.dbs
passadminopenerp = 'admin'
pass_admin_new_base = 'admin'


print 
print "-"*80
print 
try:
    print  options.protocoles + '://', options.hosts, options.ports, basesource
    connectionsource = openerp_connection.openerp(options.protocoles + '://', options.hosts, options.ports, basesource, options.users, options.passwds)
except:
    print "Connection Source H.S."
    sys.exit()  

try:
    
    if options.createdb == 'true':
        recree_db()
        print "end creation ",datetime.datetime.now()-Start
    connectioncible = openerp_connection.openerp(options.protocolec + '://', options.hostc, options.portc, basecible, 'admin', pass_admin_new_base)
    
    
    if options.createdb == 'true':
        connectioncible.object.execute(connectioncible.dbname, connectioncible.uid, connectioncible.pwd, 'base.module.update', 'create', {})
        connectioncible.object.execute(connectioncible.dbname, connectioncible.uid, connectioncible.pwd, 'base.module.update', 'update_module', [1])   
        
        
        source_module_ids = connectionsource.search('ir.module.module', [('state', '=', 'installed')])
        cible_module_ids = connectioncible.search('ir.module.module', [])
        
        cible_modules = {}
        for cible_module_id in cible_module_ids: 
            cible_module = connectioncible.read('ir.module.module', cible_module_id)
            cible_modules[cible_module['name']] = cible_module_id
            
        source_modules = {}
        for source_module_id in source_module_ids: 
            source_module = connectionsource.read('ir.module.module', source_module_id)
            source_modules[source_module['name']] = source_module_id
            
            
            
        for source_module in source_modules:
            if cible_modules.has_key(source_module):
                cible_module_id = cible_modules[source_module]
                cible_module = connectioncible.read('ir.module.module', cible_module_id)
                if cible_module['state'] <> 'installed':
                    print "installation module %s  " % source_module    
                    connectioncible.object.execute(connectioncible.dbname, connectioncible.uid, connectioncible.pwd, 'ir.module.module', 'button_install', [cible_module_id])         
                    connectioncible.object.execute(connectioncible.dbname, connectioncible.uid, connectioncible.pwd, 'base.module.upgrade', 'upgrade_module', [1])   
                    print "installation module %s ok " % source_module    
            else:
                print "Module       %s inexistant " % source_module
        
        print options.dbc, "end Installation Modules"
        print "-"*80
        print 
    
    actserver_ids = connectioncible.search('ir.actions.server',[],0,1000)
    connectioncible.unlink('ir.actions.server',actserver_ids)
    migre_product_product()
    sys.exit(0)
    print u"Start ajout x_old_id "
    add_old_id()
    print "end ajout X_old_id"
    print options.dbc, u"Start Migration etat"
    migre_etat()
    print options.dbc, "end Migration etat"
    print options.dbc, u"Start Migration groupes"           
    migre_res_groups() 
    print options.dbc, "end Migration groupes"
    print options.dbc, u"Start Migration Utilisateurs"   
    migre_res_users()
    print options.dbc, "end Migration Utilisateurs"   
    print options.dbc, u"Start migration devises"
    migre_res_currency()
    print options.dbc,  "end migration devises"
    print options.dbc, u"Start migration type de compte"
    migre_account_type()
    print options.dbc, "end migration type de compte"
    print options.dbc, u"Start migration compte"
    migre_account()
    print options.dbc, "end migration compte"
    print options.dbc, u"Start migration code de taxe"
    migre_account_tax_code()
    print options.dbc, "end migration code de taxe"
    print options.dbc, u"Start migration taxe"
    migre_account_tax()
    print options.dbc, "end migration taxe"
    print options.dbc, u"Start Migration banque"
    migre_bank()
    print options.dbc, "end Migration banque"
    print options.dbc, u"Start Migration categorie partenaire"
    migre_partner_category()
    print options.dbc, "end Migration categorie partenaire"
    print options.dbc, u"Start Migration titre"
    migre_partner_title()
    print options.dbc, "end Migration titre"
  
    print options.dbc, u"Start Migration partner type banque"
    migre_partner_bank_type()
    print options.dbc, "end Migration partner type banque"
    print options.dbc, u"Start Migration Partenaire"
    migre_partenaire()
    print options.dbc, "end Migration Partenaire"
    print options.dbc, u"Start migration addresses"
    migre_res_partner_address()
    print options.dbc, "end migration addresses"
    print options.dbc, u"Start migration categorie produit"
    migre_product_category()
    print options.dbc, "end migration categorie produit"
    print options.dbc, u"Start migration categorie unite produit"
    migre_product_uom_categ()
    print options.dbc, "end migration categorie unite produit"
    print options.dbc, u"Start migration unite produit"
    migre_product_uom()
    print options.dbc, "end migration unite produit"
    print options.dbc, u"Start migration produit"
    migre_product_product()
    print options.dbc, "end migration produit"
    print options.dbc, "Start migration res company"
    migre_res_company()
    print options.dbc, "end migration res company"
    print options.dbc, u"Start Migration journal analytique"
    migre_analytic_account_journal()
    print options.dbc, "end migration journal analytique"
    print options.dbc, u"Start Migration journaux comptable"
    migre_account_journal()
    print options.dbc, "end migration journaux comptable"
    print options.dbc, u"Start Migration annee fiscale et periode"
    migre_account_fiscal_year()
    print options.dbc, "end de Migration annee fiscale et periode"
    print options.dbc, u"Start migration Compte Analytique"
    migre_analytic_account()
    print options.dbc, "end migration Compte Analytique"
    print options.dbc,"Start recalcul parent"
    parent_store_compute()
    print options.dbc,"end recalcul parent"
    print options.dbc,"Start Migration bank statement"
    migre_account_bank_statement()
    print options.dbc,"end Migration bank statement"
    print options.dbc,"Start Migration account move"
    Start_move = datetime.datetime.now()
    migre_account_move()
    print options.dbc,"end Migration account move ",(datetime.datetime.now() -Start_move)
    print options.dbc,"start migre payment term "
    
    migre_account_payment_term()

    print options.dbc,"end Migration account payment term"
    
    print options.dbc,"start Migration account invoice"
    migre_account_invoice()
    print options.dbc,"end Migration account invoice"
    end = datetime.datetime.now()
    print options.dbc,"end ",end-Start
except Exception, e:
    affiche_erreur(e, 0, {},'Erreur generale')    
    pass
finally:
    connectionsource.logout()

#TODO recncile
#TODO compte client fournisseur pour partenaire
