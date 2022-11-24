# Copyright (c) 2022, abayomi.awosusi@sgatechsolutions.com and contributors
# For license information, please see license.txt

from logging import debug
import frappe
import calendar
import numpy as np
import json
import datetime
#import openpyxl
import csv
import pandas as pd
from frappe import _, scrub
from frappe.utils import add_days, add_to_date, flt, getdate, cint
from six import iteritems
from erpnext.accounts.utils import get_fiscal_year
from erpnext.stock.utils import get_incoming_rate
from erpnext.controllers.queries import get_match_cond
#from openpyxl import load_workbook
from datetime import date
#from datetime import datetime

#
def execute(filters=None):
    return WeeklySales(filters).run()

#
class WeeklySales(object):
    def __init__(self, filters=None):
        self.filters = frappe._dict(filters or {})		
        self.date_field = (
            "transaction_date"
            # if self.filters.doc_type in ["Sales Order", "Purchase Order"]
            # else "posting_date"
        )
        self.months = [
            "Jan",
            "Feb",
            "Mar",
            "Apr",
            "May",
            "Jun",
            "Jul",
            "Aug",
            "Sep",
            "Oct",
            "Nov",
            "Dec",
        ]		
        self.get_period_date_ranges()
        #print("c1")

    def run(self):		
        #self.filters.tree_type="Cost Center"
        self.get_columns()
        self.get_data()
    
        # Skipping total row for tree-view reports
        skip_total_row = 0

    
        return self.columns, self.data, None, None, skip_total_row

    def get_columns(self):
        self.columns = [
            {
                "label": self.filters.cost_center,				
                "fieldname": "cost_center",
                "fieldtype": "data",
                "width": 140,
                "hidden":1
            }
        ]
        self.columns.append(
                {
                    "label": "Backlog", 
                    "fieldname": "Backlog", 
                    "fieldtype": "data",
                     "width": 120
                }
            )
        for end_date in self.periodic_daterange:
            period = self.get_period(end_date)					
            self.columns.append(
                {
                    "label": _(period), 
                    "fieldname": scrub(period), 
                    "fieldtype": "Float",
                     "width": 120
                }
            )
        self.columns.append(
            {"label": _("Total"), "fieldname": "total", "fieldtype": "Float", "width": 120}
        )

    def get_data(self):				
        #if self.filters.tree_type == "Cost Center":			
        self.get_sales_transactions_based_on_cost_center()			
        self.get_rows()			

        value_field = "base_net_total as value_field"
        # if self.filters["value_quantity"] == "Value":
        # 	value_field = "base_net_total as value_field"
        # else:
        # 	value_field = "total_qty as value_field"

        entity = "project as entity"	
        # self.entries = frappe.get_all(
        # 	self.filters.cost_center,
        # 	fields=[entity, value_field, self.date_field],
        # 	filters={
        # 		"docstatus": 1,
        # 		"company": self.filters.cost_center,
        # 		"project": ["!=", ""],				
        # 	},
        # )
        
    def get_sales_transactions_based_on_cost_center(self):			
        value_field = "base_amount"	
        #print(self.filters.cost_center)	
        # self.entries = frappe.db.sql(
        # 	"""
        # 	(select distinct `tabSales Order`.grand_total,`tabSales Order Item`.item_group as entity,`tabSales Order`.cost_center, `tabSales Order`.name, 
        # 	`tabSales Order Item`.base_amount as value_field,`tabSales Order`.transaction_date,
        # 	`tabSales Order`.customer_name, `tabSales Order`.status,`tabSales Order`.delivery_status, 
        # 	`tabSales Order`.billing_status,`tabSales Order Item`.delivery_date from `tabSales Order`, 
        # 	`tabSales Order Item` where `tabSales Order`.name = `tabSales Order Item`.parent and 
        # 	`tabSales Order`.status <> 'Cancelled' and  `tabSales Order`.cost_center = %(cost_center)s
        # 	and `tabSales Order Item`.delivery_date <=  %(to_date)s
        # 	)	
        # """, {
        # 		'cost_center': self.filters.cost_center,'from_date': self.filters.start_date ,'to_date':  self.filters.to_date				
        # 	},		
        # 	as_dict=1,
        # )	
        #print("c5")
        #self.get_groups()
        if self.filters.cost_center:		
            self.entries = frappe.db.sql(
            """
            (select distinct s.cost_center as entity, i.base_amount as value_field, s.transaction_date
            from `tabSales Order` s,`tabSales Order Item` i 
            where s.name = i.parent and 
            s.status <> 'Cancelled' and  s.cost_center IN %(cost_center)s 
            and i.delivery_date <=  %(to_date)s
            )	
            """, {
                    'cost_center': self.filters.cost_center,'from_date': self.filters.start_date ,'to_date':  self.filters.to_date				
                },		
                as_dict=1,
            )
        else:
            self.entries = frappe.db.sql(
            """
            (select distinct s.cost_center as entity, i.base_amount as value_field, s.transaction_date
            from `tabSales Order` s,`tabSales Order Item` i 
            where s.name = i.parent and 
            s.status <> 'Cancelled' and  s.cost_center = %(cost_center)s 
            and i.delivery_date <=  %(to_date)s
            )	
            """, {
                    'cost_center': '','from_date': self.filters.start_date ,'to_date':  self.filters.to_date				
                },		
                as_dict=1,
            )    	

    def get_rows(self):
        self.data = []		
        self.get_periodic_data()	
        self.get_period_rowweek_ranges()		
        for entity, period_data in iteritems(self.entity_periodic_data):	
            
            row = {
                "entity": entity,
                "entity_name": self.entity_names.get(entity) if hasattr(self, "entity_names") else None,
            }				
            total = 0
            for end_date in self.week_periodic_daterange:				
                period = self.get_weekperiod(end_date)
                amount = flt(period_data.get(period, 0.0))
                row[scrub(period)] = amount
                total += amount

            row["total"] = total	

            self.data.append(row)
    def get_period_rowweek_ranges(self):
        from dateutil.relativedelta import MO, relativedelta

        from_date, to_date = getdate(self.filters.from_date), getdate(self.filters.to_date)

        increment = {"Monthly": 1, "Quarterly": 3, "Half-Yearly": 6, "Yearly": 12}.get(
            self.filters.range, 1
        )

        if self.filters.range in ["Monthly", "Quarterly","Weekly"]:
            from_date = from_date.replace(day=1)
        elif self.filters.range == "Yearly":
            from_date = get_fiscal_year(from_date)[1]
        else:
            from_date = from_date + relativedelta(from_date, weekday=MO(-1))

        self.week_periodic_daterange = []
        for dummy in range(1, 53):
            if self.filters.range == "Weekly":
                period_end_date = add_days(from_date, 6)
            else:
                period_end_date = add_to_date(from_date, months=increment, days=-1)

            if period_end_date > to_date:
                period_end_date = to_date

            self.week_periodic_daterange.append(period_end_date)

            from_date = add_days(period_end_date, 1)
            if period_end_date == to_date:
                break
    
    def get_periodic_data(self):
        self.entity_periodic_data = frappe._dict()		
        if self.filters.range == "Weekly":
            for d in self.entries:				
                period = self.get_weekperiod(d.get(self.date_field))				
                self.entity_periodic_data.setdefault(d.entity, frappe._dict()).setdefault(period.split('@')[0], 0.0)
                self.entity_periodic_data[d.entity][period.split('@')[0]] += flt(d.value_field)

                # if self.filters.tree_type == "Item":
                # 	self.entity_periodic_data[d.entity]["stock_uom"] = d.stock_uom

    def get_period(self, posting_date):			
        calendar.setfirstweekday(5)
        if self.filters.range == "Weekly":
            mnthname= posting_date.strftime('%b')
            x = np.array(calendar.monthcalendar(posting_date.year, posting_date.month)) 
            week_of_month = np.where(x == posting_date.day)[0][0] + 1			
            #period = "Week " + str(posting_date.isocalendar()[1]) + " "+ mnthname +" "+ str(posting_date.year)
            period = mnthname +"-"+ str(posting_date.year)[-2:]			
            # elif self.filters.range == "Monthly":
            # 	period = str(self.months[posting_date.month - 1]) + " " + str(posting_date.year)
            # elif self.filters.range == "Quarterly":
            # 	period = "Quarter " + str(((posting_date.month - 1) // 3) + 1) + " " + str(posting_date.year)
            # else:
            # 	year = get_fiscal_year(posting_date, company=self.filters.company)
            # 	period = str(year[0])		
        return period

    def get_weekperiod(self, posting_date):
        calendar.setfirstweekday(5)
        if self.filters.range == "Weekly":
            mnthname= posting_date.strftime('%b')
            x = np.array(calendar.monthcalendar(posting_date.year, posting_date.month)) 
            week_of_month = np.where(x == posting_date.day)[0][0] + 1			
            #period = "Week " + str(posting_date.isocalendar()[1]) + " "+ mnthname +" "+ str(posting_date.year)			
            weekperiod= "Week " + str(week_of_month) +"@"+mnthname +"-"+ str(posting_date.year)[-2:]	
        return weekperiod	

    #for setting column month or week wise
    def get_period_date_ranges(self):
        from dateutil.relativedelta import MO, relativedelta

        from_date, to_date = getdate(self.filters.from_date), getdate(self.filters.to_date)

        increment = {"Monthly": 1, "Quarterly": 3, "Half-Yearly": 6, "Yearly": 12}.get(
            self.filters.range, 1
        )
        
        if self.filters.range in ["Monthly", "Quarterly","Weekly"]:
            from_date = from_date.replace(day=1)
        elif self.filters.range == "Yearly":
            from_date = get_fiscal_year(from_date)[1]
        else:
            from_date = from_date + relativedelta(from_date, weekday=MO(-1))

        self.periodic_daterange = []
        for dummy in range(1, 53):
            if self.filters.range == "Week":
                period_end_date = add_days(from_date, 6)
            else:
                period_end_date = add_to_date(from_date, months=increment, days=-1)

            if period_end_date > to_date:
                period_end_date = to_date

            self.periodic_daterange.append(period_end_date)

            from_date = add_days(period_end_date, 1)
            if period_end_date == to_date:
                break
    
sales_allrecord=[]
@frappe.whitelist()
def get_weekly_report_record(report_name,filters):
    from dateutil.relativedelta import MO, relativedelta
    # Skipping total row for tree-view reports
    skip_total_row = 0
    #return self.columns, self.data, None, None, skip_total_row
    

    filterDt= json.loads(filters)	
    filters = frappe._dict(filterDt or {})	
    
    if filters.to_date:
        end_date= filters.to_date
    else:
        end_date= date.today()
    
    fiscalyeardt= fetchselected_fiscalyear(end_date)
    for fy in fiscalyeardt:
        start_date=fy.get('year_start_date').strftime('%Y-%m-%d')
        fiscal_endDt=fy.get('year_end_date').strftime('%Y-%m-%d')
        fiscalyr=fy.get('year')

    filters.update({"fiscal_endDt":fiscal_endDt})
    filters.update({"from_date":start_date})
    filters.update({"fiscalyr":fiscalyr})
    
    #######
    coycostcenters,coycostcenternos = getcostcenters(filters)
    
    fiscalyeardtprev, prevyrsstartdate = fetch5yrsback_fiscalyear(5,filters)
    
    #
    compnyName=""
    if filters.cost_center:	
        
        currdat = start_date
        sales_allrecord = [] #frappe._dict() 
        i=0
        firstdayislastwkday = 0
        #flagfirstdaypass = 0
        #currdat,itsdlastday,firstdayislastwkday = getwkenddate(currdat,flagfirstdaypass)
        while currdat <= filters.to_date:
            flagfirstdaypass = 0
            if (firstdayislastwkday == 1):
                flagfirstdaypass = 1
            currdat,itsdlastday,firstdayislastwkday = getwkenddate(currdat,flagfirstdaypass)
            sales_recssubset = getsalesbacklogforweek(currdat,filters)
            
            if (itsdlastday == 1):
                currdat = add_to_date(currdat,days=1)

            #year_total_list = frappe._dict()	
            for dd in sales_recssubset:		
            #print(dd)
                conrec = []
                conrec.append('Consolidated')
                conrec.append(dd[1])
                conrec.append(dd[2])
                conrec.append(dd[3])
                sales_allrecord.append(conrec)
                sales_allrecord.append(dd)
                ftch_cmpny=dd[2]
                compnyName=ftch_cmpny
                
        
        min_date_backloglst = []
        #
        for fy3 in fiscalyeardtprev: 
            fyr = fy3.year
            fsd = fy3.year_start_date
            fed = fy3.year_end_date
            currdt = fy3.year_start_date
        
            i = 1
            while ((i < 13) and (currdt < fed)):
                #date_time_obj = datetime.datetime.strptime(currdt, '%Y-%m-%d')
                mth_end_day = calendar.monthrange(currdt.year,currdt.month)[1]
                mth_end_date = datetime.date(currdt.year, currdt.month, mth_end_day)
                #print(mth_end_date)
                sales_recssubset2 = getsalesbacklogforyr(mth_end_date,filters)
                i += 1
                currdt2 = currdt + relativedelta(months=+1)
                currdt = currdt2
                
                for dd in sales_recssubset2:		
                    conrec = []
                    conrec.append(dd[0])
                    conrec.append(dd[1])
                    conrec.append(dd[2])
                    conrec.append('Consolidated')
                    min_date_backloglst.append(conrec)
                    min_date_backloglst.append(dd)
        
    else:
        currdat = start_date
        sales_allrecord = [] #frappe._dict() 
        i=0
        firstdayislastwkday = 0
        #flagfirstdaypass = 0
        #currdat,itsdlastday,firstdayislastwkday = getwkenddate(currdat,flagfirstdaypass)

        while currdat <= filters.to_date:
            flagfirstdaypass = 0
            if (firstdayislastwkday == 1):
                flagfirstdaypass = 1
            currdat,itsdlastday,firstdayislastwkday = getwkenddate(currdat,flagfirstdaypass)
            print(str(currdat) + " - " + str(itsdlastday)+ " - " + str(firstdayislastwkday))
            sales_recssubset = getsalesbacklogforweek(currdat,filters)

            

            if (itsdlastday == 1):
                currdat = add_to_date(currdat,days=1)    
            
            
            #year_total_list = frappe._dict()	
            for dd in sales_recssubset:		
            #print(dd)
                conrec = []
                conrec.append('Consolidated')
                conrec.append(dd[1])
                conrec.append(dd[2])
                conrec.append(dd[3])
                sales_allrecord.append(conrec)
                sales_allrecord.append(dd)
                ftch_cmpny=dd[2]
                compnyName=ftch_cmpny
                
        min_date_backloglst = []
        #
        for fy3 in fiscalyeardtprev: 
            fyr = fy3.year
            fsd = fy3.year_start_date
            fed = fy3.year_end_date
            currdt = fy3.year_start_date
        
            i = 1
            while ((i < 13) and (currdt < fed)):
                #date_time_obj = datetime.datetime.strptime(currdt, '%Y-%m-%d')
                mth_end_day = calendar.monthrange(currdt.year,currdt.month)[1]
                mth_end_date = datetime.date(currdt.year, currdt.month, mth_end_day)
                #print(mth_end_date)
                sales_recssubset2 = getsalesbacklogforyr(mth_end_date,filters)
                i += 1
                currdt2 = currdt + relativedelta(months=+1)
                currdt = currdt2
                
                for dd in sales_recssubset2:		
                    conrec = []
                    conrec.append(dd[0])
                    conrec.append(dd[1])
                    conrec.append(dd[2])
                    conrec.append('Consolidated')
                    min_date_backloglst.append(conrec)
                    min_date_backloglst.append(dd)
                
       
    year_total_list = frappe._dict()	
    
    
    # check through all cost centers and prev yrs and see missing months and year and initialize to zero
    year_total_list2 = frappe._dict()
    for fy3 in fiscalyeardtprev: 
        fyr = fy3.year
        fsd = fy3.year_start_date
        fed = fy3.year_end_date
        currdt = fy3.year_start_date
        bkltotamt = 0.0
        #for x in range(1, 12):
        i = 1
        while ((i < 13) and (currdt < fed)):
            i += 1
            #print(currdt)
            mthyrstr = currdt.strftime("%b") + "-" + fyr[-2:]
            #print(mthyrstr)
            #take care of consolidated cost center
            #loop through all cost centers
            consolidatedcc = 'Consolidated'
            ccTotalAmt0 = 0
            ##for dd in min_date_backlog:
            for dd in min_date_backloglst:
                if ((dd[3]==consolidatedcc) and (dd[0]==mthyrstr) and (dd[1]==fyr)):
                    ccTotalAmt0 += dd[2]
            year_total_list2.setdefault(consolidatedcc, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(mthyrstr,ccTotalAmt0)
            year_total_list2[consolidatedcc][fyr][mthyrstr] += flt(ccTotalAmt0)    
                #if ((dd.cost_center==consolidatedcc) and (dd.Date==mthyrstr) and (dd.year==fyr)):
                #    ccTotalAmt0 = dd.TotalAmt
            #year_total_list2.setdefault(consolidatedcc, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(mthyrstr,ccTotalAmt0)
            #year_total_list2[consolidatedcc][fyr][mthyrstr] += flt(ccTotalAmt0)
            #
            if filters.cost_center:
                ccc = filters.cost_center
                for cc in ccc:
                    ccTotalAmt = 0
                    for dd in min_date_backloglst:    
                        if ((dd[3]==cc) and (dd[0]==mthyrstr) and (dd[1]==fyr)):
                            ccTotalAmt = dd[2]
                    year_total_list2.setdefault(cc, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(mthyrstr,ccTotalAmt)
                    year_total_list2[cc][fyr][mthyrstr] += flt(ccTotalAmt)            
            else:
                for cc in coycostcenters:
                    ccTotalAmt = 0
                    #for dd in min_date_backlog:
                    #for dd in min_date_backlog:    
                    #    if ((dd.cost_center==cc) and (dd.Date==mthyrstr) and (dd.year==fyr)):
                    #        ccTotalAmt = dd.TotalAmt
                    #year_total_list2.setdefault(cc, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(mthyrstr,ccTotalAmt)
                    #year_total_list2[cc][fyr][mthyrstr] += flt(ccTotalAmt)
                    for dd in min_date_backloglst:    
                        if ((dd[3]==cc) and (dd[0]==mthyrstr) and (dd[1]==fyr)):
                            ccTotalAmt = dd[2]
                    year_total_list2.setdefault(cc, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(mthyrstr,ccTotalAmt)
                    year_total_list2[cc][fyr][mthyrstr] += flt(ccTotalAmt)

            currdt2 = currdt + relativedelta(months=+1)
            currdt = currdt2


    #print(year_total_list2)        
    
    #year_lis = list(year_total_list.items())  #convert dict to list
    year_lis = list(year_total_list2.items())
    
    WSobj = WeeklySales()
    WSobj.__init__()	
    #compnyName=""	
    #if sales_allrecord:
    #    ftch_cmpny = {entry.get('company') for entry in sales_allrecord}		
    #    compnyName=ftch_cmpny
        
    Cust_periodic_daterange=cust_get_period_date_ranges(filters)
    Cust_colum_name=cust_get_columns(filters,Cust_periodic_daterange)
    #print(Cust_colum_name)	
    #Cust_rows_values=cust_get_rows(filters,sales_allrecord,Cust_periodic_daterange)
    Cust_rows_values=cust_get_rows_forallweeks(filters,sales_allrecord,Cust_periodic_daterange,coycostcenters,start_date,fiscal_endDt)
    #print(Cust_rows_values)
    combined_list=[]
    combined_list.append((list(Cust_rows_values), year_lis))
    ############
    # call the sales section
    cust_salescolum_name=cust_getsales_columns(filters,Cust_periodic_daterange)
    wksalesdata,yrsalesdata = get_weeklysales_report_record(filters,start_date,fiscal_endDt,coycostcenters,fiscalyeardtprev,coycostcenternos)
    wksalesdatalst = cust_get_sales_rows_forallweeks(filters,wksalesdata,coycostcenters,start_date,fiscal_endDt)
    combinedsales_list=[]
    combinedsales_list.append((list(wksalesdatalst),yrsalesdata))
    #print(combinedsales_list)
    #combinedsales_list.append(wksalesdata)
    #combinedsales_list.append(yrsalesdata)
    
    #data = get_merged_dataongrossprofls(filters,gross_profit_data.si_list)

    return Cust_colum_name,combined_list,compnyName,fiscal_endDt,cust_salescolum_name,combinedsales_list	

#

def getsalesbacklogforweek(week_end_date,filters):
    retsql = ""
    if (filters.cost_center) :
        cc = filters.cost_center
        sales_allrecordm = frappe.db.sql(
                """
                select X.*
                from (
                select s.cost_center as entity, sum(i.base_amount) as value_field, s.company, STR_TO_DATE(%(to_date)s,'%%Y-%%m-%%d') as endofweekdate, WEEK(%(to_date)s) as weekno
                from `tabSales Order` s inner join `tabSales Order Item` i 
                 on s.name = i.parent left join `tabDelivery Note Item` b on 
                s.name = b.against_sales_order and b.so_detail = i.name left join `tabDelivery Note` a 
                on b.parent = a.name 
                where s.transaction_date <= %(to_date)s 
                and s.status <> 'Cancelled' and s.cost_center IN %(cost_center)s
                and ((a.posting_date > %(to_date)s) or (a.posting_date IS NULL))
                GROUP BY s.cost_center, s.company, STR_TO_DATE(%(to_date)s,'%%Y-%%m-%%d'), WEEK(%(to_date)s)
                    ) X
                """, {
                        'to_date': week_end_date,'cost_center': cc				
                    },		
                    as_dict=0,
                )
    else :
        sales_allrecordm = frappe.db.sql(
                """
                select X.*
                from (
                select s.cost_center as entity, sum(i.base_amount) as value_field, s.company, STR_TO_DATE(%(to_date)s,'%%Y-%%m-%%d') as endofweekdate, WEEK(%(to_date)s) as weekno
                from `tabSales Order` s inner join `tabSales Order Item` i 
                 on s.name = i.parent left join `tabDelivery Note Item` b on 
                s.name = b.against_sales_order and b.so_detail = i.name left join `tabDelivery Note` a 
                on b.parent = a.name 
                where s.transaction_date <= %(to_date)s 
                and s.status <> 'Cancelled' 
                and ((a.posting_date > %(to_date)s) or (a.posting_date IS NULL))
                GROUP BY s.cost_center, s.company, STR_TO_DATE(%(to_date)s,'%%Y-%%m-%%d'), WEEK(%(to_date)s)
                    ) X
                """, {
                        'to_date': week_end_date				
                    },		
                    as_dict=0,
                )

    return sales_allrecordm


def getsalesbacklogforyr(month_end_date,filters):
    retsql = ""
    if (filters.cost_center) :
        cc = filters.cost_center
        min_date_backlog = frappe.db.sql(
                """
                select M.*
                from (
                select CONCAT(DATE_FORMAT(STR_TO_DATE(%(to_date)s,'%%Y-%%m-%%d'), %(b)s),"-", RIGHT(fy.year,2)) as Date,
                fy.year as year,
                sum(i.base_amount) AS TotalAmt, s.cost_center
                from `tabSales Order` s inner join  
                `tabSales Order Item` i on s.name = i.parent
                inner join `tabFiscal Year` fy on %(to_date)s >= fy.year_start_date and %(to_date)s <= fy.year_end_date
                left join `tabDelivery Note Item` b on s.name = b.against_sales_order and b.so_detail = i.name left join `tabDelivery Note` a on b.parent = a.name  
                where s.status <> 'Cancelled' and s.cost_center IN %(cost_center)s 
                and s.transaction_date <= %(to_date)s 
                and ((a.posting_date > %(to_date)s) or (a.posting_date IS NULL))
                group by CONCAT(DATE_FORMAT(STR_TO_DATE(%(to_date)s,'%%Y-%%m-%%d'), %(b)s),"-", RIGHT(fy.year,2)), fy.year,s.cost_center					
                    ) M 						
                """, {
                       'to_date': month_end_date,'b':'%b','cost_center': cc				
                     },		
                       as_dict=0,
                   )
    else :
        min_date_backlog = frappe.db.sql(
                """
                select M.*
                from (
                select CONCAT(DATE_FORMAT(STR_TO_DATE(%(to_date)s,'%%Y-%%m-%%d'), %(b)s),"-", RIGHT(fy.year,2)) as Date,
                fy.year as year,
                sum(i.base_amount) AS TotalAmt, s.cost_center
                from `tabSales Order` s inner join  
                `tabSales Order Item` i on s.name = i.parent
                inner join `tabFiscal Year` fy on %(to_date)s >= fy.year_start_date and %(to_date)s <= fy.year_end_date
                left join `tabDelivery Note Item` b on s.name = b.against_sales_order and b.so_detail = i.name left join `tabDelivery Note` a on b.parent = a.name  
                where s.status <> 'Cancelled' 
                and s.transaction_date <= %(to_date)s 
                and ((a.posting_date > %(to_date)s) or (a.posting_date IS NULL))
                group by CONCAT(DATE_FORMAT(STR_TO_DATE(%(to_date)s,'%%Y-%%m-%%d'), %(b)s),"-", RIGHT(fy.year,2)), fy.year,s.cost_center					
                    ) M 						
                """, {
                       'to_date': month_end_date,'b':'%b'				
                     },		
                       as_dict=0,
                   )                 

    return min_date_backlog    


def cust_get_columns(filters,Cust_periodic_daterange):
    cust_columns=[
            {
                "label": "Backlog", 
                "fieldname": "Backlog", 
                "fieldtype": "data",
                    "width": 120
            }
        ]
    for end_date in Cust_periodic_daterange:
        period = cust_get_period(end_date,filters)							
        cust_columns.append(
            {
                "label": _(period), 
                "fieldname": scrub(period), 
                "fieldtype": "Float",
                    "width": 120
            }
        )
    # cust_columns.append(
    # 	{"label": _("Total"), "fieldname": "total", "fieldtype": "Float", "width": 120}
    # )
    #print("c14")
    return cust_columns

def cust_getsales_columns(filters,Cust_periodic_daterange):
    cust_columns=[
            {
                "label": "Sales", 
                "fieldname": "Sales", 
                "fieldtype": "data",
                    "width": 120
            }
        ]
    for end_date in Cust_periodic_daterange:
        period = cust_get_period(end_date,filters)							
        cust_columns.append(
            {
                "label": _(period), 
                "fieldname": scrub(period), 
                "fieldtype": "Float",
                    "width": 120
            }
        )
        cust_columns.append(    
            {
                "label": "Gross Margin", 
                "fieldname": "Gross Margin", 
                "fieldtype": "Float",
                    "width": 120
            }
        )
    cust_columns.append(
        {
            "label": "YTD Total", 
            "fieldname": "YTD Total", 
            "fieldtype": "Float",
                "width": 120
        }
    )
    cust_columns.append(    
        {
            "label": "Gross Margin", 
            "fieldname": "Gross Margin", 
            "fieldtype": "Float",
                "width": 120
        }
        )
    # cust_columns.append(
    # 	{"label": _("Total"), "fieldname": "total", "fieldtype": "Float", "width": 120}
    # )
    #print("c14")
    return cust_columns    

def getwkenddate(currdt,flag1stdatepassdone):
    wkdate =  getdate(currdt)
    retdate = wkdate
    mth_endday = calendar.monthrange(wkdate.year,wkdate.month)[1]
    mth_enddate = datetime.date(wkdate.year, wkdate.month, mth_endday)
    islastday = 0
    firstdayislastwkday =0
    nextdate = add_to_date(wkdate , days=7)
    # if first date in month select last weekend date for first week
    # 
    if (wkdate.day==1):
        endofwkday = 4 # Friday
        stofwkday = 5 # Saturday   - Sat,Sun,Mon,Tue,Wed,Thur,Fri  - 5, 6,0,1,2,3,4
        # logic - if start of month falls on sat - then wk 1 end date will be next friday or next endofwkday
        if (wkdate.weekday()==stofwkday):
            retdate = add_to_date(wkdate , days=6)
        # if start of week is Sunday = 6 - then first week ends on the date that falls on Friday
        elif (wkdate.weekday()==6):
            retdate = add_to_date(wkdate , days=5)
        # if start of month = 4 () end of weekday - then first week ends on the friday for next week
        elif ((wkdate.weekday()==endofwkday) and (flag1stdatepassdone==0)):
            #retdate = add_to_date(wkdate , days=7)
            firstdayislastwkday = 1
        elif ((wkdate.weekday()==endofwkday) and (flag1stdatepassdone==1)):
            #retdate = add_to_date(wkdate , days=7)
            retdate = add_to_date(wkdate , days=7)             
        # if start of month less than 4 () start of weekday - then first week ends on the friday for that week
        else:
            noofdays = endofwkday - wkdate.weekday() + 1
            retdate = datetime.date(wkdate.year, wkdate.month, noofdays)
        #calendar.setfirstweekday(5)
    # if not first date and less than last date of month then increment by 7 days
    #    
    elif(nextdate<mth_enddate):
        if ((mth_enddate == add_to_date(nextdate , days=1)) or (mth_enddate == add_to_date(nextdate , days=2))) :
            retdate = mth_enddate
            islastday = 1
        else:
            retdate = nextdate     
    # if its the addition makes it greater than last day of month then set as last date
    # 
    else:
        retdate = mth_enddate
        islastday = 1
    # return date, and if last day = 1
    print(retdate.strftime('%Y-%m-%d') + " - " + str(islastday))
    return retdate.strftime('%Y-%m-%d'), islastday , firstdayislastwkday 

def getwkstartenddate(currdt,flag1stdatepassdone):
    wkdate =  getdate(currdt)
    retdate = wkdate
    retdate0 = wkdate
    mth_endday = calendar.monthrange(wkdate.year,wkdate.month)[1]
    mth_enddate = datetime.date(wkdate.year, wkdate.month, mth_endday)
    islastday = 0
    firstdayislastwkday =0
    nextdate = add_to_date(wkdate , days=7)
    nextstdate = add_to_date(wkdate , days=1)
    # if first date in month select last weekend date for first week
    # 
    if (wkdate.day==1):
        endofwkday = 4 # Friday
        stofwkday = 5 # Saturday   - Sat,Sun,Mon,Tue,Wed,Thur,Fri  - 5, 6,0,1,2,3,4
        # logic - if start of month falls on sat - then wk 1 end date will be next friday or next endofwkday
        if (wkdate.weekday()==stofwkday):
            retdate = add_to_date(wkdate , days=6)
        # if start of week is Sunday = 6 - then first week ends on the date that falls on Friday
        elif (wkdate.weekday()==6):
            retdate = add_to_date(wkdate , days=5)
        # if start of month = 4 () end of weekday - then first week ends on the friday for next week
        elif ((wkdate.weekday()==endofwkday) and (flag1stdatepassdone==0)):
            #retdate = add_to_date(wkdate , days=7)
            firstdayislastwkday = 1
        elif ((wkdate.weekday()==endofwkday) and (flag1stdatepassdone==1)):
            #retdate = add_to_date(wkdate , days=7)
            retdate = add_to_date(wkdate , days=7)             
        # if start of month less than 4 () start of weekday - then first week ends on the friday for that week
        else:
            noofdays = endofwkday - wkdate.weekday() + 1
            retdate = datetime.date(wkdate.year, wkdate.month, noofdays)
        retdate0 =  datetime.date(wkdate.year, wkdate.month, 1)   
        #calendar.setfirstweekday(5)
    # if not first date and less than last date of month then increment by 7 days
    #    
    elif(nextdate<mth_enddate):
        if ((mth_enddate == add_to_date(nextdate , days=1)) or (mth_enddate == add_to_date(nextdate , days=2))) :
            retdate = mth_enddate
            #retdate0 = add_to_date(nextdate , days=1)
            retdate0 = nextstdate
            islastday = 1
        else:
            retdate = nextdate
            retdate0 = nextstdate     
    # if its the addition makes it greater than last day of month then set as last date
    # 
    else:
        retdate = mth_enddate
        retdate0 = nextstdate
        islastday = 1
    # return date, and if last day = 1
    #print(retdate0.strftime('%Y-%m-%d') + " - " + retdate.strftime('%Y-%m-%d') + " - " + str(islastday))
    return retdate0.strftime('%Y-%m-%d'),retdate.strftime('%Y-%m-%d'), islastday , firstdayislastwkday 


def getwkno(currdt):
    retwkno = 0
    wkdate =  getdate(currdt)
    retdate = wkdate
    mth_endday = calendar.monthrange(wkdate.year,wkdate.month)[1]
    mth_enddate = datetime.date(wkdate.year, wkdate.month, mth_endday)
    islastday = 0
    nextdate = add_to_date(wkdate , days=7)
    # if first date in month select last weekend date for first week
    # 
    if (wkdate.day==1):
        endofwkday = 4 # Friday
        stofwkday = 5 # Saturday   - Sat,Sun,Mon,Tue,Wed,Thur,Fri  - 5, 6,0,1,2,3,4
        # logic - if start of month falls on sat - then wk 1 end date will be next friday or next endofwkday
        if (wkdate.weekday()==stofwkday):
            retdate = add_to_date(wkdate , days=6)
        # if start of week is Sunday = 6 - then first week ends on the date that falls on Friday
        elif (wkdate.weekday()==6):
            retdate = add_to_date(wkdate , days=5)
        # if start of month = 4 () end of weekday - then first week ends on the friday for next week
        elif (wkdate.weekday()==endofwkday):
            retdate = add_to_date(wkdate , days=7)         
        # if start of month less than 4 () start of weekday - then first week ends on the friday for that week
        else:
            noofdays = endofwkday - wkdate.weekday() + 1
            retdate = datetime.date(wkdate.year, wkdate.month, noofdays)
        #calendar.setfirstweekday(5)
    # if not first date and less than last date of month then increment by 7 days
    #    
    elif(nextdate<mth_enddate):
        retdate = nextdate 
    # if its the addition makes it greater than last day of month then set as last date
    # 
    else:
        retdate = mth_enddate
        islastday = 1
    # return date, and if last day = 1
    #print(retdate.strftime('%Y-%m-%d') + " - " + str(islastday))
    return retdate.strftime('%Y-%m-%d'), islastday
#
def cust_get_period(posting_date,filters):
    period = ""
    calendar.setfirstweekday(5)
    if filters.range == "Weekly":
        mnthname= posting_date.strftime('%b')
        x = np.array(calendar.monthcalendar(posting_date.year, posting_date.month)) 
        week_of_month = np.where(x == posting_date.day)[0][0] + 1			
        #period = "Week " + str(posting_date.isocalendar()[1]) + " "+ mnthname +" "+ str(posting_date.year)
        period = mnthname +"-"+ str(posting_date.year)[-2:]			
                
    return period

#for setting column month from week wise
def cust_get_period_date_ranges(filters):
    from dateutil.relativedelta import MO, relativedelta	
    from_date, to_date = getdate(filters.from_date), getdate(filters.fiscal_endDt)

    increment = {"Monthly": 1, "Quarterly": 3, "Half-Yearly": 6, "Yearly": 12}.get(
        filters.range, 1
    )
    
    if filters.range in ["Monthly", "Quarterly","Weekly"]:
        from_date = get_fiscal_year(from_date)[1]
    elif filters.range == "Yearly":
        from_date = get_fiscal_year(from_date)[1]
    else:
        from_date = from_date + relativedelta(from_date, weekday=MO(-1))

    periodic_daterange = []
    for dummy in range(1, 53):
        if filters.range == "Week":
            period_end_date = add_days(from_date, 6)
        else:
            period_end_date = add_to_date(from_date, months=increment, days=-1)

        if period_end_date > to_date:
            period_end_date = to_date

        periodic_daterange.append(period_end_date)

        from_date = add_days(period_end_date, 1)
        if period_end_date == to_date:
            break
    return periodic_daterange

#
def cust_get_weekperiod_prev(filters, posting_date):
    calendar.setfirstweekday(5)
    if filters.range == "Weekly":
        mnthname= posting_date.strftime('%b')
        x = np.array(calendar.monthcalendar(posting_date.year, posting_date.month)) 		
        week_of_month = np.where(x == posting_date.day)[0][0] + 1			
        #period = "Week " + str(posting_date.isocalendar()[1]) + " "+ mnthname +" "+ str(posting_date.year)			
        weekperiod= "Week " + str(week_of_month) +"@"+mnthname +"-"+ str(posting_date.year)[-2:]
    #print(weekperiod + " - " + str(posting_date))	
    return weekperiod

def cust_get_weekperiod(filters, posting_date):
    calendar.setfirstweekday(5)
    if filters.range == "Weekly":
        mnthname= posting_date.strftime('%b')
        x = np.array(calendar.monthcalendar(posting_date.year, posting_date.month)) 		
        week_of_month = np.where(x == posting_date.day)[0][0] + 1			
        #period = "Week " + str(posting_date.isocalendar()[1]) + " "+ mnthname +" "+ str(posting_date.year)			
        if (week_of_month==6):
            week_of_month = 5
        weekperiod= "Week " + str(week_of_month) +"@"+mnthname +"-"+ str(posting_date.year)[-2:]
        #print(str(week_of_month) + " - " + weekperiod + " - " + str(posting_date))	
    return weekperiod 

def cust_get_mthperiod(filters, posting_date,fiscyr):
    mnthname= posting_date.strftime('%b')
    mthperiod= mnthname +"-"+ str(fiscyr)[-2:] #"Week " + str(week_of_month) +"@"+mnthname +"-"+ str(posting_date.year)[-2:]
    return mthperiod


def cust_get_allweekperiods(filters, start_date, end_date):
    from dateutil.relativedelta import relativedelta
    data = []
    calendar.setfirstweekday(5)
    if filters.range == "Weekly":
        currdt = getdate(start_date)
        while (currdt <= getdate(end_date)):
            for x in range(1, 6):
                mnthname= currdt.strftime('%b')
                weekperiod= "Week " + str(x) +"@"+mnthname +"-"+ str(currdt.year)[-2:]
                data.append(weekperiod)
            currdt2 = currdt + relativedelta(months=+1)
            currdt = currdt2
    return data


#bind rows according to the record
def cust_get_rows(filters,records,Cust_periodic_daterange):
    data = []	
    ## start get week from month
    entity_periodic_data = frappe._dict()	
    if filters.range == "Weekly":			
        for d in records:								
            cust_period = cust_get_weekperiod(filters,d.transaction_date)				
            entity_periodic_data.setdefault(d.entity, frappe._dict()).setdefault(cust_period,0.0)						
            entity_periodic_data[d.entity][cust_period] += flt(d.value_field)			
        
    con_lis = list(entity_periodic_data.items())  #convert dict to list
    return con_lis

def cust_get_rows_forallweeks(filters,records,Cust_periodic_daterange,coycostcenters,from_date, to_date):
    data = []	
    ## start get week from month
    #print('here now 1')
    entity_periodic_data = frappe._dict()	
    if filters.range == "Weekly":
        # set all week periods
        cust_periods_list = cust_get_allweekperiods(filters, from_date, to_date)
        consolidcc = "Consolidated"
        for cp in cust_periods_list:
            ccTotalAmt0 = 0.0
            for d in records:
                cust_period = cust_get_weekperiod(filters,d[3])
                if ((consolidcc==d[0]) and (cp==cust_period)):
                    ccTotalAmt0 += flt(d[1])
                        
            entity_periodic_data.setdefault(consolidcc, frappe._dict()).setdefault(cp,ccTotalAmt0)
        
        if filters.cost_center:
            ccc = filters.cost_center
            for cc in ccc:
                for cp in cust_periods_list:
                    ccTotalAmt = 0.0
                    for d in records:
                        cust_period = cust_get_weekperiod(filters,d[3])
                        if ((cc==d[0]) and (cp==cust_period)):
                            ccTotalAmt += flt(d[1])
                        
                    entity_periodic_data.setdefault(cc, frappe._dict()).setdefault(cp,ccTotalAmt)						
        else:
            for cc in coycostcenters:
                for cp in cust_periods_list:
                    ccTotalAmt = 0.0
                    for d in records:
                        cust_period = cust_get_weekperiod(filters,d[3])
                        if ((cc==d[0]) and (cp==cust_period)):
                            ccTotalAmt += flt(d[1])
                        
                    entity_periodic_data.setdefault(cc, frappe._dict()).setdefault(cp,ccTotalAmt)

    con_lis = list(entity_periodic_data.items())  #convert dict to list
    
    #print(con_lis)
    return con_lis

def cust_get_sales_rows_forallweeks(filters,records,coycostcenters,from_date, to_date):
    data = []	
    ## start get week from month
    #print('here now 1')
    salesweekstr = "sales"
    grossprofitstr = "grossprofit"
    grossprofitmarginstr = "grossprofitmargin"
    entity_periodic_data = frappe._dict()	
    if filters.range == "Weekly":
        # set all week periods
        cust_periods_list = cust_get_allweekperiods(filters, from_date, to_date)
        consolidcc = "Consolidated"
        
        for cp in cust_periods_list:
            try:
                result = records[consolidcc][cp]
                entity_periodic_data.setdefault(consolidcc, frappe._dict()).setdefault(cp,frappe._dict()).setdefault(salesweekstr,records[consolidcc][cp][salesweekstr])
                entity_periodic_data.setdefault(consolidcc, frappe._dict()).setdefault(cp,frappe._dict()).setdefault(grossprofitstr,records[consolidcc][cp][grossprofitstr])
                entity_periodic_data.setdefault(consolidcc, frappe._dict()).setdefault(cp,frappe._dict()).setdefault(grossprofitmarginstr,records[consolidcc][cp][grossprofitmarginstr])
            except KeyError:
                entity_periodic_data.setdefault(consolidcc, frappe._dict()).setdefault(cp,frappe._dict()).setdefault(salesweekstr,0.0)
                entity_periodic_data.setdefault(consolidcc, frappe._dict()).setdefault(cp,frappe._dict()).setdefault(grossprofitstr,0.0)
                entity_periodic_data.setdefault(consolidcc, frappe._dict()).setdefault(cp,frappe._dict()).setdefault(grossprofitmarginstr,0.0)
            
            #ccTotalAmt0 = 0.0
            #for d in records:
            #    cust_period = cust_get_weekperiod(filters,d[3])
            #    if ((consolidcc==d[0]) and (cp==cust_period)):
            #        ccTotalAmt0 += flt(d[1])
                        
            #entity_periodic_data.setdefault(consolidcc, frappe._dict()).setdefault(cp,ccTotalAmt0)
        
        if filters.cost_center:
            ccc = filters.cost_center
            for cc in ccc:
                for cp in cust_periods_list:
                    try:
                        result = records[cc][cp]
                        entity_periodic_data.setdefault(cc, frappe._dict()).setdefault(cp,frappe._dict()).setdefault(salesweekstr,records[cc][cp][salesweekstr])
                        entity_periodic_data.setdefault(cc, frappe._dict()).setdefault(cp,frappe._dict()).setdefault(grossprofitstr,records[cc][cp][grossprofitstr])
                        entity_periodic_data.setdefault(cc, frappe._dict()).setdefault(cp,frappe._dict()).setdefault(grossprofitmarginstr,records[cc][cp][grossprofitmarginstr])
                    except KeyError:
                        entity_periodic_data.setdefault(cc, frappe._dict()).setdefault(cp,frappe._dict()).setdefault(salesweekstr,0.0)
                        entity_periodic_data.setdefault(cc, frappe._dict()).setdefault(cp,frappe._dict()).setdefault(grossprofitstr,0.0)
                        entity_periodic_data.setdefault(cc, frappe._dict()).setdefault(cp,frappe._dict()).setdefault(grossprofitmarginstr,0.0)
                    #ccTotalAmt = 0.0
                    #for d in records:
                    #    cust_period = cust_get_weekperiod(filters,d[3])
                    #    if ((cc==d[0]) and (cp==cust_period)):
                    #        ccTotalAmt += flt(d[1])
                        
                    #entity_periodic_data.setdefault(cc, frappe._dict()).setdefault(cp,ccTotalAmt)						
        else:
            for cc in coycostcenters:
                for cp in cust_periods_list:
                    try:
                        result = records[cc][cp]
                        entity_periodic_data.setdefault(cc, frappe._dict()).setdefault(cp,frappe._dict()).setdefault(salesweekstr,records[cc][cp][salesweekstr])
                        entity_periodic_data.setdefault(cc, frappe._dict()).setdefault(cp,frappe._dict()).setdefault(grossprofitstr,records[cc][cp][grossprofitstr])
                        entity_periodic_data.setdefault(cc, frappe._dict()).setdefault(cp,frappe._dict()).setdefault(grossprofitmarginstr,records[cc][cp][grossprofitmarginstr])
                    except KeyError:
                        entity_periodic_data.setdefault(cc, frappe._dict()).setdefault(cp,frappe._dict()).setdefault(salesweekstr,0.0)
                        entity_periodic_data.setdefault(cc, frappe._dict()).setdefault(cp,frappe._dict()).setdefault(grossprofitstr,0.0)
                        entity_periodic_data.setdefault(cc, frappe._dict()).setdefault(cp,frappe._dict()).setdefault(grossprofitmarginstr,0.0)
                    #ccTotalAmt = 0.0
                    #for d in records:
                    #    cust_period = cust_get_weekperiod(filters,d[3])
                    #    if ((cc==d[0]) and (cp==cust_period)):
                    #        ccTotalAmt += flt(d[1])
                        
                    #entity_periodic_data.setdefault(cc, frappe._dict()).setdefault(cp,ccTotalAmt)

    con_lis = list(entity_periodic_data.items())  #convert dict to list
    
    #print(con_lis)
    return con_lis        

#
def fetchselected_fiscalyear(end_date):
    fetch_fiscalyearslctn = frappe.db.sql(
        """
        (select year_start_date , year_end_date, year from `tabFiscal Year` 
        where  %(Slct_date)s between year_start_date and year_end_date
        )	
    """,{
            'Slct_date': end_date
        },		
        as_dict=1,
    )
    #print("c19")			
    return fetch_fiscalyearslctn

def fetch5yrsback_fiscalyear(noofyrsback,filters):
    if filters.to_date:
        end_date= filters.to_date
    else:
        end_date= date.today()
    fetch_fiscalyearslctn_1 = frappe.db.sql(
        """
        (select year, year_start_date , year_end_date from `tabFiscal Year` 
        where  %(Slct_date)s between year_start_date and year_end_date
        )	
    """,{
            'Slct_date': end_date
        },		
        as_dict=1,
    )
    curryr = 0
    prevyrsback = 0
    for ff in fetch_fiscalyearslctn_1:		
        #print(dd)				
        curryr = ff.year						
    prevyrsback = int(curryr) - noofyrsback

    fetch_fiscalyearslctn = frappe.db.sql(
        """
        (select year, year_start_date , year_end_date from `tabFiscal Year` 
        where year >= %(startyr)s and year < %(endyr)s order by year asc
        )	
    """,{
            'startyr': prevyrsback, 'endyr': curryr
        },		
        as_dict=1,
    )  

    fetch_fiscalyearslctn_3 = frappe.db.sql(
        """
        (select min(year_start_date) as begindate from `tabFiscal Year` 
        where year >= %(startyr)s and year < %(endyr)s
        )	
    """,{
            'startyr': prevyrsback, 'endyr': curryr
        },		
        as_dict=1,
    )      
    for ff2 in fetch_fiscalyearslctn_3:		
        prevyrsstartdate = ff2.begindate				
    #print(prevyrsstartdate)			
    return fetch_fiscalyearslctn,prevyrsstartdate


def getcostcenters(filters):
    cstcnt = [] # get function to fetch cost centers
    cstcntno = []
    cstcnt0 = frappe.db.get_list("Cost Center",filters={'company': filters.company,'is_group':0},fields=['name', 'cost_center_number'])
    # change the order of cost center this is customized for this client
    #specify order here 02, 03, 01, 06
    #cstorder = []
    cstorder = ['02', '03', '06', '01']
    i = 0
    while(i<len(cstorder)):
        for cstr in cstcnt0:
            if ((cstr.name).startswith(cstorder[i])):
                cstcnt.append(cstr.name)
                cstcntno.append(cstr.cost_center_number)
        i+=1
        
    # if created cost centers increase
    if ((len(cstorder)<len(cstcnt0)) and (len(cstcnt)>0) ):
        for cstr2 in cstcnt0:
            cstfound = False
            for m in cstcnt:
                if (m==cstr2.name):
                    cstfound = True
            if (cstfound == False):
                 cstcnt.append(cstr2.name)         
                 cstcntno.append(cstr2.cost_center_number) 
    if (len(cstcnt)==0):
        for cstr in cstcnt0:
            cstcnt.append(cstr.name)
            cstcntno.append(cstr.cost_center_number)
        
              
    return cstcnt,cstcntno

#def getcostcenters(filters):
#    cstcnt = [] # get function to fetch cost centers
#    cstcnt0 = frappe.db.get_list("Cost Center",pluck='name',filters={'company': filters.company,'is_group':0})
#    # change the order of cost center this is customized for this client
#    #specify order here 02, 03, 01, 06
#    #cstorder = []
#    cstorder = ['02', '03', '06', '01']
#    i = 0
#    while(i<len(cstorder)):
#        for cstr in cstcnt0:
#            if (cstr.startswith(cstorder[i])):
#                cstcnt.append(cstr)
#        i+=1
        
#    # if created cost centers increase
#    if ((len(cstorder)<len(cstcnt0)) and (len(cstcnt)>0) ):
#        for cstr2 in cstcnt0:
#            cstfound = False
#            for m in cstcnt:
#                if (m==cstr2):
#                    cstfound = True
#            if (cstfound == False):
#                 cstcnt.append(cstr2)         

#    if (len(cstcnt)==0):
#       cstcnt = cstcnt0 
#    return cstcnt

############## sales with gross margin section    			


class GrossProfitGeneratorbydaterange(object):
    def __init__(self,stdate,enddate, filters=None):
        self.data = []
        self.average_buying_rate = {}
        self.filters = frappe._dict(filters)
        self.stdate = stdate
        self.enddate = enddate


        self.load_invoice_items(stdate,enddate)

        self.group_items_by_invoice()

        self.load_stock_ledger_entries()
        self.load_product_bundle()
        self.load_non_stock_items()
        self.get_returned_invoice_items()
        self.process()

    def process(self):
        self.grouped = {}
        self.grouped_data = []

        self.currency_precision = cint(frappe.db.get_default("currency_precision")) or 3
        self.float_precision = cint(frappe.db.get_default("float_precision")) or 2
        self.filters.group_by = "Invoice"
        grouped_by_invoice = True if self.filters.get("group_by") == "Invoice" else False

        buying_amount = 0

        for row in reversed(self.si_list):
            if self.skip_row(row):
                continue

            row.base_amount = flt(row.base_net_amount, self.currency_precision)

            product_bundles = []
            if row.update_stock:
                product_bundles = self.product_bundles.get(row.parenttype, {}).get(row.parent, frappe._dict())
            elif row.dn_detail:
                product_bundles = self.product_bundles.get("Delivery Note", {}).get(
                    row.delivery_note, frappe._dict()
                )
                row.item_row = row.dn_detail

            # get buying amount
            if row.item_code in product_bundles:
                row.buying_amount = flt(
                    self.get_buying_amount_from_product_bundle(row, product_bundles[row.item_code]),
                    self.currency_precision,
                )
            else:
                row.buying_amount = flt(self.get_buying_amount(row, row.item_code), self.currency_precision)

            if grouped_by_invoice:
                if row.indent == 1.0:
                    buying_amount += row.buying_amount
                elif row.indent == 0.0:
                    row.buying_amount = buying_amount
                    buying_amount = 0

            # get buying rate
            if flt(row.qty):
                row.buying_rate = flt(row.buying_amount / flt(row.qty), self.float_precision)
                row.base_rate = flt(row.base_amount / flt(row.qty), self.float_precision)
            else:
                if self.is_not_invoice_row(row):
                    row.buying_rate, row.base_rate = 0.0, 0.0

            # calculate gross profit
            row.gross_profit = flt(row.base_amount - row.buying_amount, self.currency_precision)
            if row.base_amount:
                row.gross_profit_percent = flt(
                    (row.gross_profit / row.base_amount) * 100.0, self.currency_precision
                )
            else:
                row.gross_profit_percent = 0.0

            # add to grouped
            self.grouped.setdefault(row.get(scrub(self.filters.group_by)), []).append(row)

        if self.grouped:
            self.get_average_rate_based_on_group_by()
        #print(self.si_list)    

    def get_average_rate_based_on_group_by(self):
        for key in list(self.grouped):
            if self.filters.get("group_by") != "Invoice":
                for i, row in enumerate(self.grouped[key]):
                    if i == 0:
                        new_row = row
                    else:
                        new_row.qty += flt(row.qty)
                        new_row.buying_amount += flt(row.buying_amount, self.currency_precision)
                        new_row.base_amount += flt(row.base_amount, self.currency_precision)
                new_row = self.set_average_rate(new_row)
                self.grouped_data.append(new_row)
            else:
                for i, row in enumerate(self.grouped[key]):
                    if row.indent == 1.0:
                        if (
                            row.parent in self.returned_invoices and row.item_code in self.returned_invoices[row.parent]
                        ):
                            returned_item_rows = self.returned_invoices[row.parent][row.item_code]
                            for returned_item_row in returned_item_rows:
                                row.qty += flt(returned_item_row.qty)
                                row.base_amount += flt(returned_item_row.base_amount, self.currency_precision)
                            row.buying_amount = flt(flt(row.qty) * flt(row.buying_rate), self.currency_precision)
                        if flt(row.qty) or row.base_amount:
                            row = self.set_average_rate(row)
                            self.grouped_data.append(row)

    def is_not_invoice_row(self, row):
        return (self.filters.get("group_by") == "Invoice" and row.indent != 0.0) or self.filters.get(
            "group_by"
        ) != "Invoice"

    def set_average_rate(self, new_row):
        self.set_average_gross_profit(new_row)
        new_row.buying_rate = (
            flt(new_row.buying_amount / new_row.qty, self.float_precision) if new_row.qty else 0
        )
        new_row.base_rate = (
            flt(new_row.base_amount / new_row.qty, self.float_precision) if new_row.qty else 0
        )
        return new_row

    def set_average_gross_profit(self, new_row):
        new_row.gross_profit = flt(new_row.base_amount - new_row.buying_amount, self.currency_precision)
        new_row.gross_profit_percent = (
            flt(((new_row.gross_profit / new_row.base_amount) * 100.0), self.currency_precision)
            if new_row.base_amount
            else 0
        )
        new_row.buying_rate = (
            flt(new_row.buying_amount / flt(new_row.qty), self.float_precision) if flt(new_row.qty) else 0
        )
        new_row.base_rate = (
            flt(new_row.base_amount / flt(new_row.qty), self.float_precision) if flt(new_row.qty) else 0
        )

    def get_returned_invoice_items(self):
        returned_invoices = frappe.db.sql(
            """
            select
                si.name, si_item.item_code, si_item.stock_qty as qty, si_item.base_net_amount as base_amount, si.return_against
            from
                `tabSales Invoice` si, `tabSales Invoice Item` si_item
            where
                si.name = si_item.parent
                and si.docstatus = 1
                and si.is_return = 1
        """,
            as_dict=1,
        )

        self.returned_invoices = frappe._dict()
        for inv in returned_invoices:
            self.returned_invoices.setdefault(inv.return_against, frappe._dict()).setdefault(
                inv.item_code, []
            ).append(inv)

    def skip_row(self, row):
        if self.filters.get("group_by") != "Invoice":
            if not row.get(scrub(self.filters.get("group_by", ""))):
                return True

        return False

    def get_buying_amount_from_product_bundle(self, row, product_bundle):
        buying_amount = 0.0
        for packed_item in product_bundle:
            if packed_item.get("parent_detail_docname") == row.item_row:
                buying_amount += self.get_buying_amount(row, packed_item.item_code)

        return flt(buying_amount, self.currency_precision)

    def get_buying_amount(self, row, item_code):
        # IMP NOTE
        # stock_ledger_entries should already be filtered by item_code and warehouse and
        # sorted by posting_date desc, posting_time desc
        if item_code in self.non_stock_items and (row.project or row.cost_center):
            # Issue 6089-Get last purchasing rate for non-stock item
            item_rate = self.get_last_purchase_rate(item_code, row)
            return flt(row.qty) * item_rate

        else:
            my_sle = self.sle.get((item_code, row.warehouse))
            if (row.update_stock or row.dn_detail) and my_sle:
                parenttype, parent = row.parenttype, row.parent
                if row.dn_detail:
                    parenttype, parent = "Delivery Note", row.delivery_note

                for i, sle in enumerate(my_sle):
                    # find the stock valution rate from stock ledger entry
                    if (
                        sle.voucher_type == parenttype
                        and parent == sle.voucher_no
                        and sle.voucher_detail_no == row.item_row
                    ):
                        previous_stock_value = len(my_sle) > i + 1 and flt(my_sle[i + 1].stock_value) or 0.0

                        if previous_stock_value:
                            return (previous_stock_value - flt(sle.stock_value)) * flt(row.qty) / abs(flt(sle.qty))
                        else:
                            return flt(row.qty) * self.get_average_buying_rate(row, item_code)
            else:
                return flt(row.qty) * self.get_average_buying_rate(row, item_code)

        return 0.0

    def get_average_buying_rate(self, row, item_code):
        args = row
        if not item_code in self.average_buying_rate:
            args.update(
                {
                    "voucher_type": row.parenttype,
                    "voucher_no": row.parent,
                    "allow_zero_valuation": True,
                    "company": self.filters.company,
                }
            )

            average_buying_rate = get_incoming_rate(args)
            self.average_buying_rate[item_code] = flt(average_buying_rate)

        return self.average_buying_rate[item_code]

    def get_last_purchase_rate(self, item_code, row):
        purchase_invoice = frappe.qb.DocType("Purchase Invoice")
        purchase_invoice_item = frappe.qb.DocType("Purchase Invoice Item")

        query = (
            frappe.qb.from_(purchase_invoice_item)
            .inner_join(purchase_invoice)
            .on(purchase_invoice.name == purchase_invoice_item.parent)
            .select(purchase_invoice_item.base_rate / purchase_invoice_item.conversion_factor)
            .where(purchase_invoice.docstatus == 1)
            .where(purchase_invoice.posting_date <= self.filters.to_date)
            .where(purchase_invoice_item.item_code == item_code)
        )

        if row.project:
            query.where(purchase_invoice_item.project == row.project)

        if row.cost_center:
            query.where(purchase_invoice_item.cost_center == row.cost_center)

        query.orderby(purchase_invoice.posting_date, order=frappe.qb.desc)
        query.limit(1)
        last_purchase_rate = query.run()

        return flt(last_purchase_rate[0][0]) if last_purchase_rate else 0

    
    def load_invoice_items(self,start_date,end_date):
        conditions = ""
        #conditions2 = ""
        if self.filters.company:
            conditions += " and company = %(company)s"
        if self.filters.get("cost_center"):
            conditions += " and `tabSales Invoice Item`.cost_center IN %(cost_center)s"    
        conditions += " and (posting_date >= '" + start_date + "' and posting_date <= '" + end_date + "')"
        skip_total_row = 0
        
        self.si_list = frappe.db.sql(
            """
            select
                `tabSales Invoice Item`.parenttype, `tabSales Invoice Item`.parent,
                `tabSales Invoice`.posting_date, `tabSales Invoice`.posting_time,
                `tabSales Invoice`.project, `tabSales Invoice`.update_stock,
                `tabSales Invoice`.customer, `tabSales Invoice`.customer_group,
                `tabSales Invoice`.territory, `tabSales Invoice Item`.item_code,
                `tabSales Invoice Item`.item_name, `tabSales Invoice Item`.description,
                `tabSales Invoice Item`.warehouse, `tabSales Invoice Item`.item_group,
                `tabSales Invoice Item`.brand, `tabSales Invoice Item`.dn_detail,
                `tabSales Invoice Item`.delivery_note, `tabSales Invoice Item`.stock_qty as qty,
                `tabSales Invoice Item`.base_net_rate, `tabSales Invoice Item`.base_net_amount,
                `tabSales Invoice Item`.name as "item_row", `tabSales Invoice`.is_return,
                `tabSales Invoice Item`.cost_center
            
            from
                `tabSales Invoice` inner join `tabSales Invoice Item`
                    on `tabSales Invoice Item`.parent = `tabSales Invoice`.name
                
            where
                `tabSales Invoice`.docstatus=1 and `tabSales Invoice`.is_opening!='Yes' {conditions} {match_cond}
            order by
                `tabSales Invoice`.posting_date desc, `tabSales Invoice`.posting_time desc""".format(
                conditions=conditions,
                match_cond=get_match_cond("Sales Invoice"),
            ),
            self.filters,
            as_dict=1,
            #debug=1
        )
    
    def group_items_by_invoice(self):
        """
        Turns list of Sales Invoice Items to a tree of Sales Invoices with their Items as children.
        """

        parents = []

        for row in self.si_list:
            if row.parent not in parents:
                parents.append(row.parent)

        parents_index = 0
        for index, row in enumerate(self.si_list):
            if parents_index < len(parents) and row.parent == parents[parents_index]:
                invoice = self.get_invoice_row(row)
                self.si_list.insert(index, invoice)
                parents_index += 1

            else:
                # skipping the bundle items rows
                if not row.indent:
                    row.indent = 1.0
                    row.parent_invoice = row.parent
                    row.invoice_or_item = row.item_code

                    if frappe.db.exists("Product Bundle", row.item_code):
                        self.add_bundle_items(row, index)

    def get_invoice_row(self, row):
        return frappe._dict(
            {
                "parent_invoice": "",
                "indent": 0.0,
                "invoice_or_item": row.parent,
                "parent": None,
                "posting_date": row.posting_date,
                "posting_time": row.posting_time,
                "project": row.project,
                "update_stock": row.update_stock,
                "customer": row.customer,
                "customer_group": row.customer_group,
                "item_code": None,
                "item_name": None,
                "description": None,
                "warehouse": None,
                "item_group": None,
                "brand": None,
                "dn_detail": None,
                "delivery_note": None,
                "qty": None,
                "item_row": None,
                "is_return": row.is_return,
                "cost_center": row.cost_center,
                "base_net_amount": frappe.db.get_value("Sales Invoice", row.parent, "base_net_total"),
            }
        )

    def add_bundle_items(self, product_bundle, index):
        bundle_items = self.get_bundle_items(product_bundle)

        for i, item in enumerate(bundle_items):
            bundle_item = self.get_bundle_item_row(product_bundle, item)
            self.si_list.insert((index + i + 1), bundle_item)

    def get_bundle_items(self, product_bundle):
        return frappe.get_all(
            "Product Bundle Item", filters={"parent": product_bundle.item_code}, fields=["item_code", "qty"]
        )

    def get_bundle_item_row(self, product_bundle, item):
        item_name, description, item_group, brand = self.get_bundle_item_details(item.item_code)

        return frappe._dict(
            {
                "parent_invoice": product_bundle.item_code,
                "indent": product_bundle.indent + 1,
                "parent": None,
                "invoice_or_item": item.item_code,
                "posting_date": product_bundle.posting_date,
                "posting_time": product_bundle.posting_time,
                "project": product_bundle.project,
                "customer": product_bundle.customer,
                "customer_group": product_bundle.customer_group,
                "item_code": item.item_code,
                "item_name": item_name,
                "description": description,
                "warehouse": product_bundle.warehouse,
                "item_group": item_group,
                "brand": brand,
                "dn_detail": product_bundle.dn_detail,
                "delivery_note": product_bundle.delivery_note,
                "qty": (flt(product_bundle.qty) * flt(item.qty)),
                "item_row": None,
                "is_return": product_bundle.is_return,
                "cost_center": product_bundle.cost_center,
            }
        )

    def get_bundle_item_details(self, item_code):
        return frappe.db.get_value(
            "Item", item_code, ["item_name", "description", "item_group", "brand"]
        )

    def load_stock_ledger_entries(self):
        res = frappe.db.sql(
            """select item_code, voucher_type, voucher_no,
                voucher_detail_no, stock_value, warehouse, actual_qty as qty
            from `tabStock Ledger Entry`
            where company=%(company)s and is_cancelled = 0
            order by
                item_code desc, warehouse desc, posting_date desc,
                posting_time desc, creation desc""",
            self.filters,
            as_dict=True,
        )
        self.sle = {}
        for r in res:
            if (r.item_code, r.warehouse) not in self.sle:
                self.sle[(r.item_code, r.warehouse)] = []

            self.sle[(r.item_code, r.warehouse)].append(r)

    def load_product_bundle(self):
        self.product_bundles = {}

        for d in frappe.db.sql(
            """select parenttype, parent, parent_item,
            item_code, warehouse, -1*qty as total_qty, parent_detail_docname
            from `tabPacked Item` where docstatus=1""",
            as_dict=True,
        ):
            self.product_bundles.setdefault(d.parenttype, frappe._dict()).setdefault(
                d.parent, frappe._dict()
            ).setdefault(d.parent_item, []).append(d)

    def load_non_stock_items(self):
        self.non_stock_items = frappe.db.sql_list(
            """select name from tabItem
            where is_stock_item=0"""
        )

def cust_get_columns_for_weeklysales(filters,Cust_periodic_daterange):
    cust_columns=[
            {
                "label": "Sales", 
                "fieldname": "Sales", 
                "fieldtype": "data",
                    "width": 120
            }
        ]
    for end_date in Cust_periodic_daterange:
        period = cust_get_period(end_date,filters)							
        cust_columns.append(
            {
                "label": _(period), 
                "fieldname": scrub(period), 
                "fieldtype": "Float",
                    "width": 120
            }
        )
        cust_columns.append(
            {
                "label": "Gross Margin", 
                "fieldname": "Gross Margin", 
                "fieldtype": "Float",
                    "width": 120
            }
        )
    return cust_columns


def get_weeklysales_report_record(filters,start_date,fiscal_endDt,coycostcenters,fiscalyeardtprev,coycostcenternos):
    from dateutil.relativedelta import MO, relativedelta
    # Skipping total row for tree-view reports
    
    wk_total_list = frappe._dict()
    mth_total_list = frappe._dict()
    #for it to be well sorted
    mth_total_list2 = frappe._dict()
    if filters.cost_center:	
        wkst_date = start_date
        currdat = start_date
        sales_allrecord = [] #frappe._dict() 
        i=0
        firstdayislastwkday = 0
        cumsalesmtd1 = 0.0
        cumsalesmtd2 = 0.0
        while currdat <= filters.to_date:
            flagfirstdaypass = 0
            if (firstdayislastwkday == 1):
                flagfirstdaypass = 1
            currstartdat,currdat,itsdlastday,firstdayislastwkday = getwkstartenddate(currdat,flagfirstdaypass)
            gross_profit_data_forweek = GrossProfitGeneratorbydaterange(currstartdat,currdat,filters)
            if (itsdlastday == 1):
                currdat = add_to_date(currdat,days=1)
                currstartdat = currdat
           
            cumsaleswk1 = 0.0
            wkgrossprf = 0.0
            salesweekstr = "sales"
            grossprofitstr = "grossprofit"
            grossprofitmarginstr = "grossprofitmargin"
            concstr = "Consolidated"	
            for dd in gross_profit_data_forweek.si_list:
                if (dd["indent"]==0.0):
                    cumsaleswk1 += dd["base_net_amount"]
                    cumsalesmtd1 += dd["base_net_amount"] 
                    wkgrossprf += dd["gross_profit"]
                    cust_period = cust_get_weekperiod(filters,dd["posting_date"])
                    wk_total_list.setdefault(dd.cost_center, frappe._dict()).setdefault(cust_period,frappe._dict()).setdefault(salesweekstr,0.0)
                    wk_total_list[dd.cost_center][cust_period][salesweekstr] += flt(dd["base_net_amount"])
                    wk_total_list.setdefault(dd.cost_center, frappe._dict()).setdefault(cust_period,frappe._dict()).setdefault(grossprofitstr,0.0)
                    wk_total_list[dd.cost_center][cust_period][grossprofitstr] += flt(dd["gross_profit"])
                    wk_total_list.setdefault(dd.cost_center, frappe._dict()).setdefault(cust_period,frappe._dict()).setdefault(grossprofitmarginstr,0.0)
                    
                    wk_total_list.setdefault(concstr, frappe._dict()).setdefault(cust_period,frappe._dict()).setdefault(salesweekstr,0.0)
                    wk_total_list[concstr][cust_period][salesweekstr] += flt(dd["base_net_amount"])
                    wk_total_list.setdefault(concstr, frappe._dict()).setdefault(cust_period,frappe._dict()).setdefault(grossprofitstr,0.0)
                    wk_total_list[concstr][cust_period][grossprofitstr] += flt(dd["gross_profit"])
                    wk_total_list.setdefault(concstr, frappe._dict()).setdefault(cust_period,frappe._dict()).setdefault(grossprofitmarginstr,0.0)
                    try:
                        wk_total_list[dd.cost_center][cust_period][grossprofitmarginstr] = wk_total_list[dd.cost_center][cust_period][grossprofitstr]/wk_total_list[dd.cost_center][cust_period][salesweekstr] * 100
                    except ZeroDivisionError:
                        wk_total_list[dd.cost_center][cust_period][grossprofitmarginstr] = 0
                    try:
                        wk_total_list[concstr][cust_period][grossprofitmarginstr] = wk_total_list[concstr][cust_period][grossprofitstr]/wk_total_list[concstr][cust_period][salesweekstr] * 100
                    except ZeroDivisionError:
                        wk_total_list[concstr][cust_period][grossprofitmarginstr] = 0
                    
        #print(wk_total_list)
        min_date_backloglst = []
        #
        
        for fy3 in fiscalyeardtprev: 
            fyr = fy3.year
            fsd = fy3.year_start_date
            fed = fy3.year_end_date
            currdt = fy3.year_start_date
        
            i = 1
            while ((i < 13) and (currdt < fed)):
                mth_end_day = calendar.monthrange(currdt.year,currdt.month)[1]
                mth_end_date = datetime.date(currdt.year, currdt.month, mth_end_day)
                mth_start_date = datetime.date(currdt.year, currdt.month, 1)
                mth_start_datestr = mth_start_date.strftime('%Y-%m-%d')
                mth_end_datestr = mth_end_date.strftime('%Y-%m-%d')
                gross_profit_data_formth = GrossProfitGeneratorbydaterange(mth_start_datestr,mth_end_datestr,filters)
                i += 1
                currdt2 = currdt + relativedelta(months=+1)
                currdt = currdt2
                
                cumsaleswk2 = 0.0
                wkgrossprf2 = 0.0
                salesweekstr = "sales"
                grossprofitstr = "grossprofit"
                grossprofitmarginstr = "grossprofitmargin"
                concstr = "Consolidated"	
                for dd in gross_profit_data_formth.si_list:
                    if (dd["indent"]==0.0):
                        cumsaleswk2 += dd["base_net_amount"]
                        cumsalesmtd2 += dd["base_net_amount"] 
                        wkgrossprf2 += dd["gross_profit"]
                        cust_period = cust_get_mthperiod(filters,dd["posting_date"],fyr)
                        mth_total_list.setdefault(dd.cost_center, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(cust_period,frappe._dict()).setdefault(salesweekstr,0.0)
                        mth_total_list[dd.cost_center][fyr][cust_period][salesweekstr] += flt(dd["base_net_amount"])
                        mth_total_list.setdefault(dd.cost_center, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(cust_period,frappe._dict()).setdefault(grossprofitstr,0.0)
                        mth_total_list[dd.cost_center][fyr][cust_period][grossprofitstr] += flt(dd["gross_profit"])
                        mth_total_list.setdefault(dd.cost_center, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(cust_period,frappe._dict()).setdefault(grossprofitmarginstr,0.0)
                    
                        mth_total_list.setdefault(concstr, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(cust_period,frappe._dict()).setdefault(salesweekstr,0.0)
                        mth_total_list[concstr][fyr][cust_period][salesweekstr] += flt(dd["base_net_amount"])
                        mth_total_list.setdefault(concstr, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(cust_period,frappe._dict()).setdefault(grossprofitstr,0.0)
                        mth_total_list[concstr][fyr][cust_period][grossprofitstr] += flt(dd["gross_profit"])
                        mth_total_list.setdefault(concstr, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(cust_period,frappe._dict()).setdefault(grossprofitmarginstr,0.0)
                        try:
                            mth_total_list[dd.cost_center][fyr][cust_period][grossprofitmarginstr] = mth_total_list[dd.cost_center][fyr][cust_period][grossprofitstr]/mth_total_list[dd.cost_center][fyr][cust_period][salesweekstr] * 100
                        except ZeroDivisionError:
                            mth_total_list[dd.cost_center][fyr][cust_period][grossprofitmarginstr] = 0
                        try:
                            mth_total_list[concstr][fyr][cust_period][grossprofitmarginstr] = mth_total_list[concstr][fyr][cust_period][grossprofitstr]/mth_total_list[concstr][fyr][cust_period][salesweekstr] * 100
                        except ZeroDivisionError:
                            mth_total_list[concstr][fyr][cust_period][grossprofitmarginstr] = 0
        
        
                
    else:
        currdat = start_date
        sales_allrecord = [] #frappe._dict() 
        i=0
        firstdayislastwkday = 0
        cumsalesmtd1 = 0.0
        cumsalesmtd2 = 0.0
        while currdat <= filters.to_date:
            flagfirstdaypass = 0
            if (firstdayislastwkday == 1):
                flagfirstdaypass = 1
            currstartdat,currdat,itsdlastday,firstdayislastwkday = getwkstartenddate(currdat,flagfirstdaypass)
            gross_profit_data_forweek = GrossProfitGeneratorbydaterange(currstartdat,currdat,filters)
            if (itsdlastday == 1):
                currdat = add_to_date(currdat,days=1)
                currstartdat = currdat    
            
            cumsaleswk1 = 0.0
            wkgrossprf = 0.0
            salesweekstr = "sales"
            grossprofitstr = "grossprofit"
            grossprofitmarginstr = "grossprofitmargin"
            concstr = "Consolidated"	
            for dd in gross_profit_data_forweek.si_list:
                if (dd["indent"]==0.0):
                    cumsaleswk1 += dd["base_net_amount"]
                    cumsalesmtd1 += dd["base_net_amount"] 
                    wkgrossprf += dd["gross_profit"]
                    cust_period = cust_get_weekperiod(filters,dd["posting_date"])
                    wk_total_list.setdefault(dd.cost_center, frappe._dict()).setdefault(cust_period,frappe._dict()).setdefault(salesweekstr,0.0)
                    wk_total_list[dd.cost_center][cust_period][salesweekstr] += flt(dd["base_net_amount"])
                    wk_total_list.setdefault(dd.cost_center, frappe._dict()).setdefault(cust_period,frappe._dict()).setdefault(grossprofitstr,0.0)
                    wk_total_list[dd.cost_center][cust_period][grossprofitstr] += flt(dd["gross_profit"])
                    wk_total_list.setdefault(dd.cost_center, frappe._dict()).setdefault(cust_period,frappe._dict()).setdefault(grossprofitmarginstr,0.0)
                    
                    wk_total_list.setdefault(concstr, frappe._dict()).setdefault(cust_period,frappe._dict()).setdefault(salesweekstr,0.0)
                    wk_total_list[concstr][cust_period][salesweekstr] += flt(dd["base_net_amount"])
                    wk_total_list.setdefault(concstr, frappe._dict()).setdefault(cust_period,frappe._dict()).setdefault(grossprofitstr,0.0)
                    wk_total_list[concstr][cust_period][grossprofitstr] += flt(dd["gross_profit"])
                    wk_total_list.setdefault(concstr, frappe._dict()).setdefault(cust_period,frappe._dict()).setdefault(grossprofitmarginstr,0.0)
                    try:
                        wk_total_list[dd.cost_center][cust_period][grossprofitmarginstr] = wk_total_list[dd.cost_center][cust_period][grossprofitstr]/wk_total_list[dd.cost_center][cust_period][salesweekstr] * 100
                    except ZeroDivisionError:
                        wk_total_list[dd.cost_center][cust_period][grossprofitmarginstr] = 0
                    try:
                        wk_total_list[concstr][cust_period][grossprofitmarginstr] = wk_total_list[concstr][cust_period][grossprofitstr]/wk_total_list[concstr][cust_period][salesweekstr] * 100
                    except ZeroDivisionError:
                        wk_total_list[concstr][cust_period][grossprofitmarginstr] = 0
        #print(wk_total_list)


        min_date_backloglst = []
        #
        styear = 0
        endyear = 0
        j = 0
        for fy3 in fiscalyeardtprev: 
            if j == 0 :
                styear = fy3.year
            j+=1
            if j == 4 :    
                endyear = fy3.year 
            #print('j-' + str(j))   

        ddictkey = get_keycode(coycostcenternos,'sl',styear,endyear)
        prevyrsales_sessions = get_prevweeklysalesdata(ddictkey)   #frappe.cache().hget("costcenter-yrfrom-yrto-sessions","check1")
        if prevyrsales_sessions=='':     #None:
            for fy3 in fiscalyeardtprev: 
                fyr = fy3.year
                fsd = fy3.year_start_date
                fed = fy3.year_end_date
                currdt = fy3.year_start_date
        
                i = 1
                while ((i < 13) and (currdt < fed)):
                    mth_end_day = calendar.monthrange(currdt.year,currdt.month)[1]
                    mth_end_date = datetime.date(currdt.year, currdt.month, mth_end_day)
                    mth_start_date = datetime.date(currdt.year, currdt.month, 1)
                    mth_start_datestr = mth_start_date.strftime('%Y-%m-%d')
                    mth_end_datestr = mth_end_date.strftime('%Y-%m-%d')
                    gross_profit_data_formth = GrossProfitGeneratorbydaterange(mth_start_datestr,mth_end_datestr,filters)
                    i += 1
                    currdt2 = currdt + relativedelta(months=+1)
                    currdt = currdt2
                
                    cumsaleswk2 = 0.0
                    wkgrossprf2 = 0.0
                    salesweekstr = "sales"
                    grossprofitstr = "grossprofit"
                    grossprofitmarginstr = "grossprofitmargin"
                    concstr = "Consolidated"	
                    for dd in gross_profit_data_formth.si_list:
                        if (dd["indent"]==0.0):
                            cumsaleswk2 += dd["base_net_amount"]
                            cumsalesmtd2 += dd["base_net_amount"] 
                            wkgrossprf2 += dd["gross_profit"]
                            cust_period = cust_get_mthperiod(filters,dd["posting_date"],fyr)
                            mth_total_list.setdefault(dd.cost_center, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(cust_period,frappe._dict()).setdefault(salesweekstr,0.0)
                            mth_total_list[dd.cost_center][fyr][cust_period][salesweekstr] += flt(dd["base_net_amount"])
                            mth_total_list.setdefault(dd.cost_center, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(cust_period,frappe._dict()).setdefault(grossprofitstr,0.0)
                            mth_total_list[dd.cost_center][fyr][cust_period][grossprofitstr] += flt(dd["gross_profit"])
                            mth_total_list.setdefault(dd.cost_center, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(cust_period,frappe._dict()).setdefault(grossprofitmarginstr,0.0)
                    
                            mth_total_list.setdefault(concstr, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(cust_period,frappe._dict()).setdefault(salesweekstr,0.0)
                            mth_total_list[concstr][fyr][cust_period][salesweekstr] += flt(dd["base_net_amount"])
                            mth_total_list.setdefault(concstr, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(cust_period,frappe._dict()).setdefault(grossprofitstr,0.0)
                            mth_total_list[concstr][fyr][cust_period][grossprofitstr] += flt(dd["gross_profit"])
                            mth_total_list.setdefault(concstr, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(cust_period,frappe._dict()).setdefault(grossprofitmarginstr,0.0)
                            try:
                                mth_total_list[dd.cost_center][fyr][cust_period][grossprofitmarginstr] = mth_total_list[dd.cost_center][fyr][cust_period][grossprofitstr]/mth_total_list[dd.cost_center][fyr][cust_period][salesweekstr] * 100
                            except ZeroDivisionError:
                                mth_total_list[dd.cost_center][fyr][cust_period][grossprofitmarginstr] = 0
                            try:
                                mth_total_list[concstr][fyr][cust_period][grossprofitmarginstr] = mth_total_list[concstr][fyr][cust_period][grossprofitstr]/mth_total_list[concstr][fyr][cust_period][salesweekstr] * 100
                            except ZeroDivisionError:
                                mth_total_list[concstr][fyr][cust_period][grossprofitmarginstr] = 0
        
            set_prevweeklysalesdata(ddictkey,json.dumps(mth_total_list)) 
            #frappe.cache().hset("costcenter-yrfrom-yrto-sessions","check1",mth_total_list)
            #frappe.cache().bgsave()
        else :
            mth_total_list = json.loads(prevyrsales_sessions)   #json.loads(get_prevweeklysalesdata(ddictkey))
            #mth_total_list = frappe.cache().hget("costcenter-yrfrom-yrto-sessions","check1") 
            print("cache found")
            
            #print(mth_total_list)
            ##### to convert dict to byte
            #frappe.cache().set("costcenter-yrfrom-yrto-sessions",mth_total_list)
            #res_bytes = json.dumps(mth_total_list).encode('utf-8')
            ##### to convert back to dict
            #res_bytes = frappe.cache().get("costcenter-yrfrom-yrto-sessions")
            #mth_total_list = json.loads(res_bytes.decode('utf-8'))   
        print(mth_total_list)       
       
    year_total_list = frappe._dict()	
    
    # check through all cost centers and consolidated and see missing weeks and initialize to zero


    # check through all cost centers and prev yrs and see missing months and year and initialize to zero
    year_total_list2 = frappe._dict()
    salesweekstr = "sales"
    grossprofitstr = "grossprofit"
    grossprofitmarginstr = "grossprofitmargin"
    concstr = 'Consolidated'
    for fy3 in fiscalyeardtprev: 
        fyr = fy3.year
        fsd = fy3.year_start_date
        fed = fy3.year_end_date
        currdt = fy3.year_start_date
        bkltotamt = 0.0
        #for x in range(1, 12):
        i = 1
        while ((i < 13) and (currdt < fed)):
            i += 1
            #print(currdt)
            mthyrstr = currdt.strftime("%b") + "-" + fyr[-2:]
            #print(mthyrstr)
            #take care of consolidated cost center
            #loop through all cost centers
            consolidatedcc = 'Consolidated'
            ccTotalAmt0 = 0
            ##for dd in min_date_backlog:
            try:
                result = mth_total_list[concstr][fyr][mthyrstr]
                mth_total_list2.setdefault(concstr, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(mthyrstr,frappe._dict()).setdefault(salesweekstr,mth_total_list[concstr][fyr][mthyrstr][salesweekstr])
                mth_total_list2.setdefault(concstr, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(mthyrstr,frappe._dict()).setdefault(grossprofitstr,mth_total_list[concstr][fyr][mthyrstr][grossprofitstr])
                mth_total_list2.setdefault(concstr, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(mthyrstr,frappe._dict()).setdefault(grossprofitmarginstr,mth_total_list[concstr][fyr][mthyrstr][grossprofitmarginstr])
            except KeyError:
                mth_total_list2.setdefault(concstr, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(mthyrstr,frappe._dict()).setdefault(salesweekstr,0.0)
                mth_total_list2.setdefault(concstr, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(mthyrstr,frappe._dict()).setdefault(grossprofitstr,0.0)
                mth_total_list2.setdefault(concstr, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(mthyrstr,frappe._dict()).setdefault(grossprofitmarginstr,0.0)

            #for dd in mth_total_list:
            #    if ((dd[3]==consolidatedcc) and (dd[0]==mthyrstr) and (dd[1]==fyr)):
            #        ccTotalAmt0 += dd[2]
            #year_total_list2.setdefault(consolidatedcc, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(mthyrstr,ccTotalAmt0)
            #year_total_list2[consolidatedcc][fyr][mthyrstr] += flt(ccTotalAmt0)    
                #if ((dd.cost_center==consolidatedcc) and (dd.Date==mthyrstr) and (dd.year==fyr)):
                #    ccTotalAmt0 = dd.TotalAmt
            #year_total_list2.setdefault(consolidatedcc, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(mthyrstr,ccTotalAmt0)
            #year_total_list2[consolidatedcc][fyr][mthyrstr] += flt(ccTotalAmt0)
            #
            if filters.cost_center:
                ccc = filters.cost_center
                for cc in ccc:
                    try:
                        result = mth_total_list[cc][fyr][mthyrstr]
                        mth_total_list2.setdefault(cc, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(mthyrstr,frappe._dict()).setdefault(salesweekstr,mth_total_list[cc][fyr][mthyrstr][salesweekstr])
                        mth_total_list2.setdefault(cc, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(mthyrstr,frappe._dict()).setdefault(grossprofitstr,mth_total_list[cc][fyr][mthyrstr][grossprofitstr])
                        mth_total_list2.setdefault(cc, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(mthyrstr,frappe._dict()).setdefault(grossprofitmarginstr,mth_total_list[cc][fyr][mthyrstr][grossprofitmarginstr])
                    except KeyError:
                        mth_total_list2.setdefault(cc, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(mthyrstr,frappe._dict()).setdefault(salesweekstr,0.0)
                        mth_total_list2.setdefault(cc, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(mthyrstr,frappe._dict()).setdefault(grossprofitstr,0.0)
                        mth_total_list2.setdefault(cc, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(mthyrstr,frappe._dict()).setdefault(grossprofitmarginstr,0.0)
                    #ccTotalAmt = 0
                    #for dd in min_date_backloglst:    
                    #    if ((dd[3]==cc) and (dd[0]==mthyrstr) and (dd[1]==fyr)):
                    #        ccTotalAmt = dd[2]
                    #year_total_list2.setdefault(cc, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(mthyrstr,ccTotalAmt)
                    #year_total_list2[cc][fyr][mthyrstr] += flt(ccTotalAmt)            
            else:
                for cc in coycostcenters:
                    ccTotalAmt = 0
                    try:
                        result = mth_total_list[cc][fyr][mthyrstr]
                        mth_total_list2.setdefault(cc, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(mthyrstr,frappe._dict()).setdefault(salesweekstr,mth_total_list[cc][fyr][mthyrstr][salesweekstr])
                        mth_total_list2.setdefault(cc, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(mthyrstr,frappe._dict()).setdefault(grossprofitstr,mth_total_list[cc][fyr][mthyrstr][grossprofitstr])
                        mth_total_list2.setdefault(cc, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(mthyrstr,frappe._dict()).setdefault(grossprofitmarginstr,mth_total_list[cc][fyr][mthyrstr][grossprofitmarginstr])
                    except KeyError:
                        mth_total_list2.setdefault(cc, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(mthyrstr,frappe._dict()).setdefault(salesweekstr,0.0)
                        mth_total_list2.setdefault(cc, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(mthyrstr,frappe._dict()).setdefault(grossprofitstr,0.0)
                        mth_total_list2.setdefault(cc, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(mthyrstr,frappe._dict()).setdefault(grossprofitmarginstr,0.0)
                    #for dd in min_date_backlog:
                    #for dd in min_date_backlog:    
                    #    if ((dd.cost_center==cc) and (dd.Date==mthyrstr) and (dd.year==fyr)):
                    #        ccTotalAmt = dd.TotalAmt
                    #year_total_list2.setdefault(cc, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(mthyrstr,ccTotalAmt)
                    #year_total_list2[cc][fyr][mthyrstr] += flt(ccTotalAmt)
                    #for dd in min_date_backloglst:    
                    #    if ((dd[3]==cc) and (dd[0]==mthyrstr) and (dd[1]==fyr)):
                    #        ccTotalAmt = dd[2]
                    #year_total_list2.setdefault(cc, frappe._dict()).setdefault(fyr,frappe._dict()).setdefault(mthyrstr,ccTotalAmt)
                    #year_total_list2[cc][fyr][mthyrstr] += flt(ccTotalAmt)

            currdt2 = currdt + relativedelta(months=+1)
            currdt = currdt2


    #print(mth_total_list)        
    
    

    #year_lis = list(year_total_list.items())  #convert dict to list
    year_lis = list(mth_total_list2.items())
    wk_lis = wk_total_list #list(wk_total_list.items())
    return wk_lis,year_lis	
  
def get_prevweeklysalesdata(dictkey):
    # call function to retrieve data from table
    retval = ''
    fetch_prevweeklysalesdata = frappe.db.sql(
        """
        select id, dictkey , dictval from `tabWeeklyreportdata`
        where dictkey = %(dictkey)s 
        """,{
            'dictkey': dictkey
        },		
        as_dict=0,
    )
    if fetch_prevweeklysalesdata:
        print(fetch_prevweeklysalesdata)
        retval = fetch_prevweeklysalesdata[0][2]
    print(retval)
    return retval     
    # return retuned value - can be null /none
    
def set_prevweeklysalesdata(dictkey,dictval):
    # call function to save data to table
    set_prevweeklysalesdata = frappe.db.sql(
        """
        insert into `tabWeeklyreportdata`(dictkey , dictval) values
         (%(dictkey)s , %(dictval)s)
        """,{
            'dictkey': dictkey,'dictval': dictval,
        },		
        debug=1,
        auto_commit=1
    )
    if set_prevweeklysalesdata:
        print("saved")

def get_keycode(coycostcenternos,secsuffix,styear,endyear):
    retval = ''
    yrcode = str(styear)+'-'+str(endyear)
    coycostcenternosstr = ''
    for ccno in coycostcenternos:
        coycostcenternosstr += ccno  
    retval = 'wk-' + secsuffix + '-' + coycostcenternosstr + '-' + yrcode
    print(retval)
    return retval        
            

